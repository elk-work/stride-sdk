package stridesdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/elkproject/stride-sdk/stride"
)

// capture is an httptest handler that records every ingest request.
type capture struct {
	mu   sync.Mutex
	fail int // respond 503 to this many requests before succeeding
	reqs []capturedRequest
}

type capturedRequest struct {
	path   string
	auth   string
	ctype  string
	body   []byte
	status int
}

func (c *capture) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	status := http.StatusOK
	if c.fail > 0 {
		c.fail--
		status = http.StatusServiceUnavailable
	}
	c.reqs = append(c.reqs, capturedRequest{
		path:   r.URL.Path,
		auth:   r.Header.Get("Authorization"),
		ctype:  r.Header.Get("Content-Type"),
		body:   body,
		status: status,
	})
	c.mu.Unlock()
	w.WriteHeader(status)
}

func (c *capture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.reqs)
}

// batches decodes every request that was answered 200.
func (c *capture) batches(t *testing.T) []stride.BatchRequest {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []stride.BatchRequest
	for _, r := range c.reqs {
		if r.status != http.StatusOK {
			continue
		}
		var b stride.BatchRequest
		if err := json.Unmarshal(r.body, &b); err != nil {
			t.Fatalf("unmarshal captured batch: %v\nbody: %s", err, r.body)
		}
		out = append(out, b)
	}
	return out
}

// observations flattens every accepted batch, preserving order.
func (c *capture) observations(t *testing.T) []stride.Observation {
	t.Helper()
	var out []stride.Observation
	for _, b := range c.batches(t) {
		out = append(out, b.Observations...)
	}
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

var testSource = stride.SourceRef{
	System: "elk", Service: "scoutd", Environment: "test", Version: "abc123",
}

// testClient builds an enabled client against srv with test-friendly
// knobs. Callers may override fields on cfg via mutate.
func testClient(srv *httptest.Server, mutate func(*Config)) *Client {
	cfg := Config{
		Endpoint:      srv.URL,
		Token:         "tkn",
		Enabled:       true,
		Source:        testSource,
		FlushInterval: time.Hour, // tests flush via BatchSize or Close
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return newClient(cfg, time.Millisecond)
}

func TestBatchWireShapeAndPropagation(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(cap.handler))
	defer srv.Close()
	c := testClient(srv, nil)

	ctx := ContextWithTrace(context.Background(), "trace-in", "req-in")
	ctx, root := c.Start(ctx, "op.root")
	childCtx, child := c.Start(ctx, "op.child")
	c.Event(childCtx, stride.KindJobStarted, Fields{Operation: "job.x"})
	var err error
	child.End(&err)
	root.End(&err)
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cap.mu.Lock()
	req := cap.reqs[0]
	cap.mu.Unlock()
	if req.path != "/v1/observations:batch" {
		t.Errorf("path = %q, want /v1/observations:batch", req.path)
	}
	if req.auth != "Bearer tkn" {
		t.Errorf("Authorization = %q, want Bearer tkn", req.auth)
	}
	if req.ctype != "application/json" {
		t.Errorf("Content-Type = %q", req.ctype)
	}

	batches := cap.batches(t)
	if len(batches) == 0 {
		t.Fatal("no accepted batches")
	}
	if batches[0].Source != testSource {
		t.Errorf("batch source = %+v, want %+v", batches[0].Source, testSource)
	}

	byKindOp := map[string]stride.Observation{}
	for _, o := range cap.observations(t) {
		byKindOp[o.Kind+"/"+o.Operation] = o
	}
	rootObs, ok := byKindOp[stride.KindOperationCompleted+"/op.root"]
	if !ok {
		t.Fatal("missing operation_completed for op.root")
	}
	childObs, ok := byKindOp[stride.KindOperationCompleted+"/op.child"]
	if !ok {
		t.Fatal("missing operation_completed for op.child")
	}
	eventObs, ok := byKindOp[stride.KindJobStarted+"/job.x"]
	if !ok {
		t.Fatal("missing job_started for job.x")
	}

	// Inbound trace seed is inherited all the way down.
	for name, o := range map[string]stride.Observation{"root": rootObs, "child": childObs, "event": eventObs} {
		if o.Context.TraceID != "trace-in" {
			t.Errorf("%s trace_id = %q, want trace-in", name, o.Context.TraceID)
		}
		if o.Context.RequestID != "req-in" {
			t.Errorf("%s request_id = %q, want req-in", name, o.Context.RequestID)
		}
	}
	if rootObs.Context.SpanID == "" || rootObs.Context.ParentSpanID != "" {
		t.Errorf("root context = %+v, want span_id set and no parent", rootObs.Context)
	}
	if childObs.Context.ParentSpanID != rootObs.Context.SpanID {
		t.Errorf("child parent_span_id = %q, want root span id %q",
			childObs.Context.ParentSpanID, rootObs.Context.SpanID)
	}
	if eventObs.Context.ParentSpanID != childObs.Context.SpanID {
		t.Errorf("event parent_span_id = %q, want child span id %q",
			eventObs.Context.ParentSpanID, childObs.Context.SpanID)
	}
	if eventObs.Context.SpanID != "" {
		t.Errorf("event span_id = %q, want empty (points are not spans)", eventObs.Context.SpanID)
	}
	if rootObs.Status != stride.StatusSuccess || rootObs.DurationMS == nil {
		t.Errorf("root obs = %+v, want success with duration", rootObs)
	}
	if rootObs.ID == "" || len(rootObs.ID) != 26 {
		t.Errorf("root obs id = %q, want a ULID", rootObs.ID)
	}

	// Outbound propagation headers.
	h := child.Headers()
	if h[HeaderTraceID] != "trace-in" || h[HeaderParentSpanID] != childObs.Context.SpanID || h[HeaderRequestID] != "req-in" {
		t.Errorf("child headers = %v", h)
	}
	if child.Context().SpanID != childObs.Context.SpanID {
		t.Errorf("span.Context() span_id = %q, want %q", child.Context().SpanID, childObs.Context.SpanID)
	}
}

func TestFixtureRoundTrip(t *testing.T) {
	raw, err := os.ReadFile("../fixtures/batch.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var first stride.BatchRequest
	if err := json.Unmarshal(raw, &first); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	if first.Source.System != "clawfight" || len(first.Observations) != 6 {
		t.Fatalf("fixture = system %q with %d observations, want clawfight with 6",
			first.Source.System, len(first.Observations))
	}
	failed := first.Observations[2]
	if failed.Kind != stride.KindOperationFailed || failed.Error == nil ||
		failed.Error.Class != "DatabaseError" || !failed.Error.Retryable {
		t.Errorf("fixture observation 3 lost error record: %+v", failed)
	}
	if failed.Context.TraceID == "" || failed.Context.SpanID != "span-root" {
		t.Errorf("fixture observation 3 lost context: %+v", failed.Context)
	}

	again, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var second stride.BatchRequest
	if err := json.Unmarshal(again, &second); err != nil {
		t.Fatalf("unmarshal round trip: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Errorf("round trip changed the batch:\nfirst:  %+v\nsecond: %+v", first, second)
	}
}

func TestDisabledClientNoops(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(cap.handler))
	defer srv.Close()

	for name, cfg := range map[string]Config{
		"zero":        {},
		"no endpoint": {Enabled: true, Token: "tkn"},
		"no token":    {Enabled: true, Endpoint: srv.URL},
		"not enabled": {Endpoint: srv.URL, Token: "tkn"},
	} {
		c := New(cfg)
		if c == nil {
			t.Fatalf("%s: New returned nil", name)
		}
		if c.Enabled() {
			t.Errorf("%s: Enabled() = true", name)
		}
		ctx, span := c.Start(context.Background(), "op.disabled")
		if span == nil {
			t.Fatalf("%s: Start returned nil span", name)
		}
		span.Annotate(map[string]any{"k": "v"})
		c.Event(ctx, stride.KindJobStarted, Fields{Operation: "job.x"})
		c.Heartbeat(ctx, "hb")
		var err error
		span.End(&err)

		// Context propagation still works while disabled.
		_, child := c.Start(ctx, "op.child")
		if child.Context().TraceID != span.Context().TraceID {
			t.Errorf("%s: child did not inherit trace id", name)
		}
		if child.Context().ParentSpanID != span.Context().SpanID {
			t.Errorf("%s: child did not set parent span id", name)
		}
		if err := c.Close(context.Background()); err != nil {
			t.Errorf("%s: Close = %v", name, err)
		}
	}
	if n := cap.count(); n != 0 {
		t.Errorf("disabled clients sent %d requests", n)
	}
}

func TestNilClientAndSpanSafe(t *testing.T) {
	var c *Client
	var s *Span
	if c.Enabled() {
		t.Error("nil client reports enabled")
	}
	ctx, span := c.Start(context.Background(), "op.nil")
	if span == nil {
		t.Fatal("nil client Start returned nil span")
	}
	c.Event(ctx, stride.KindHeartbeat, Fields{})
	c.Heartbeat(ctx, "hb")
	var err error
	span.End(&err)
	if err := c.Close(context.Background()); err != nil {
		t.Errorf("nil client Close = %v", err)
	}

	s.Annotate(map[string]any{"k": "v"})
	s.Fail(errors.New("boom"))
	s.End(nil)
	if got := s.Context(); got != (stride.Context{}) {
		t.Errorf("nil span Context = %+v", got)
	}
	if got := s.Headers(); len(got) != 0 {
		t.Errorf("nil span Headers = %v", got)
	}
	if SpanFromContext(context.Background()) != nil {
		t.Error("SpanFromContext on bare context is not nil")
	}
}

func TestNeverBlocksWhenServerDown(t *testing.T) {
	// Nothing listens here: every flush fails fast with conn refused.
	cfg := Config{
		Endpoint:      "http://127.0.0.1:9",
		Token:         "tkn",
		Enabled:       true,
		Source:        testSource,
		FlushInterval: 10 * time.Millisecond,
		HTTPClient:    &http.Client{Timeout: 100 * time.Millisecond},
	}
	c := newClient(cfg, time.Millisecond)

	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 10000; i++ {
		c.Event(ctx, stride.KindMetricSampled, Fields{Operation: "op.flood"})
	}
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Errorf("10k emits took %v, emission must never block", elapsed)
	}
	c.mu.Lock()
	buffered := len(c.buf)
	c.mu.Unlock()
	if buffered > DefaultBufferSize {
		t.Errorf("buffer holds %d observations, bound is %d", buffered, DefaultBufferSize)
	}

	closeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	closeStart := time.Now()
	c.Close(closeCtx)
	if d := time.Since(closeStart); d > 3*time.Second {
		t.Errorf("Close took %v with server down", d)
	}
}

func TestBufferOverflowDropsOldestAndReports(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(cap.handler))
	defer srv.Close()
	c := testClient(srv, func(cfg *Config) {
		cfg.BufferSize = 5
		cfg.BatchSize = 100 // never triggers; only Close flushes
	})

	ctx := context.Background()
	ops := []string{"op.0", "op.1", "op.2", "op.3", "op.4", "op.5", "op.6", "op.7", "op.8", "op.9"}
	for _, op := range ops {
		c.Event(ctx, stride.KindMetricSampled, Fields{Operation: op})
	}
	c.mu.Lock()
	buffered, dropped := len(c.buf), c.dropped
	c.mu.Unlock()
	if buffered != 5 || dropped != 5 {
		t.Fatalf("buffered %d dropped %d, want 5 and 5", buffered, dropped)
	}

	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	obs := cap.observations(t)
	if len(obs) != 6 {
		t.Fatalf("server received %d observations, want marker + newest 5", len(obs))
	}
	marker := obs[0]
	if marker.Kind != stride.KindMetricSampled || marker.Operation != droppedOperation {
		t.Fatalf("first observation = %s/%s, want metric_sampled/%s", marker.Kind, marker.Operation, droppedOperation)
	}
	if got := marker.Attributes["dropped"]; got != float64(5) {
		t.Errorf("dropped attribute = %v, want 5", got)
	}
	for i, o := range obs[1:] {
		if want := ops[5+i]; o.Operation != want {
			t.Errorf("observation %d = %q, want %q (oldest must be dropped)", i+1, o.Operation, want)
		}
	}
}

func TestGiveUpDropsAndReportsOnNextBatch(t *testing.T) {
	cap := &capture{fail: 3} // exactly the first batch's 1 try + 2 retries
	srv := httptest.NewServer(http.HandlerFunc(cap.handler))
	defer srv.Close()
	c := testClient(srv, func(cfg *Config) {
		cfg.BatchSize = 1 // every emit kicks a flush
	})
	defer c.Close(context.Background())

	ctx := context.Background()
	c.Event(ctx, stride.KindJobStarted, Fields{Operation: "job.a"})
	waitFor(t, "batch A to be retried and given up", func() bool { return cap.count() >= 3 })

	c.Event(ctx, stride.KindJobStarted, Fields{Operation: "job.b"})
	waitFor(t, "a successful batch", func() bool { return len(cap.batches(t)) >= 1 })

	obs := cap.observations(t)
	if len(obs) != 2 {
		t.Fatalf("accepted %d observations, want marker + job.b", len(obs))
	}
	if obs[0].Kind != stride.KindMetricSampled || obs[0].Operation != droppedOperation {
		t.Fatalf("first accepted observation = %s/%s, want the dropped marker", obs[0].Kind, obs[0].Operation)
	}
	if got := obs[0].Attributes["dropped"]; got != float64(1) {
		t.Errorf("dropped attribute = %v, want 1 (batch A)", got)
	}
	if obs[1].Operation != "job.b" {
		t.Errorf("second observation = %q, want job.b", obs[1].Operation)
	}
}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

func TestEndSuccessAndFailurePaths(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(cap.handler))
	defer srv.Close()
	c := testClient(srv, nil)
	ctx := context.Background()

	// Success: defer span.End(&err) with err nil.
	func() {
		var err error
		_, span := c.Start(ctx, "op.ok")
		defer span.End(&err)
		span.Annotate(map[string]any{"source": "github"})
	}()

	// Failure: named-return error set before the deferred End runs.
	longMsg := strings.Repeat("x", maxErrorMessage+100)
	func() (err error) {
		_, span := c.Start(ctx, "op.bad")
		defer span.End(&err)
		return &testError{msg: longMsg}
	}()

	// Explicit Fail without an error at End time.
	_, span := c.Start(ctx, "op.swallowed")
	span.Fail(errors.New("handled locally"))
	span.End(nil)
	span.End(nil) // second End must not emit again

	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	byOp := map[string]stride.Observation{}
	obs := cap.observations(t)
	for _, o := range obs {
		byOp[o.Operation] = o
	}
	if len(obs) != 3 {
		t.Fatalf("got %d observations, want 3 (End must fire once per span)", len(obs))
	}

	ok := byOp["op.ok"]
	if ok.Kind != stride.KindOperationCompleted || ok.Status != stride.StatusSuccess {
		t.Errorf("op.ok = %s/%s, want operation_completed/success", ok.Kind, ok.Status)
	}
	if ok.Error != nil || ok.DurationMS == nil || *ok.DurationMS < 0 {
		t.Errorf("op.ok error/duration wrong: %+v", ok)
	}
	if ok.Attributes["source"] != "github" {
		t.Errorf("op.ok attributes = %v, want annotated source", ok.Attributes)
	}

	bad := byOp["op.bad"]
	if bad.Kind != stride.KindOperationFailed || bad.Status != stride.StatusFailure {
		t.Errorf("op.bad = %s/%s, want operation_failed/failure", bad.Kind, bad.Status)
	}
	if bad.Error == nil {
		t.Fatal("op.bad has no error record")
	}
	if bad.Error.Class != "testError" {
		t.Errorf("op.bad error class = %q, want testError (type name without package path)", bad.Error.Class)
	}
	if len(bad.Error.Message) != maxErrorMessage {
		t.Errorf("op.bad message length = %d, want capped at %d", len(bad.Error.Message), maxErrorMessage)
	}
	if bad.Error.Fingerprint != "" {
		t.Errorf("op.bad fingerprint = %q, want empty (server derives)", bad.Error.Fingerprint)
	}

	swallowed := byOp["op.swallowed"]
	if swallowed.Kind != stride.KindOperationFailed || swallowed.Error == nil ||
		swallowed.Error.Class != "errorString" || swallowed.Error.Message != "handled locally" {
		t.Errorf("op.swallowed = %+v, want failure with recorded error", swallowed)
	}
}

func TestCloseFlushesAndSeals(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(cap.handler))
	defer srv.Close()
	c := testClient(srv, nil)

	ctx := context.Background()
	c.Heartbeat(ctx, "scout.heartbeat")
	if n := cap.count(); n != 0 {
		t.Fatalf("flushed before Close (%d requests) despite long interval", n)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	obs := cap.observations(t)
	if len(obs) != 1 || obs[0].Kind != stride.KindHeartbeat || obs[0].Operation != "scout.heartbeat" {
		t.Fatalf("after Close server has %+v, want the heartbeat", obs)
	}

	c.Heartbeat(ctx, "late") // after Close: dropped silently
	if err := c.Close(ctx); err != nil {
		t.Errorf("second Close = %v", err)
	}
	if got := len(cap.observations(t)); got != 1 {
		t.Errorf("emission after Close reached the server (%d observations)", got)
	}
}

func TestAttributeFiltering(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(cap.handler))
	defer srv.Close()
	c := testClient(srv, nil)

	long := strings.Repeat("s", maxAttributeString+50)
	c.Event(context.Background(), stride.KindMetricSampled, Fields{
		Operation: "op.attrs",
		Attributes: map[string]any{
			"str":    long,
			"int":    42,
			"float":  1.5,
			"bool":   true,
			"nan":    math.NaN(),
			"inf":    math.Inf(1),
			"slice":  []int{1, 2},
			"map":    map[string]string{"no": "pe"},
			"struct": struct{ X int }{1},
			"nil":    nil,
		},
	})
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	obs := cap.observations(t)
	if len(obs) != 1 {
		t.Fatalf("got %d observations, want 1", len(obs))
	}
	attrs := obs[0].Attributes
	if len(attrs) != 4 {
		t.Errorf("kept %d attributes %v, want the 4 scalars", len(attrs), attrs)
	}
	if s, _ := attrs["str"].(string); len(s) != maxAttributeString {
		t.Errorf("string attribute length = %d, want capped at %d", len(s), maxAttributeString)
	}
	if attrs["int"] != float64(42) || attrs["float"] != 1.5 || attrs["bool"] != true {
		t.Errorf("scalar attributes mangled: %v", attrs)
	}
	for _, k := range []string{"nan", "inf", "slice", "map", "struct", "nil"} {
		if _, ok := attrs[k]; ok {
			t.Errorf("non-scalar attribute %q survived", k)
		}
	}
}
