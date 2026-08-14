package stridesdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/elk-work/stride-sdk/stride"
)

// Defaults for Config; see the field docs.
const (
	DefaultBufferSize    = 1000
	DefaultBatchSize     = 200
	DefaultFlushInterval = 5 * time.Second

	maxErrorMessage    = 4096 // bytes of ErrorRecord.Message kept
	maxAttributeString = 1024 // bytes of a string attribute kept
	maxAttempts        = 3    // 1 initial POST + 2 retries
	defaultBackoff     = 250 * time.Millisecond
	defaultHTTPTimeout = 10 * time.Second

	// droppedOperation is a wire value, not a noun: it lands in the
	// stored `operation` column and is already queried there. Frozen.
	droppedOperation = "watch.sdk.dropped"
)

// Config configures a Client. The zero value is a valid disabled config.
type Config struct {
	// Endpoint is the ingest base URL; the SDK posts to
	// {Endpoint}/v1/observations:batch. There is no default: the same
	// wire format serves more than one receiver, so the caller names
	// the one it means. Empty disables the client.
	Endpoint string
	// Token is sent as the Authorization Bearer token. Empty disables
	// the client.
	Token string
	// Enabled must be true for the client to emit anything.
	Enabled bool
	// Source identifies the emitting service on every batch.
	Source stride.SourceRef

	// BufferSize bounds the in-memory buffer (default 1000). When full,
	// the oldest observation is dropped and counted.
	BufferSize int
	// BatchSize triggers an early flush when the buffer reaches it
	// (default 200).
	BatchSize int
	// FlushInterval is the background flush cadence (default 5s).
	FlushInterval time.Duration
	// HTTPClient overrides the transport (default: 10s timeout).
	HTTPClient *http.Client
}

// Fields are the optional parts of a point observation emitted with
// Event. Context identifiers (trace, parent span) come from ctx.
type Fields struct {
	Operation  string
	Status     string         // one of stride.Statuses; empty means unknown
	Duration   time.Duration  // >0 becomes duration_ms
	RequestID  string         // overrides the request id carried in ctx
	Attributes map[string]any // scalars only; others silently dropped
	Error      error
	Time       time.Time // zero means now
}

// Client buffers observations and ships them in the background. Create
// with New; all methods are safe on a nil or disabled Client.
type Client struct {
	source   stride.SourceRef
	endpoint string
	token    string
	enabled  bool

	bufferSize    int
	batchSize     int
	flushInterval time.Duration
	httpClient    *http.Client
	backoff       time.Duration // retry backoff base; shortened in tests

	mu      sync.Mutex
	buf     []stride.Observation
	dropped int64
	closed  bool

	kick chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
}

// New builds a Client. A disabled config (Enabled false, or missing
// Endpoint/Token) returns a no-op client — never nil — so callers can
// instrument unconditionally.
func New(cfg Config) *Client { return newClient(cfg, defaultBackoff) }

// newClient is New with an explicit retry backoff base, so tests can
// shorten it before the flusher goroutine starts.
func newClient(cfg Config, backoff time.Duration) *Client {
	c := &Client{
		source:        cfg.Source,
		endpoint:      strings.TrimRight(cfg.Endpoint, "/"),
		token:         cfg.Token,
		bufferSize:    cfg.BufferSize,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		httpClient:    cfg.HTTPClient,
		backoff:       backoff,
		kick:          make(chan struct{}, 1),
		done:          make(chan struct{}),
	}
	if c.bufferSize <= 0 {
		c.bufferSize = DefaultBufferSize
	}
	if c.batchSize <= 0 {
		c.batchSize = DefaultBatchSize
	}
	if c.flushInterval <= 0 {
		c.flushInterval = DefaultFlushInterval
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}
	c.enabled = cfg.Enabled && c.endpoint != "" && c.token != ""
	if c.enabled {
		c.wg.Add(1)
		go c.run()
	}
	return c
}

// Enabled reports whether the client will emit anything.
func (c *Client) Enabled() bool { return c != nil && c.enabled }

// Event buffers one point observation of the given kind. Trace context
// is derived from ctx: inside a span the observation carries the span's
// trace id and parent_span_id = the span's id. Unknown kinds are
// dropped and counted. Never blocks.
func (c *Client) Event(ctx context.Context, kind string, f Fields) {
	if c == nil || !c.enabled {
		return
	}
	if !stride.Kinds[kind] {
		c.countDropped(1)
		return
	}
	wc := contextFor(ctx)
	if f.RequestID != "" {
		wc.RequestID = f.RequestID
	}
	when := f.Time
	if when.IsZero() {
		when = time.Now()
	}
	obs := stride.Observation{
		ID:         newID(),
		Time:       when.UTC(),
		Kind:       kind,
		Operation:  f.Operation,
		Status:     f.Status,
		Context:    wc,
		Attributes: filterAttributes(f.Attributes),
	}
	if f.Duration > 0 {
		ms := f.Duration.Milliseconds()
		obs.DurationMS = &ms
	}
	if f.Error != nil {
		obs.Error = errorRecord(f.Error)
	}
	c.emit(obs)
}

// Heartbeat emits a heartbeat observation for the named operation.
func (c *Client) Heartbeat(ctx context.Context, operation string) {
	c.Event(ctx, stride.KindHeartbeat, Fields{Operation: operation})
}

// Close stops the background flusher and performs a final synchronous
// flush bounded by ctx. Safe to call more than once; subsequent
// emissions are no-ops.
func (c *Client) Close(ctx context.Context) error {
	if c == nil || !c.enabled {
		return nil
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	close(c.done)
	c.wg.Wait()
	c.flush(ctx)
	return ctx.Err()
}

// emit appends one observation to the bounded buffer. Lock-guarded
// append only: when the buffer is full the oldest observation is
// dropped and counted, never queued blocking.
func (c *Client) emit(o stride.Observation) {
	if c == nil || !c.enabled {
		return
	}
	kickFlush := false
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if len(c.buf) >= c.bufferSize {
		copy(c.buf, c.buf[1:])
		c.buf = c.buf[:len(c.buf)-1]
		c.dropped++
	}
	c.buf = append(c.buf, o)
	kickFlush = len(c.buf) >= c.batchSize
	c.mu.Unlock()
	if kickFlush {
		select {
		case c.kick <- struct{}{}:
		default:
		}
	}
}

func (c *Client) countDropped(n int64) {
	c.mu.Lock()
	c.dropped += n
	c.mu.Unlock()
}

// run is the background flusher: flush every FlushInterval or when the
// buffer reaches BatchSize.
func (c *Client) run() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.flush(context.Background())
		case <-c.kick:
			c.flush(context.Background())
		case <-c.done:
			return
		}
	}
}

// flush swaps the buffer out and posts it, prepending a metric_sampled
// watch.sdk.dropped observation when observations were dropped since
// the last successful send. Never panics into the product.
func (c *Client) flush(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			c.countDropped(1)
		}
	}()
	c.mu.Lock()
	batch := c.buf
	c.buf = nil
	dropped := c.dropped
	c.dropped = 0
	c.mu.Unlock()
	if dropped > 0 {
		marker := stride.Observation{
			ID:         newID(),
			Time:       time.Now().UTC(),
			Kind:       stride.KindMetricSampled,
			Operation:  droppedOperation,
			Attributes: map[string]any{"dropped": dropped},
		}
		batch = append([]stride.Observation{marker}, batch...)
	}
	for start := 0; start < len(batch); start += stride.BatchLimit {
		end := min(start+stride.BatchLimit, len(batch))
		chunk := batch[start:end]
		if !c.post(ctx, chunk) {
			// Give up: drop and count; the count rides the next
			// successful batch as watch.sdk.dropped.
			c.countDropped(int64(len(chunk)))
		}
	}
}

// post sends one batch, retrying a failed POST twice with backoff.
// Non-5xx HTTP errors are not retried (retrying will not help).
func (c *Client) post(ctx context.Context, observations []stride.Observation) bool {
	body, err := json.Marshal(stride.BatchRequest{Source: c.source, Observations: observations})
	if err != nil {
		return false
	}
	delay := c.backoff
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return false
			}
			delay *= 2
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			c.endpoint+"/v1/observations:batch", bytes.NewReader(body))
		if err != nil {
			return false
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+c.token)
		resp, err := c.httpClient.Do(req)
		if err != nil {
			continue // network error: retry
		}
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return true
		}
		if resp.StatusCode < 500 {
			return false // validation/auth: retrying won't help
		}
	}
	return false
}

// newID returns a ULID, the id convention for observations, spans, and
// traces.
func newID() string { return ulid.Make().String() }

// errorRecord normalizes a Go error into the wire ErrorRecord: class is
// the concrete type name trimmed of package path and pointer stars,
// message is capped at 4KB, and the fingerprint is left empty for the
// server to derive from stable properties.
func errorRecord(err error) *stride.ErrorRecord {
	if err == nil {
		return nil
	}
	class := fmt.Sprintf("%T", err)
	class = strings.TrimLeft(class, "*")
	if i := strings.LastIndexByte(class, '.'); i >= 0 {
		class = class[i+1:]
	}
	msg := err.Error()
	if len(msg) > maxErrorMessage {
		msg = msg[:maxErrorMessage]
	}
	return &stride.ErrorRecord{Class: class, Message: msg}
}

// filterAttributes keeps scalar values only — strings (capped at 1KB),
// bools, ints, and finite floats — silently dropping everything else.
// Returns nil when nothing survives.
func filterAttributes(attrs map[string]any) map[string]any {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]any, len(attrs))
	for k, v := range attrs {
		switch val := v.(type) {
		case string:
			if len(val) > maxAttributeString {
				val = val[:maxAttributeString]
			}
			out[k] = val
		case bool,
			int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			out[k] = val
		case float32:
			if !math.IsNaN(float64(val)) && !math.IsInf(float64(val), 0) {
				out[k] = val
			}
		case float64:
			if !math.IsNaN(val) && !math.IsInf(val, 0) {
				out[k] = val
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
