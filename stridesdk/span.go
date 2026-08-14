package stridesdk

import (
	"context"
	"sync"
	"time"

	"github.com/elk-work/stride-sdk/stride"
)

// Propagation header names, mirroring the TypeScript SDK. The x-watch-
// prefix predates the Stride name and is frozen: already-instrumented
// services read these exact headers, so renaming them would silently break
// trace stitching across a partially-upgraded fleet.
const (
	HeaderTraceID      = "x-watch-trace-id"
	HeaderParentSpanID = "x-watch-parent-span-id"
	HeaderRequestID    = "x-watch-request-id"
)

// Span is one timed execution of an operation. Create with
// Client.Start; end exactly once with End (or let Fail mark it failed
// first). All methods are safe on a nil *Span.
type Span struct {
	client    *Client
	operation string
	id        string
	wctx      stride.Context // SpanID == id
	start     time.Time

	mu     sync.Mutex
	attrs  map[string]any
	rec    *stride.ErrorRecord
	failed bool
	ended  bool
}

// Start begins a span and returns a derived context carrying it. A
// parent span already in ctx makes this a child: the trace id is
// inherited and parent_span_id is the parent's span id. Otherwise the
// trace seeded by ContextWithTrace is used, and a fresh ULID trace id
// is minted when there is none. Works on a nil or disabled client (the
// span propagates context but emits nothing).
func (c *Client) Start(ctx context.Context, operation string) (context.Context, *Span) {
	wc := stride.Context{}
	if parent := SpanFromContext(ctx); parent != nil {
		wc = parent.wctx
		wc.ParentSpanID = parent.id
	} else if tr, ok := traceFromContext(ctx); ok {
		wc.TraceID = tr.traceID
		wc.RequestID = tr.requestID
	}
	if wc.TraceID == "" {
		wc.TraceID = newID()
	}
	s := &Span{
		client:    c,
		operation: operation,
		id:        newID(),
		start:     time.Now(),
	}
	wc.SpanID = s.id
	s.wctx = wc
	return context.WithValue(ctx, spanContextKey{}, s), s
}

// Annotate attaches attributes to the span's terminal observation.
// Scalars only; other values are silently dropped, strings capped at
// 1KB. Later keys overwrite earlier ones.
func (s *Span) Annotate(attrs map[string]any) {
	if s == nil {
		return
	}
	clean := filterAttributes(attrs)
	if clean == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attrs == nil {
		s.attrs = make(map[string]any, len(clean))
	}
	for k, v := range clean {
		s.attrs[k] = v
	}
}

// Fail marks the span failed with err without ending it: the eventual
// End emits operation_failed carrying err even when *its* error is nil.
// Use it on paths that handle an error locally but should still count
// as failures.
func (s *Span) Fail(err error) {
	if s == nil || err == nil {
		return
	}
	rec := errorRecord(err)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = true
	s.rec = rec
}

// End emits the span's terminal observation, exactly once. It reads
// *errp at call time so it composes with named returns:
//
//	ctx, span := client.Start(ctx, "scout.refresh.run")
//	defer span.End(&err)
//
// A nil errp or nil *errp emits operation_completed with status
// success; a non-nil error emits operation_failed with an ErrorRecord.
func (s *Span) End(errp *error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	rec := s.rec
	failed := s.failed
	attrs := s.attrs
	s.attrs = nil
	s.mu.Unlock()

	if errp != nil && *errp != nil {
		rec = errorRecord(*errp)
		failed = true
	}
	kind, status := stride.KindOperationCompleted, stride.StatusSuccess
	if failed {
		kind, status = stride.KindOperationFailed, stride.StatusFailure
	}
	ms := time.Since(s.start).Milliseconds()
	if ms < 0 {
		ms = 0
	}
	s.client.emit(stride.Observation{
		ID:         newID(),
		Time:       time.Now().UTC(),
		Kind:       kind,
		Operation:  s.operation,
		Status:     status,
		DurationMS: &ms,
		Context:    s.wctx,
		Attributes: attrs,
		Error:      rec,
	})
}

// Context returns the span's wire context (trace id, span id, parent
// span id, request id, ...). Zero value on a nil span.
func (s *Span) Context() stride.Context {
	if s == nil {
		return stride.Context{}
	}
	return s.wctx
}

// Headers returns the outbound propagation headers for downstream
// calls: x-watch-trace-id, x-watch-parent-span-id (this span's id), and
// x-watch-request-id. Empty map on a nil span.
func (s *Span) Headers() map[string]string {
	h := map[string]string{}
	if s == nil {
		return h
	}
	if s.wctx.TraceID != "" {
		h[HeaderTraceID] = s.wctx.TraceID
	}
	h[HeaderParentSpanID] = s.id
	if s.wctx.RequestID != "" {
		h[HeaderRequestID] = s.wctx.RequestID
	}
	return h
}

type spanContextKey struct{}

type traceSeed struct {
	traceID   string
	requestID string
}

type traceContextKey struct{}

// ContextWithTrace seeds ctx with inbound trace identifiers (typically
// read from x-watch-trace-id / x-watch-request-id headers) so spans and
// events derived from ctx join the caller's trace. Empty values are
// preserved as absent, never invented.
func ContextWithTrace(ctx context.Context, traceID, requestID string) context.Context {
	return context.WithValue(ctx, traceContextKey{}, traceSeed{traceID: traceID, requestID: requestID})
}

// SpanFromContext returns the span carried by ctx, or nil.
func SpanFromContext(ctx context.Context) *Span {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(spanContextKey{}).(*Span)
	return s
}

func traceFromContext(ctx context.Context) (traceSeed, bool) {
	if ctx == nil {
		return traceSeed{}, false
	}
	tr, ok := ctx.Value(traceContextKey{}).(traceSeed)
	return tr, ok
}

// contextFor derives the wire context for a point observation: inside a
// span it carries the span's identifiers with parent_span_id set to the
// span's id (the point is a child of the span, not a span itself);
// otherwise whatever ContextWithTrace seeded, possibly nothing.
func contextFor(ctx context.Context) stride.Context {
	if sp := SpanFromContext(ctx); sp != nil {
		wc := sp.wctx
		wc.SpanID = ""
		wc.ParentSpanID = sp.id
		return wc
	}
	if tr, ok := traceFromContext(ctx); ok {
		return stride.Context{TraceID: tr.traceID, RequestID: tr.requestID}
	}
	return stride.Context{}
}
