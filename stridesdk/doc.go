// Package stridesdk is the Go instrumentation SDK for Stride. It buffers
// observations in memory and ships them in batches to
// POST {Endpoint}/v1/observations:batch, mirroring the TypeScript (ts/) and
// Python (python/) SDKs and reusing the wire types from
// github.com/elkproject/stride-sdk/stride verbatim.
//
// # Usage
//
//	client := stridesdk.New(stridesdk.Config{
//	    Endpoint: os.Getenv("STRIDE_INGEST_URL"),
//	    Token:    os.Getenv("STRIDE_TOKEN"),
//	    Enabled:  os.Getenv("STRIDE_ENABLED") == "true",
//	    Source: stride.SourceRef{
//	        System: "acme", Service: "checkout",
//	        Environment: "production", Version: buildSHA,
//	    },
//	})
//	defer client.Close(context.Background()) // final synchronous flush
//
// There is no default endpoint. A disabled config (Enabled false, or missing
// Endpoint/Token) yields a no-op client, never nil; every method is safe on
// it, and on a nil *Client or nil *Span.
//
// # Spans
//
// Start begins a span and stores it in the context; child spans started
// from that context inherit the trace id and set parent_span_id
// automatically. The canonical shape is the defer pattern:
//
//	func HandlePrompt(ctx context.Context, p Prompt) (result Result, err error) {
//	    ctx, span := client.Start(ctx, "checkout.prompt.respond")
//	    defer span.End(&err)
//
//	    span.Annotate(map[string]any{"source": "github"})
//	    // work
//	}
//
// End reads *err at defer time: nil emits operation_completed with
// status success; non-nil emits operation_failed with an ErrorRecord
// derived from the error (class is the Go type name without package
// path, message capped at 4KB, fingerprint left for the server to
// derive). span.Fail(err) marks the span failed without ending it, for
// paths that swallow the error.
//
// # Point observations
//
//	client.Event(ctx, stride.KindJobStarted, stridesdk.Fields{
//	    Operation: "job.x", RequestID: jobID,
//	})
//	client.Heartbeat(ctx, "checkout.heartbeat")
//
// # Propagation
//
// For inbound requests, seed the context from transport headers with
// ContextWithTrace(ctx, traceID, requestID). For outbound calls,
// span.Headers() returns the propagation headers, and span.Context() the
// wire stride.Context.
//
// # Delivery semantics
//
// Emission is a lock-guarded append and never blocks or panics: when
// the bounded buffer (default 1000) is full the oldest observation is
// dropped and counted. A background flusher posts batches every
// FlushInterval (default 5s) or when the buffer reaches BatchSize
// (default 200), retrying a failed POST twice with backoff. When a
// batch is given up on, its observations are dropped and counted, and
// the next successful batch is prepended with a metric_sampled
// observation carrying the count. There is no disk buffering. Close
// stops the flusher and performs a final synchronous flush bounded by
// its context.
//
// Attributes accept scalars only (strings capped at 1KB, bools, ints,
// finite floats); other values are silently dropped. There is no
// client-side attribute allowlist: server-side validation is the guard.
//
// # Frozen wire values
//
// Two strings in this package look like renameable nouns and are not. The
// propagation headers are x-watch-trace-id / x-watch-parent-span-id /
// x-watch-request-id, and the drop marker's operation is
// "watch.sdk.dropped". Both predate the Stride name, both are read by
// deployed services and stored rows, and changing either would break
// propagation across an already-instrumented fleet. See docs/wire-format.md.
package stridesdk
