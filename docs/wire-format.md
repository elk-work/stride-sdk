# The Stride wire format

One `POST`, one body shape, one bearer token.

```
POST {endpoint}/v1/observations:batch
Authorization: Bearer {token}
Content-Type: application/json

{"source": {...}, "observations": [{...}, ...]}
```

At most **1000** observations per batch. The Go package
[`stride`](../stride/stride.go) is the normative definition; `ts/src/types.ts`
and `python/elk_stride/__init__.py` mirror it, and `fixtures/batch.json` is
the shared fixture all three test suites parse.

## What is frozen

Everything a receiver reads or a stored row is queried by:

- every JSON field name in `source`, `context`, `error` and the observation
  itself — `duration_ms`, `parent_span_id`, `trace_id`, `fingerprint`, …
- the 14 observation `kind` strings
- the 6 `status` strings
- the endpoint path `/v1/observations:batch` and the `Bearer` scheme

Language-level names are *not* frozen. Go identifiers, TypeScript type names
and Python symbols can be renamed freely, because none of them cross the wire.
That distinction is the whole rule: **rename nouns, never fields.**

## Names that look renameable and are not

Three strings still carry the SDK's pre-Stride vocabulary. Each is a value on
the wire, not a name in an API, and each is load-bearing somewhere already
deployed:

| String | Where it lives | Why it stays |
|---|---|---|
| `x-watch-trace-id`, `x-watch-parent-span-id`, `x-watch-request-id` | outbound propagation headers | An upgraded service and a not-yet-upgraded one must still stitch into one trace. Renaming these breaks propagation silently — traces do not error, they just come apart. |
| `watch.sdk.dropped` | the drop marker's `operation` | It sits in the stored `operation` column of existing rows and is queried there. Renaming it splits one metric into two with no migration. |
| `watch.heartbeat` | Python `Emitter.heartbeat()` default | Same reason; pass your own operation name instead of relying on the default. |

These are the cost of a rename that came after deployment rather than before.
They are inert — nothing reads them as a product name — and buying cosmetic
consistency with a fleet-wide propagation break is not a trade worth making.

## Observation kinds

```
operation_started    operation_completed    operation_failed
dependency_called    dependency_failed
job_enqueued         job_started            job_completed      job_failed
log_emitted          metric_sampled
deployment_started   deployment_completed   heartbeat
```

## Statuses

```
success   failure   timeout   cancelled   partial   unknown
```

Absent means `unknown`; the receiver never invents one.

## Attributes

Scalars only — strings (capped at 1KB), booleans, and finite numbers.
Everything else is dropped client-side before it can be serialized. There is
no client-side key allowlist in the Go and Python SDKs; server-side validation
is the guard. The TypeScript SDK does carry an allowlist, because it runs in
request handlers where the surrounding scope holds request bodies and headers.

## Errors

`class`, `code`, `message` (capped), `fingerprint`, `retryable`, `stack`.

Leave `fingerprint` empty and the server derives one from stable properties —
class, code, operation — and never from message text, so that two failures
differing only in an interpolated id group together.

## Responses

`200` with `{"accepted": n, "dropped": n, "errors": [...]}`. The envelope
succeeds even when individual observations were rejected: one malformed
observation must not cost you the other 999. Non-2xx bodies are
`{"code": ..., "message": ...}` where code is one of `validation`,
`permission`, `too_large`, `not_found`, `internal`.

Clients retry `5xx` with backoff and do not retry `4xx` — a validation or auth
failure will fail identically the second time.
