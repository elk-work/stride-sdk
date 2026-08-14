# Stride SDK

Client libraries for **Stride**, an observation stream: your services emit
immutable facts — spans, job boundaries, dependency calls, heartbeats — and a
receiver turns them into health, traces and findings.

One wire format, three languages, one repository.

| Language | Directory | Install |
|---|---|---|
| Go | [`stride/`](stride), [`stridesdk/`](stridesdk) | `go get github.com/elk-work/stride-sdk` |
| TypeScript | [`ts/`](ts) | `npm install @elk-work/stride-sdk` |
| Python | [`python/`](python) | `pip install elk-stride-sdk` |

The Go module sits at the repository root so its import path is
`github.com/elk-work/stride-sdk/stridesdk` rather than
`.../stride-sdk/go/stridesdk`. Go is the only one of the three whose public
name is decided by directory layout, so it gets the root; `ts/` and `python/`
name themselves in their own manifests.

## The shape of it

An **observation** is one immutable fact with a time, a `kind`, an
`operation`, a `status`, and optional trace context, attributes and error
record. A **batch** carries a `source` (who is speaking) and up to 1000
observations to `POST {endpoint}/v1/observations:batch` with a bearer token.

Every SDK here holds the same posture:

- **Never throw into product code.** Instrumentation that can break the thing
  it observes is worse than no instrumentation.
- **Never block.** Emission is a bounded, lock-guarded append. When the buffer
  is full the *oldest* observation is dropped.
- **Report the drops.** The next successful batch is prepended with a
  `metric_sampled` observation carrying the count, so a gap in the data is
  visible as a number rather than inferred from absence.
- **Scalars only.** Attributes accept strings, bools and finite numbers.
  Nothing else is serialized, so no accidental payload exfiltration.

## Go

```go
client := stridesdk.New(stridesdk.Config{
    Endpoint: os.Getenv("STRIDE_INGEST_URL"),
    Token:    os.Getenv("STRIDE_TOKEN"),
    Enabled:  os.Getenv("STRIDE_ENABLED") == "true",
    Source: stride.SourceRef{
        System: "acme", Service: "checkout",
        Environment: "production", Version: buildSHA,
    },
})
defer client.Close(context.Background())

func Charge(ctx context.Context, o Order) (err error) {
    ctx, span := client.Start(ctx, "checkout.charge")
    defer span.End(&err)          // reads *err at defer time
    span.Annotate(map[string]any{"provider": "stripe"})
    return doCharge(ctx, o)
}
```

A disabled config yields a no-op client — never nil — so you can instrument
unconditionally and decide at deploy time. Every method is safe on a nil
`*Client` and a nil `*Span`.

## TypeScript

Built for Cloudflare Workers first: no background timers, one flush per
request via the runtime's `waitUntil`.

```ts
import { createStrideFromEnv } from '@elk-work/stride-sdk';

export default {
  async fetch(req: Request, env: Env, ctx: ExecutionContext) {
    const stride = createStrideFromEnv(env, { system: 'acme', service: 'web' });
    const span = stride.start('web.request');
    try {
      return await handle(req);
    } finally {
      span.end();
      stride.flush(ctx.waitUntil.bind(ctx));
    }
  },
};
```

## Python

Standard library only — no dependencies to audit.

```python
emitter = Emitter(
    endpoint=os.environ["STRIDE_INGEST_URL"],
    token=os.environ["STRIDE_TOKEN"],
    source={"system": "acme", "service": "renderer",
            "environment": "production", "version": VERSION},
)

with emitter.span("acme.render.job", request_id=job_id):
    run_render(job)
```

## There is no default endpoint

Every SDK requires the ingest URL from its caller. The same wire format is
served by more than one receiver, and a library that guesses which one you
meant is a library that silently sends your telemetry somewhere you did not
choose.

## The wire format is frozen

JSON field names, observation kinds, status strings, the propagation header
names, and the drop marker's operation name are all compatibility
commitments — deployed receivers parse them and stored rows are queried by
them. Language-level names (Go identifiers, TypeScript types) may be renamed;
what goes on the wire may not. See [`docs/wire-format.md`](docs/wire-format.md)
for the full contract and the list of names that look renameable and are not.

`fixtures/batch.json` is one real batch, and all three test suites parse it.
It is the tiebreaker when the implementations disagree.

## License

Apache-2.0. See [LICENSE](LICENSE).
