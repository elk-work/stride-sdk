# @elk/stride-sdk

Stride observation emitter for Cloudflare Workers, Node, and any fetch-capable
runtime. Zero runtime dependencies.

```bash
npm install @elk/stride-sdk
```

```ts
import { createStrideFromEnv } from '@elk/stride-sdk';

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

Design posture: never throws into product code, no background timers (buffer
per instance, flush once via the injected `waitUntil`), attributes pass an
allowlist, and drops are reported rather than hidden.

`createStrideFromEnv` reads `STRIDE_INGEST_URL`, `STRIDE_TOKEN`,
`STRIDE_ENABLED`, `STRIDE_ENVIRONMENT` and `STRIDE_VERSION`, each falling back
to its `WATCH_*` predecessor. There is no default ingest URL.

Full documentation, the wire contract, and the Go and Python SDKs:
<https://github.com/elkproject/stride-sdk>

Apache-2.0.
