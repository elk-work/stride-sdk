# elk-stride-sdk

Stride observation emitter for Python. Standard library only — no
dependencies to audit.

```bash
pip install elk-stride-sdk
```

```python
import os
from elk_stride import Emitter

emitter = Emitter(
    endpoint=os.environ["STRIDE_INGEST_URL"],
    token=os.environ["STRIDE_TOKEN"],
    source={"system": "acme", "service": "renderer",
            "environment": "production", "version": VERSION},
)

with emitter.span("acme.render.job", request_id=job_id):
    run_render(job)
```

Buffered and thread-safe: emission never raises into product code, the buffer
is bounded (oldest dropped first), failed batches are retried with backoff,
and the drop count rides the next successful batch as a `metric_sampled`
observation. `atexit` flushes what is left.

There is no default endpoint. Full documentation, the wire contract, and the
Go and TypeScript SDKs: <https://github.com/elk-work/stride-sdk>

Apache-2.0.
