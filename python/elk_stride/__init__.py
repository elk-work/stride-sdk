"""Stride observation emitter (stdlib only).

Mirrors the wire contract in the Go package `stride`. Buffer in memory,
flush in batches with bounded retries, never raise into product code, drop
under pressure and report the drops.

There is no default endpoint: the same wire format serves more than one
receiver, so the caller names the one it means.

    emitter = Emitter(
        endpoint=os.environ["STRIDE_INGEST_URL"],
        token=os.environ["STRIDE_TOKEN"],
        source={"system": "acme", "service": "renderer",
                "environment": "production", "version": RENDER_VERSION},
    )

    with emitter.span("acme.render.job", request_id=job_id):
        run_render(job)
"""

from __future__ import annotations

import atexit
import contextlib
import json
import threading
import time as _time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any, Iterator, Optional

BATCH_LIMIT = 1000
MAX_ERROR_MESSAGE = 4096

_KINDS = {
    "operation_started", "operation_completed", "operation_failed",
    "dependency_called", "dependency_failed",
    "job_enqueued", "job_started", "job_completed", "job_failed",
    "log_emitted", "metric_sampled",
    "deployment_started", "deployment_completed", "heartbeat",
}

_CONTEXT_KEYS = {
    "trace_id", "span_id", "parent_span_id", "request_id", "session_id",
    "user_id", "workspace_id", "deployment_id", "ark_version",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Emitter:
    """Buffered, thread-safe, never-raising observation emitter."""

    def __init__(
        self,
        endpoint: Optional[str],
        token: Optional[str],
        source: dict[str, Any],
        *,
        enabled: bool = True,
        flush_at: int = 200,
        max_buffer: int = 5000,
        retries: int = 3,
        timeout: float = 10.0,
    ) -> None:
        self.endpoint = (endpoint or "").rstrip("/")
        self.token = token or ""
        self.source = dict(source)
        self.enabled = bool(enabled and self.endpoint and self.token)
        self.flush_at = flush_at
        self.max_buffer = max_buffer
        self.retries = retries
        self.timeout = timeout
        self.dropped = 0
        self._buffer: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        if self.enabled:
            atexit.register(self.flush)

    # -- emit -------------------------------------------------------------

    def observe(
        self,
        kind: str,
        operation: Optional[str] = None,
        *,
        status: Optional[str] = None,
        duration_ms: Optional[int] = None,
        attributes: Optional[dict[str, Any]] = None,
        error: Optional[dict[str, Any]] = None,
        time: Optional[str] = None,
        **context: str,
    ) -> None:
        """Buffer one observation. Unknown kinds are dropped and counted."""
        try:
            if not self.enabled:
                return
            if kind not in _KINDS:
                self.dropped += 1
                return
            obs: dict[str, Any] = {
                "id": str(uuid.uuid4()),
                "time": time or _now_iso(),
                "kind": kind,
            }
            if operation:
                obs["operation"] = operation
            if status:
                obs["status"] = status
            if duration_ms is not None:
                obs["duration_ms"] = max(0, int(duration_ms))
            ctx = {k: v for k, v in context.items() if k in _CONTEXT_KEYS and v}
            if ctx:
                obs["context"] = ctx
            if attributes:
                clean = {
                    k: v for k, v in attributes.items()
                    if isinstance(v, (str, int, float, bool)) and not isinstance(v, bytes)
                }
                if clean:
                    obs["attributes"] = clean
            if error:
                err = {k: error[k] for k in
                       ("class", "code", "message", "fingerprint", "retryable", "stack")
                       if k in error}
                if "message" in err:
                    err["message"] = str(err["message"])[:MAX_ERROR_MESSAGE]
                obs["error"] = err

            do_flush = False
            with self._lock:
                self._buffer.append(obs)
                if len(self._buffer) > self.max_buffer:
                    self._buffer.pop(0)
                    self.dropped += 1
                do_flush = len(self._buffer) >= self.flush_at
            if do_flush:
                self.flush()
        except Exception:
            self.dropped += 1

    @contextlib.contextmanager
    def span(self, operation: str, *, attributes: Optional[dict[str, Any]] = None,
             **context: str) -> Iterator[None]:
        """Time a block; emit operation_completed/failed; re-raise errors."""
        started = _time.monotonic()
        span_id = str(uuid.uuid4())
        context.setdefault("trace_id", str(uuid.uuid4()))
        context["span_id"] = span_id
        try:
            yield
        except Exception as exc:
            self.observe(
                "operation_failed", operation,
                status="failure",
                duration_ms=int((_time.monotonic() - started) * 1000),
                attributes=attributes,
                error={"class": type(exc).__name__, "message": str(exc)},
                **context,
            )
            raise
        self.observe(
            "operation_completed", operation,
            status="success",
            duration_ms=int((_time.monotonic() - started) * 1000),
            attributes=attributes,
            **context,
        )

    def heartbeat(self, operation: str = "watch.heartbeat") -> None:
        """Emit a heartbeat.

        The default operation name predates the Stride rename and is frozen:
        it is a wire value that already sits in stored rows and is queried
        there. Pass your own operation name.
        """
        self.observe("heartbeat", operation)

    # -- flush ------------------------------------------------------------

    def flush(self) -> None:
        """Send everything buffered. Safe to call from anywhere, anytime."""
        try:
            if not self.enabled:
                return
            with self._lock:
                batch, self._buffer = self._buffer, []
                dropped, self.dropped = self.dropped, 0
            if dropped:
                batch.insert(0, {
                    "id": str(uuid.uuid4()),
                    "time": _now_iso(),
                    "kind": "metric_sampled",
                    "operation": "watch.sdk.dropped",
                    "attributes": {"dropped": dropped},
                })
            for start in range(0, len(batch), BATCH_LIMIT):
                chunk = batch[start:start + BATCH_LIMIT]
                if not self._post(chunk):
                    with self._lock:
                        self.dropped += len(chunk)
        except Exception:
            pass

    def _post(self, observations: list[dict[str, Any]]) -> bool:
        body = json.dumps({"source": self.source, "observations": observations}).encode()
        req = urllib.request.Request(
            self.endpoint + "/v1/observations:batch",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer " + self.token,
            },
            method="POST",
        )
        delay = 0.5
        for attempt in range(self.retries):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    if 200 <= resp.status < 300:
                        return True
                    return False  # 2xx-decoded but unexpected: don't hammer
            except urllib.error.HTTPError as e:
                if e.code < 500:
                    return False  # validation/auth: retrying won't help
            except Exception:
                pass
            if attempt < self.retries - 1:
                _time.sleep(delay)
                delay *= 2
        return False
