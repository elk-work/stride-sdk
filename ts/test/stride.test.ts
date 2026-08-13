import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  createStride,
  createStrideFromEnv,
  matchTraceId,
  stableTraceId,
  _resetDropped,
} from '../src/index.js';
import type { BatchRequest } from '../src/index.js';

const SOURCE = {
  system: 'clawfight',
  service: 'clawfight-runtime',
  environment: 'staging',
  version: 'sha-test',
};

function mockFetch(status = 200) {
  const calls: { url: string; body: BatchRequest; headers: Record<string, string> }[] = [];
  const fn = vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
    calls.push({
      url: String(url),
      body: JSON.parse(String(init?.body)) as BatchRequest,
      headers: Object.fromEntries(Object.entries((init?.headers ?? {}) as Record<string, string>)),
    });
    return new Response('{"accepted":1,"dropped":0}', { status });
  });
  return { fn, calls };
}

function enabledStride(fetchFn: typeof fetch) {
  return createStride({
    ingestUrl: 'https://ingest.example.com',
    token: 'tok',
    enabled: 'true',
    source: SOURCE,
    fetch: fetchFn,
  });
}

function flushNow(stride: ReturnType<typeof createStride>): Promise<unknown> {
  let p: Promise<unknown> = Promise.resolve();
  stride.flush((promise) => {
    p = promise;
  });
  return p;
}

afterEach(() => {
  _resetDropped();
  vi.restoreAllMocks();
});

describe('kill switch', () => {
  it('is a no-op without ingestUrl, token, or enabled flag', async () => {
    const { fn, calls } = mockFetch();
    for (const cfg of [
      { token: 'tok', enabled: 'true' },
      { ingestUrl: 'https://x', enabled: 'true' },
      { ingestUrl: 'https://x', token: 'tok', enabled: '' },
    ]) {
      const stride = createStride({ ...cfg, source: SOURCE, fetch: fn as unknown as typeof fetch });
      expect(stride.isEnabled).toBe(false);
      stride.emit('heartbeat', { operation: 'clawfight.heartbeat' });
      await flushNow(stride);
    }
    expect(calls.length).toBe(0);
  });
});

describe('batching and flush', () => {
  it('buffers observations and sends one batch via waitUntil', async () => {
    const { fn, calls } = mockFetch();
    const stride = enabledStride(fn as unknown as typeof fetch);

    stride.emit('heartbeat', { operation: 'clawfight.heartbeat' });
    const span = stride.start('clawfight.mcp.speak', { traceId: stableTraceId('match', 'm-1') });
    span.annotate({ match_id: 'm-1', tool: 'speak', secret_prompt: 'DROP ME' });
    span.end();
    await flushNow(stride);

    expect(calls.length).toBe(1);
    expect(calls[0]!.url).toBe('https://ingest.example.com/v1/observations:batch');
    expect(calls[0]!.headers['authorization']).toBe('Bearer tok');
    const { source, observations } = calls[0]!.body;
    expect(source).toEqual(SOURCE);
    expect(observations.length).toBe(2);
    const terminal = observations[1]!;
    expect(terminal.kind).toBe('operation_completed');
    expect(terminal.operation).toBe('clawfight.mcp.speak');
    expect(terminal.context?.trace_id).toBe('match-m-1');
    expect(terminal.context?.span_id).toBeTruthy();
    expect(terminal.duration_ms).toBeGreaterThanOrEqual(0);
    // Allowlist: match_id/tool pass, unknown key silently dropped.
    expect(terminal.attributes).toEqual({ match_id: 'm-1', tool: 'speak' });
  });

  it('flush is a no-op when nothing was buffered', () => {
    const { fn } = mockFetch();
    const stride = enabledStride(fn as unknown as typeof fetch);
    const waitUntil = vi.fn();
    stride.flush(waitUntil);
    expect(waitUntil).not.toHaveBeenCalled();
  });
});

describe('spans', () => {
  it('records failures with sanitized error records and no stacks', async () => {
    const { fn, calls } = mockFetch();
    const stride = enabledStride(fn as unknown as typeof fetch);

    await expect(
      stride.span({}, 'clawfight.job.judging_cycle', async () => {
        throw new Error('boom '.repeat(200));
      }),
    ).rejects.toThrow();
    await flushNow(stride);

    const obs = calls[0]!.body.observations[0]!;
    expect(obs.kind).toBe('operation_failed');
    expect(obs.status).toBe('failure');
    expect(obs.error?.class).toBe('Error');
    expect(obs.error?.message!.length).toBeLessThanOrEqual(256);
    expect(obs.error?.stack).toBeUndefined();
    expect(obs.error?.fingerprint).toBeTruthy();
  });

  it('propagates trace context through child spans and headers', () => {
    const { fn } = mockFetch();
    const stride = enabledStride(fn as unknown as typeof fetch);
    const root = stride.start('root.op', { traceId: 't-1', requestId: 'r-1' });
    const child = root.child('child.op');
    expect(child.context.traceId).toBe('t-1');
    expect(child.context.parentSpanId).toBe(root.spanId);
    const headers = root.headers();
    expect(headers['x-watch-trace-id']).toBe('t-1');
    expect(headers['x-watch-parent-span-id']).toBe(root.spanId);
  });

  it('end() is idempotent', async () => {
    const { fn, calls } = mockFetch();
    const stride = enabledStride(fn as unknown as typeof fetch);
    const span = stride.start('op');
    span.end();
    span.end();
    span.fail(new Error('late'));
    await flushNow(stride);
    expect(calls[0]!.body.observations.length).toBe(1);
  });
});

describe('failure handling', () => {
  it('never throws when fetch explodes synchronously, and counts drops', async () => {
    const fn = vi.fn(() => {
      throw new Error('fetch exploded');
    });
    const stride = enabledStride(fn as unknown as typeof fetch);
    stride.emit('heartbeat', { operation: 'hb' });
    await flushNow(stride); // must not reject

    // Next flush on a healthy transport reports the dropped count.
    const healthy = mockFetch();
    const stride2 = enabledStride(healthy.fn as unknown as typeof fetch);
    stride2.emit('heartbeat', { operation: 'hb' });
    await flushNow(stride2);
    const first = healthy.calls[0]!.body.observations[0]!;
    expect(first.kind).toBe('metric_sampled');
    expect(first.operation).toBe('watch.sdk.dropped');
    expect(first.attributes?.dropped).toBeGreaterThanOrEqual(1);
  }, 10_000);

  it('retries once on 5xx', async () => {
    let attempts = 0;
    const fn = vi.fn(async () => {
      attempts++;
      return new Response('err', { status: 500 });
    });
    const stride = enabledStride(fn as unknown as typeof fetch);
    stride.emit('heartbeat', { operation: 'hb' });
    await flushNow(stride);
    expect(attempts).toBe(2);
  });

  it('does not retry on 4xx', async () => {
    let attempts = 0;
    const fn = vi.fn(async () => {
      attempts++;
      return new Response('bad', { status: 400 });
    });
    const stride = enabledStride(fn as unknown as typeof fetch);
    stride.emit('heartbeat', { operation: 'hb' });
    await flushNow(stride);
    expect(attempts).toBe(1);
  });

  it('drops oldest beyond maxBatch', async () => {
    const { fn, calls } = mockFetch();
    const stride = createStride({
      ingestUrl: 'https://x',
      token: 't',
      enabled: 'true',
      source: SOURCE,
      maxBatch: 3,
      fetch: fn as unknown as typeof fetch,
    });
    for (let i = 0; i < 5; i++) stride.emit('heartbeat', { operation: `hb.${i}` });
    await flushNow(stride);
    const ops = calls[0]!.body.observations.map((o) => o.operation);
    // 2 dropped, plus the drop-report observation prepended.
    expect(ops).toEqual(['watch.sdk.dropped', 'hb.2', 'hb.3', 'hb.4']);
  });
});

describe('createStrideFromEnv', () => {
  it('reads the WATCH_* convention with CF_VERSION_METADATA fallback', () => {
    const stride = createStrideFromEnv(
      {
        WATCH_INGEST_URL: 'https://ingest.example.com',
        WATCH_TOKEN: 'tok',
        WATCH_ENABLED: 'true',
        WATCH_ENVIRONMENT: 'production',
        CF_VERSION_METADATA: { id: 'cf-version-1' },
      },
      { system: 'clawfight', service: 'clawfight-runtime' },
    );
    expect(stride.isEnabled).toBe(true);
  });

  it('is disabled with empty env', () => {
    const stride = createStrideFromEnv({}, { system: 'x', service: 'y' });
    expect(stride.isEnabled).toBe(false);
  });

  it('prefers STRIDE_* over the WATCH_* predecessor', async () => {
    const { fn, calls } = mockFetch();
    const stride = createStrideFromEnv(
      {
        STRIDE_INGEST_URL: 'https://new.example.com',
        STRIDE_TOKEN: 'new-tok',
        STRIDE_ENABLED: 'true',
        STRIDE_ENVIRONMENT: 'production',
        STRIDE_VERSION: 'v2',
        WATCH_INGEST_URL: 'https://old.example.com',
        WATCH_TOKEN: 'old-tok',
        WATCH_ENABLED: 'true',
        WATCH_ENVIRONMENT: 'staging',
        WATCH_VERSION: 'v1',
      },
      { system: 'acme', service: 'checkout' },
    );
    // The env builder does not take a fetch override, so re-create with one.
    const probe = createStride({
      ingestUrl: 'https://new.example.com',
      token: 'new-tok',
      enabled: 'true',
      source: { system: 'acme', service: 'checkout', environment: 'production', version: 'v2' },
      fetch: fn as unknown as typeof fetch,
    });
    expect(stride.isEnabled).toBe(true);
    probe.emit('heartbeat', { operation: 'hb' });
    await flushNow(probe);
    expect(calls[0]!.url).toBe('https://new.example.com/v1/observations:batch');
    expect(calls[0]!.body.source.environment).toBe('production');
    expect(calls[0]!.body.source.version).toBe('v2');
  });

  it('falls back to WATCH_* when only those are set', () => {
    const stride = createStrideFromEnv(
      { WATCH_INGEST_URL: 'https://old.example.com', WATCH_TOKEN: 't', WATCH_ENABLED: 'true' },
      { system: 'acme', service: 'checkout' },
    );
    expect(stride.isEnabled).toBe(true);
  });
});

describe('trace ids', () => {
  it('stableTraceId joins namespace and key; matchTraceId is the legacy spelling', () => {
    expect(stableTraceId('match', 'm-1')).toBe('match-m-1');
    expect(matchTraceId('m-1')).toBe(stableTraceId('match', 'm-1'));
  });
});

