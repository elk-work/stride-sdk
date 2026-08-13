// Stride emitter for Cloudflare Workers (and any fetch-capable runtime).
//
// Design constraints, in order:
//   1. Never throw into product code — every public method is try/caught.
//   2. No background timers: buffer per instance, flush once via the injected
//      waitUntil at the end of the request / DO event / cron branch.
//   3. Privacy by construction: attributes pass an allowlist; unknown keys
//      are silently dropped and counted. No prompt bodies, no headers, no
//      query strings, no stacks.
//   4. Degrade by dropping, and report the drops (a metric_sampled
//      observation carrying the count).
//
// There is no default ingest URL: the same wire format serves more than one
// receiver, so the caller names the one it means.

import type {
  AttributeValue,
  ErrorRecord,
  Observation,
  ObservationKind,
  SourceRef,
  StrideContext,
  ObservationStatus,
} from './types.js';

export interface StrideConfig {
  ingestUrl?: string;
  token?: string;
  enabled?: boolean | string;
  source: SourceRef;
  /** Override the attribute allowlist (defaults to DEFAULT_ATTRIBUTE_KEYS). */
  allowedAttributes?: ReadonlySet<string>;
  /** Max buffered observations per instance; oldest are dropped beyond it. */
  maxBatch?: number;
  fetch?: typeof fetch;
}

/**
 * The allowlist applied when a caller supplies none.
 *
 * These keys are inherited from the SDK's first emitter and are deliberately
 * unchanged: narrowing the default would silently stop an existing consumer's
 * attributes from being recorded. New consumers should pass their own
 * `allowedAttributes` rather than rely on this set — it is a compatibility
 * default, not a recommendation.
 */
export const DEFAULT_ATTRIBUTE_KEYS: ReadonlySet<string> = new Set([
  'match_id',
  'agent_id',
  'tool',
  'model',
  'provider',
  'input_tokens',
  'output_tokens',
  'status_code',
  'cron',
  'retry',
  'reason',
  'game_mode',
  'dropped',
  'ws_count',
]);

const MAX_ERROR_MESSAGE = 256;
const DEFAULT_MAX_BATCH = 100;

/**
 * Deterministic trace id derived from a namespace and a domain key, for work
 * that spans requests and has no natural inbound trace to join —
 * `stableTraceId('match', matchId)` yields `match-<id>`. Two emitters that
 * derive the same id land in the same trace.
 */
export function stableTraceId(namespace: string, key: string): string {
  return `${namespace}-${key}`;
}

/**
 * @deprecated Use `stableTraceId('match', matchId)`. Kept so the SDK's first
 * consumer can adopt the published package without a code change; the output
 * is byte-identical.
 */
export function matchTraceId(matchId: string): string {
  return stableTraceId('match', matchId);
}

export type WaitUntil = (promise: Promise<unknown>) => void;

export interface SpanContext {
  traceId?: string;
  spanId?: string;
  parentSpanId?: string;
  requestId?: string;
  sessionId?: string;
  userId?: string;
  workspaceId?: string;
  deploymentId?: string;
}

export interface EmitFields {
  operation?: string;
  status?: ObservationStatus;
  duration_ms?: number;
  context?: SpanContext;
  attributes?: Record<string, unknown>;
  error?: ErrorRecord;
  time?: Date;
}

// Dropped observations survive instance reuse at module scope; an evicted
// isolate loses the count, which is acceptable — the counter is best-effort.
let droppedTotal = 0;

/** Test hook. */
export function _resetDropped(): void {
  droppedTotal = 0;
}

export class Span {
  private readonly stride: Stride;
  operation: string; // mutable: HTTP middleware names the span after routing
  readonly spanId: string;
  readonly context: SpanContext;
  private readonly startedAt: number;
  private attributes: Record<string, AttributeValue> = {};
  private done = false;

  constructor(stride: Stride, operation: string, context: SpanContext) {
    this.stride = stride;
    this.operation = operation;
    this.spanId = randomId();
    this.context = { ...context };
    this.startedAt = Date.now();
  }

  /** Attach allowlisted attributes to the terminal observation. */
  annotate(attrs: Record<string, unknown>): void {
    try {
      Object.assign(this.attributes, this.stride.filterAttributes(attrs));
    } catch {
      /* never throw */
    }
  }

  /** Start a child span (explicit chaining; no async context tricks). */
  child(operation: string): Span {
    return new Span(this.stride, operation, {
      ...this.context,
      parentSpanId: this.spanId,
    });
  }

  /** Headers for propagating trace context into a downstream fetch / DO stub. */
  headers(): Record<string, string> {
    const h: Record<string, string> = {};
    if (this.context.traceId) h['x-watch-trace-id'] = this.context.traceId;
    h['x-watch-parent-span-id'] = this.spanId;
    if (this.context.requestId) h['x-watch-request-id'] = this.context.requestId;
    return h;
  }

  /** Emit the terminal observation. Pass an error (or status) for failures. */
  end(opts?: { status?: ObservationStatus; error?: unknown; attributes?: Record<string, unknown> }): void {
    try {
      if (this.done) return;
      this.done = true;
      if (opts?.attributes) this.annotate(opts.attributes);
      const failed = opts?.error !== undefined || opts?.status === 'failure' || opts?.status === 'timeout';
      this.stride.emit(failed ? 'operation_failed' : 'operation_completed', {
        operation: this.operation,
        status: opts?.status ?? (failed ? 'failure' : 'success'),
        duration_ms: Math.max(0, Date.now() - this.startedAt),
        context: { ...this.context, spanId: this.spanId },
        attributes: this.attributes,
        error: opts?.error !== undefined ? toErrorRecord(opts.error) : undefined,
      });
    } catch {
      droppedTotal++;
    }
  }

  fail(error: unknown, opts?: { status?: ObservationStatus; retryable?: boolean }): void {
    const rec = toErrorRecord(error);
    if (opts?.retryable !== undefined) rec.retryable = opts.retryable;
    this.end({ status: opts?.status ?? 'failure', error: rec });
  }
}

export class Stride {
  private readonly config: StrideConfig;
  private readonly enabled: boolean;
  private readonly allowed: ReadonlySet<string>;
  private readonly maxBatch: number;
  private buffer: Observation[] = [];
  private readonly fetchFn: typeof fetch;

  constructor(config: StrideConfig) {
    this.config = config;
    this.allowed = config.allowedAttributes ?? DEFAULT_ATTRIBUTE_KEYS;
    this.maxBatch = config.maxBatch ?? DEFAULT_MAX_BATCH;
    this.enabled = Boolean(
      config.ingestUrl &&
        config.token &&
        (config.enabled === true || config.enabled === 'true'),
    );
    this.fetchFn = config.fetch ?? globalThis.fetch?.bind(globalThis);
  }

  get isEnabled(): boolean {
    return this.enabled;
  }

  /** Begin a span. Returns a handle; always call end() or fail(). */
  start(operation: string, context: SpanContext = {}): Span {
    const ctx = { ...context };
    if (!ctx.traceId) ctx.traceId = randomId();
    return new Span(this, operation, ctx);
  }

  /** Run fn inside a span; failure is recorded and rethrown. */
  async span<T>(context: SpanContext, operation: string, fn: (span: Span) => Promise<T>): Promise<T> {
    const span = this.start(operation, context);
    try {
      const result = await fn(span);
      span.end();
      return result;
    } catch (err) {
      span.fail(err);
      throw err;
    }
  }

  /** Buffer one observation of any kind. */
  emit(kind: ObservationKind, fields: EmitFields = {}): void {
    try {
      if (!this.enabled) return;
      const obs: Observation = {
        id: randomId(),
        time: (fields.time ?? new Date()).toISOString(),
        kind,
      };
      if (fields.operation) obs.operation = fields.operation;
      if (fields.status) obs.status = fields.status;
      if (fields.duration_ms !== undefined) obs.duration_ms = Math.max(0, Math.round(fields.duration_ms));
      const ctx = toWireContext(fields.context);
      if (ctx) obs.context = ctx;
      if (fields.attributes) {
        const attrs = this.filterAttributes(fields.attributes);
        if (Object.keys(attrs).length > 0) obs.attributes = attrs;
      }
      if (fields.error) obs.error = sanitizeError(fields.error);
      this.buffer.push(obs);
      if (this.buffer.length > this.maxBatch) {
        this.buffer.shift();
        droppedTotal++;
      }
    } catch {
      droppedTotal++;
    }
  }

  /**
   * Hand the buffered batch to the runtime. The only place network I/O
   * happens; call once at the end of the request / DO event / cron branch.
   */
  flush(waitUntil: WaitUntil): void {
    try {
      if (!this.enabled || this.buffer.length === 0) return;
      if (droppedTotal > 0) {
        const n = droppedTotal;
        droppedTotal = 0;
        this.buffer.unshift({
          id: randomId(),
          time: new Date().toISOString(),
          kind: 'metric_sampled',
          operation: 'watch.sdk.dropped',
          attributes: { dropped: n },
        });
      }
      const batch = this.buffer;
      this.buffer = [];
      waitUntil(this.send(batch));
    } catch {
      droppedTotal++;
    }
  }

  /**
   * Flush and return the send promise directly — for callers already running
   * under a waitUntil (cron branches, DO alarms) that should await delivery.
   */
  flushNow(): Promise<void> {
    let p: Promise<unknown> = Promise.resolve();
    this.flush((promise) => {
      p = promise;
    });
    return p.then(
      () => undefined,
      () => undefined,
    );
  }

  private async send(observations: Observation[]): Promise<void> {
    const body = JSON.stringify({ source: this.config.source, observations });
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const resp = await this.fetchFn(`${this.config.ingestUrl!.replace(/\/$/, '')}/v1/observations:batch`, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            authorization: `Bearer ${this.config.token}`,
          },
          body,
        });
        if (resp.ok) return;
        if (resp.status < 500) break; // validation/auth: retrying won't help
      } catch {
        // network error: fall through to retry
      }
      if (attempt === 0) await sleep(250);
    }
    droppedTotal += observations.length;
  }

  filterAttributes(attrs: Record<string, unknown>): Record<string, AttributeValue> {
    const out: Record<string, AttributeValue> = {};
    for (const [key, value] of Object.entries(attrs)) {
      if (!this.allowed.has(key)) continue; // unknown keys are silently dropped
      if (typeof value === 'string') out[key] = value.slice(0, 1024);
      else if (typeof value === 'number' && Number.isFinite(value)) out[key] = value;
      else if (typeof value === 'boolean') out[key] = value;
    }
    return out;
  }
}

export function createStride(config: StrideConfig): Stride {
  return new Stride(config);
}

/**
 * Build a Stride from the STRIDE_* environment convention:
 * STRIDE_INGEST_URL, STRIDE_TOKEN, STRIDE_ENABLED, STRIDE_ENVIRONMENT,
 * STRIDE_VERSION (falling back to CF_VERSION_METADATA.id, then 'dev').
 *
 * Each name falls back to its WATCH_* predecessor when the STRIDE_* one is
 * unset, so a deployment instrumented before the rename keeps working
 * untouched. Renaming a live deployment's variables is a secret-and-URL move
 * that must happen in one step, never as a side effect of upgrading the SDK.
 *
 * `opts.allowedAttributes` widens (replaces) the attribute allowlist for
 * emitters whose vocabulary differs from the default — e.g. a browser-error
 * relay route carrying message/url/line/col fields no server-side span uses.
 * Callers own the privacy posture of any widened set.
 */
export function createStrideFromEnv(
  env: Record<string, unknown>,
  source: { system: string; service: string },
  opts?: { allowedAttributes?: ReadonlySet<string> },
): Stride {
  const raw = (k: string): string | undefined =>
    typeof env[k] === 'string' && env[k] !== '' ? (env[k] as string) : undefined;
  // STRIDE_X wins; WATCH_X is the pre-rename spelling and still honoured.
  const str = (suffix: string): string | undefined =>
    raw(`STRIDE_${suffix}`) ?? raw(`WATCH_${suffix}`);
  const meta = env['CF_VERSION_METADATA'] as { id?: string } | undefined;
  return createStride({
    ingestUrl: str('INGEST_URL'),
    token: str('TOKEN'),
    enabled: str('ENABLED'),
    allowedAttributes: opts?.allowedAttributes,
    source: {
      system: source.system,
      service: source.service,
      environment: str('ENVIRONMENT') ?? 'dev',
      version: str('VERSION') ?? meta?.id ?? 'dev',
    },
  });
}

function toWireContext(ctx: SpanContext | undefined): StrideContext | undefined {
  if (!ctx) return undefined;
  const out: StrideContext = {};
  if (ctx.traceId) out.trace_id = ctx.traceId;
  if (ctx.spanId) out.span_id = ctx.spanId;
  if (ctx?.parentSpanId) out.parent_span_id = ctx.parentSpanId;
  if (ctx?.requestId) out.request_id = ctx.requestId;
  if (ctx?.sessionId) out.session_id = ctx.sessionId;
  if (ctx?.userId) out.user_id = ctx.userId;
  if (ctx?.workspaceId) out.workspace_id = ctx.workspaceId;
  if (ctx?.deploymentId) out.deployment_id = ctx.deploymentId;
  return Object.keys(out).length > 0 ? out : undefined;
}

function toErrorRecord(err: unknown): ErrorRecord {
  if (err && typeof err === 'object' && 'class' in (err as Record<string, unknown>)) {
    return sanitizeError(err as ErrorRecord);
  }
  if (err instanceof Error) {
    return sanitizeError({
      class: err.name || 'Error',
      message: err.message,
      fingerprint: fingerprintOf(err),
    });
  }
  return { class: 'Error', message: String(err).slice(0, MAX_ERROR_MESSAGE) };
}

function sanitizeError(rec: ErrorRecord): ErrorRecord {
  const out: ErrorRecord = {};
  if (rec.class) out.class = rec.class;
  if (rec.code) out.code = rec.code;
  if (rec.message) out.message = rec.message.slice(0, MAX_ERROR_MESSAGE);
  if (rec.fingerprint) out.fingerprint = rec.fingerprint;
  if (rec.retryable !== undefined) out.retryable = rec.retryable;
  // Stacks are never sent from Workers (privacy posture).
  return out;
}

// Client-side fingerprint: error name + first meaningful stack frame. Cheap,
// stable, and never derived from the (variable) message text.
function fingerprintOf(err: Error): string | undefined {
  const frame = err.stack?.split('\n')[1]?.trim();
  if (!frame) return err.name || undefined;
  return `${err.name}@${frame.slice(0, 120)}`;
}

function randomId(): string {
  try {
    return crypto.randomUUID();
  } catch {
    return `id-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
