// Wire types mirroring the Go package `stride`, which is the contract; the
// fixture in ../../fixtures/batch.json keeps both sides honest.
//
// Every string literal and property name below is frozen: deployed receivers
// parse these names and stored rows are queried by them.

export type ObservationKind =
  | 'operation_started'
  | 'operation_completed'
  | 'operation_failed'
  | 'dependency_called'
  | 'dependency_failed'
  | 'job_enqueued'
  | 'job_started'
  | 'job_completed'
  | 'job_failed'
  | 'log_emitted'
  | 'metric_sampled'
  | 'deployment_started'
  | 'deployment_completed'
  | 'heartbeat';

export type ObservationStatus = 'success' | 'failure' | 'timeout' | 'cancelled' | 'partial' | 'unknown';

export interface SourceRef {
  system: string;
  service: string;
  instance?: string;
  environment: string;
  region?: string;
  version?: string;
}

export interface StrideContext {
  trace_id?: string;
  span_id?: string;
  parent_span_id?: string;
  request_id?: string;
  session_id?: string;
  user_id?: string;
  workspace_id?: string;
  deployment_id?: string;
  ark_version?: string;
}

export interface ErrorRecord {
  class?: string;
  code?: string;
  message?: string;
  fingerprint?: string;
  retryable?: boolean;
  stack?: string;
}

export type AttributeValue = string | number | boolean;

export interface Observation {
  id?: string;
  time: string; // RFC3339
  kind: ObservationKind;
  operation?: string;
  status?: ObservationStatus;
  duration_ms?: number;
  source?: SourceRef;
  context?: StrideContext;
  attributes?: Record<string, AttributeValue>;
  error?: ErrorRecord;
}

export interface BatchRequest {
  source: SourceRef;
  observations: Observation[];
}

export interface BatchResponse {
  accepted: number;
  dropped: number;
  errors?: { index: number; id?: string; reason: string }[];
}
