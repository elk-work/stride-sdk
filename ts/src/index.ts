export {
  Stride,
  Span,
  createStride,
  createStrideFromEnv,
  stableTraceId,
  matchTraceId,
  DEFAULT_ATTRIBUTE_KEYS,
  _resetDropped,
} from './stride.js';
export type { StrideConfig, SpanContext, EmitFields, WaitUntil } from './stride.js';
export type {
  Observation,
  ObservationKind,
  ObservationStatus,
  SourceRef,
  StrideContext,
  ErrorRecord,
  BatchRequest,
  BatchResponse,
  AttributeValue,
} from './types.js';
