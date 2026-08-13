// Package stride defines the Stride wire protocol: the JSON shapes accepted
// by POST {endpoint}/v1/observations:batch. It is the contract the Go,
// TypeScript and Python SDKs in this repository all mirror, and the contract
// the receivers on the other end parse.
//
// # The wire format is frozen
//
// Every JSON tag, every observation kind, and every status string in this
// package is a compatibility commitment. Deployed receivers parse these
// names, and stored observations are queried by them — column projections
// read payload->>'duration_ms', payload->'source'->>'version', kind,
// operation and status verbatim. Go identifiers may be renamed; the strings
// on the wire may not.
//
// The endpoint is never defaulted here. Callers supply it, because the same
// wire format serves more than one receiver.
package stride

import "time"

// ObservationKind names the boundary event an observation records.
const (
	KindOperationStarted    = "operation_started"
	KindOperationCompleted  = "operation_completed"
	KindOperationFailed     = "operation_failed"
	KindDependencyCalled    = "dependency_called"
	KindDependencyFailed    = "dependency_failed"
	KindJobEnqueued         = "job_enqueued"
	KindJobStarted          = "job_started"
	KindJobCompleted        = "job_completed"
	KindJobFailed           = "job_failed"
	KindLogEmitted          = "log_emitted"
	KindMetricSampled       = "metric_sampled"
	KindDeploymentStarted   = "deployment_started"
	KindDeploymentCompleted = "deployment_completed"
	KindHeartbeat           = "heartbeat"
)

// Kinds is the closed set of observation kinds.
var Kinds = map[string]bool{
	KindOperationStarted: true, KindOperationCompleted: true, KindOperationFailed: true,
	KindDependencyCalled: true, KindDependencyFailed: true,
	KindJobEnqueued: true, KindJobStarted: true, KindJobCompleted: true, KindJobFailed: true,
	KindLogEmitted: true, KindMetricSampled: true,
	KindDeploymentStarted: true, KindDeploymentCompleted: true, KindHeartbeat: true,
}

// Status is the outcome of attempted work.
const (
	StatusSuccess   = "success"
	StatusFailure   = "failure"
	StatusTimeout   = "timeout"
	StatusCancelled = "cancelled"
	StatusPartial   = "partial"
	StatusUnknown   = "unknown"
)

// Statuses is the closed set of outcomes.
var Statuses = map[string]bool{
	StatusSuccess: true, StatusFailure: true, StatusTimeout: true,
	StatusCancelled: true, StatusPartial: true, StatusUnknown: true,
}

// SourceRef identifies the thing that produced an observation. Version should
// resolve to an Ark promotion (merge commit SHA) or immutable artifact digest.
type SourceRef struct {
	System      string `json:"system"`
	Service     string `json:"service"`
	Instance    string `json:"instance,omitempty"`
	Environment string `json:"environment"`
	Region      string `json:"region,omitempty"`
	Version     string `json:"version,omitempty"`
}

// Context carries the identifiers that connect observations. Every field is
// optional; absent values are preserved as absent, never invented.
type Context struct {
	TraceID      string `json:"trace_id,omitempty"`
	SpanID       string `json:"span_id,omitempty"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
	RequestID    string `json:"request_id,omitempty"`
	SessionID    string `json:"session_id,omitempty"`
	UserID       string `json:"user_id,omitempty"`
	WorkspaceID  string `json:"workspace_id,omitempty"`
	DeploymentID string `json:"deployment_id,omitempty"`
	ArkVersion   string `json:"ark_version,omitempty"`
}

// ErrorRecord is a normalized description of failure. Fingerprint groups
// equivalent failures; when absent the server derives one from stable
// properties (class, code, operation), never from message text.
type ErrorRecord struct {
	Class       string `json:"class,omitempty"`
	Code        string `json:"code,omitempty"`
	Message     string `json:"message,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	Retryable   bool   `json:"retryable,omitempty"`
	Stack       string `json:"stack,omitempty"`
}

// Observation is an immutable fact received by a Stride receiver.
type Observation struct {
	ID         string         `json:"id,omitempty"` // client ULID; server assigns when absent
	Time       time.Time      `json:"time"`
	Kind       string         `json:"kind"`
	Operation  string         `json:"operation,omitempty"`
	Status     string         `json:"status,omitempty"` // defaults to unknown
	DurationMS *int64         `json:"duration_ms,omitempty"`
	Source     *SourceRef     `json:"source,omitempty"` // overrides the batch source
	Context    Context        `json:"context,omitzero"`
	Attributes map[string]any `json:"attributes,omitempty"` // scalar values only
	Error      *ErrorRecord   `json:"error,omitempty"`
}

// BatchRequest is the body of POST /v1/observations:batch.
type BatchRequest struct {
	Source       SourceRef     `json:"source"`
	Observations []Observation `json:"observations"`
}

// BatchLimit is the maximum number of observations accepted per batch.
const BatchLimit = 1000

// ItemError explains one dropped observation.
type ItemError struct {
	Index  int    `json:"index"`
	ID     string `json:"id,omitempty"`
	Reason string `json:"reason"`
}

// BatchResponse reports per-item outcomes. The envelope succeeds (200) even
// when individual observations were dropped.
type BatchResponse struct {
	Accepted int         `json:"accepted"`
	Dropped  int         `json:"dropped"`
	Errors   []ItemError `json:"errors,omitempty"`
}

// Error is the JSON error body for non-200 responses.
type Error struct {
	Code    string `json:"code"` // validation | permission | too_large | not_found | internal
	Message string `json:"message"`
}
