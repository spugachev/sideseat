//! Normalized data models for analytics storage
//!
//! These types represent the normalized form of OTEL data after extraction
//! and enrichment, ready for storage in any analytics backend.

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

use super::{AggregationTemporality, Framework, MetricType, ObservationType, SpanCategory};

/// Helper to serialize a JsonValue to an Option<String>, returning None for null.
pub fn json_to_pre_serialized(value: &JsonValue) -> Option<String> {
    if value.is_null() {
        None
    } else {
        serde_json::to_string(value).ok()
    }
}

// ============================================================================
// NORMALIZED METRIC
// ============================================================================

/// Normalized metric for analytics storage
/// One row per data point (flattened from OTLP metric structures)
#[derive(Debug, Clone, Default)]
pub struct NormalizedMetric {
    // Identity
    pub project_id: Option<String>,
    /// What makes this datapoint *this* datapoint - see `domain::metrics::identity`. ClickHouse sorts
    /// its replacing engine by it and DuckDB deduplicates on it; without one, two labelled series
    /// recorded at the same instant were one row.
    pub datapoint_id: String,
    pub metric_name: String,
    pub metric_description: Option<String>,
    pub metric_unit: Option<String>,

    // Type & Aggregation
    pub metric_type: MetricType,
    pub aggregation_temporality: AggregationTemporality,
    pub is_monotonic: Option<bool>,

    // Timing
    pub timestamp: DateTime<Utc>,
    pub start_timestamp: Option<DateTime<Utc>>,

    // Value (for Gauge/Sum)
    pub value_int: Option<i64>,
    pub value_double: Option<f64>,

    // Histogram aggregates
    pub histogram_count: Option<u64>,
    pub histogram_sum: Option<f64>,
    pub histogram_min: Option<f64>,
    pub histogram_max: Option<f64>,
    pub histogram_bucket_counts: JsonValue,
    pub histogram_explicit_bounds: JsonValue,

    // Exponential histogram specific
    pub exp_histogram_scale: Option<i32>,
    pub exp_histogram_zero_count: Option<u64>,
    pub exp_histogram_zero_threshold: Option<f64>,
    pub exp_histogram_positive: JsonValue,
    pub exp_histogram_negative: JsonValue,

    // Summary specific
    pub summary_count: Option<u64>,
    pub summary_sum: Option<f64>,
    pub summary_quantiles: JsonValue,

    // Exemplar (first one for trace correlation)
    pub exemplar_trace_id: Option<String>,
    pub exemplar_span_id: Option<String>,
    pub exemplar_value_int: Option<i64>,
    pub exemplar_value_double: Option<f64>,
    pub exemplar_timestamp: Option<DateTime<Utc>>,
    pub exemplar_attributes: JsonValue,
    /// Every exemplar of the data point, as a JSON array; `Null` when there are none.
    ///
    /// The flat `exemplar_*` fields above carry the first one, for the trace-correlation index. This
    /// carries them all, because a histogram exemplar is per bucket and dropping the rest throws away
    /// most of the trace links the exporter sent.
    pub exemplars: JsonValue,

    // Context
    pub session_id: Option<String>,
    pub user_id: Option<String>,
    pub environment: Option<String>,

    // Resource
    pub service_name: Option<String>,
    pub service_version: Option<String>,
    pub service_namespace: Option<String>,
    pub service_instance_id: Option<String>,

    // Scope
    pub scope_name: Option<String>,
    pub scope_version: Option<String>,

    // Attributes
    pub attributes: JsonValue,
    pub resource_attributes: JsonValue,

    // Flags
    pub flags: u32,

    // Raw
    pub raw_metric: JsonValue,

    // Instrumentation scope and schema, which OTel counts as part of what names a stream.
    //
    // Extracted late: they were discarded entirely, so two scopes differing only in their attributes - or
    // two resources differing only in `schema_url` - produced datapoints with the same identity, and a
    // replacing engine kept one. Stored as well as hashed, because discarding telemetry to save a column
    // is how the gap arose in the first place.
    pub scope_attributes: JsonValue,
    pub scope_schema_url: Option<String>,
    pub resource_schema_url: Option<String>,
}

// ============================================================================
// NORMALIZED SPAN
// ============================================================================

/// Normalized span for analytics storage
#[derive(Debug, Clone, Default)]
pub struct NormalizedSpan {
    // Identity
    pub project_id: Option<String>,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub trace_state: Option<String>,

    // Session and user
    pub session_id: Option<String>,
    pub user_id: Option<String>,

    // Naming and classification
    pub span_name: String,
    pub span_kind: Option<String>,
    pub span_category: Option<SpanCategory>,
    pub observation_type: Option<ObservationType>,
    pub framework: Option<Framework>,
    /// Instrumentation scope, the same pair `NormalizedMetric` has always carried: the library that
    /// produced the span, versioned. Declared *last* in both analytics schemas (a migration can only
    /// append), which is why they sit apart from the classification block they belong to logically.
    pub scope_name: Option<String>,
    pub scope_version: Option<String>,
    pub status_code: Option<String>,
    pub status_message: Option<String>,
    pub exception_type: Option<String>,
    pub exception_message: Option<String>,
    pub exception_stacktrace: Option<String>,

    // Time
    pub timestamp_start: DateTime<Utc>,
    pub timestamp_end: Option<DateTime<Utc>>,
    pub duration_ms: i64,

    // Environment
    pub environment: Option<String>,

    // GenAI core fields
    pub gen_ai_system: Option<String>,
    pub gen_ai_operation_name: Option<String>,
    pub gen_ai_request_model: Option<String>,
    pub gen_ai_response_model: Option<String>,
    pub gen_ai_response_id: Option<String>,

    // GenAI request parameters
    pub gen_ai_temperature: Option<f64>,
    pub gen_ai_top_p: Option<f64>,
    pub gen_ai_top_k: Option<i64>,
    pub gen_ai_max_tokens: Option<i64>,
    pub gen_ai_frequency_penalty: Option<f64>,
    pub gen_ai_presence_penalty: Option<f64>,
    pub gen_ai_stop_sequences: Vec<String>,

    // GenAI response
    pub gen_ai_finish_reasons: Vec<String>,

    // GenAI agent fields
    pub gen_ai_agent_id: Option<String>,
    pub gen_ai_agent_name: Option<String>,

    // GenAI tool fields
    pub gen_ai_tool_name: Option<String>,
    pub gen_ai_tool_call_id: Option<String>,

    // GenAI performance metrics
    pub gen_ai_server_ttft_ms: Option<i64>,
    pub gen_ai_server_request_duration_ms: Option<i64>,

    // Token usage (gen_ai.usage.*) - defaults to 0, never NULL
    pub gen_ai_usage_input_tokens: i64,
    pub gen_ai_usage_output_tokens: i64,
    pub gen_ai_usage_total_tokens: i64,

    // Cache token usage - defaults to 0, never NULL
    pub gen_ai_usage_cache_read_tokens: i64,
    pub gen_ai_usage_cache_write_tokens: i64,

    // Reasoning token usage - defaults to 0, never NULL
    pub gen_ai_usage_reasoning_tokens: i64,

    // Usage details (provider-specific overflow, pre-serialized JSON)
    pub gen_ai_usage_details: Option<String>,

    // Cost fields - defaults to 0.0, never NULL
    pub gen_ai_cost_input: f64,
    pub gen_ai_cost_output: f64,
    pub gen_ai_cost_cache_read: f64,
    pub gen_ai_cost_cache_write: f64,
    pub gen_ai_cost_reasoning: f64,
    pub gen_ai_cost_total: f64,

    // External services
    pub http_method: Option<String>,
    pub http_url: Option<String>,
    pub http_status_code: Option<i64>,

    pub db_system: Option<String>,
    pub db_name: Option<String>,
    pub db_operation: Option<String>,
    pub db_statement: Option<String>,

    pub storage_system: Option<String>,
    pub storage_bucket: Option<String>,
    pub storage_object: Option<String>,

    pub messaging_system: Option<String>,
    pub messaging_destination: Option<String>,

    // Tags and metadata (pre-serialized JSON)
    pub tags: Vec<String>,
    pub metadata: Option<String>,

    // Preview text for trace list display (extracted from events)
    pub input_preview: Option<String>,
    pub output_preview: Option<String>,

    // Raw messages (converted to SideML on query, pre-serialized JSON string)
    // None means "[]" at write time
    pub messages: Option<String>,

    // Raw tool definitions (separate from conversation messages, pre-serialized JSON string)
    // None means "[]" at write time
    pub tool_definitions: Option<String>,

    // Raw tool names (list of tool names, separate from full definitions, pre-serialized JSON string)
    // None means "[]" at write time
    pub tool_names: Option<String>,

    // Raw span JSON (includes attributes and resource.attributes, pre-serialized JSON)
    pub raw_span: Option<String>,

    // Ingestion time (server time when span was received, for feed cursor)
    // Note: Not used in insert - populated by DB default (now())
    pub ingested_at: Option<DateTime<Utc>>,
}
