//! Shared analytics types for all database backends
//!
//! This module contains query result types and parameters that are used
//! by both DuckDB and ClickHouse backends.

use chrono::{DateTime, Utc};

use crate::api::routes::otel::filters::Filter;
use crate::api::types::OrderBy;

// ============================================================================
// Row types (query results)
// ============================================================================

/// Result row for trace queries
#[derive(Debug)]
pub struct TraceRow {
    pub trace_id: String,
    pub trace_name: Option<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub session_id: Option<String>,
    pub user_id: Option<String>,
    pub environment: Option<String>,
    pub span_count: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reasoning_tokens: i64,
    pub input_cost: f64,
    pub output_cost: f64,
    pub cache_read_cost: f64,
    pub cache_write_cost: f64,
    pub reasoning_cost: f64,
    pub total_cost: f64,
    pub tags: Vec<String>,
    pub observation_count: i64,
    pub metadata: Option<String>,
    pub input_preview: Option<String>,
    pub output_preview: Option<String>,
    pub has_error: bool,
}

/// Result row from otel_spans table
#[derive(Debug)]
pub struct SpanRow {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub span_name: Option<String>,
    pub span_kind: Option<String>,
    pub span_category: Option<String>,
    pub observation_type: Option<String>,
    pub framework: Option<String>,
    pub status_code: Option<String>,
    pub timestamp_start: DateTime<Utc>,
    pub timestamp_end: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub environment: Option<String>,
    pub resource_attributes: Option<String>,
    pub session_id: Option<String>,
    pub user_id: Option<String>,
    pub gen_ai_system: Option<String>,
    pub gen_ai_request_model: Option<String>,
    pub gen_ai_agent_name: Option<String>,
    pub gen_ai_finish_reasons: Vec<String>,
    pub gen_ai_usage_input_tokens: i64,
    pub gen_ai_usage_output_tokens: i64,
    pub gen_ai_usage_total_tokens: i64,
    pub gen_ai_usage_cache_read_tokens: i64,
    pub gen_ai_usage_cache_write_tokens: i64,
    pub gen_ai_usage_reasoning_tokens: i64,
    pub gen_ai_cost_input: f64,
    pub gen_ai_cost_output: f64,
    pub gen_ai_cost_cache_read: f64,
    pub gen_ai_cost_cache_write: f64,
    pub gen_ai_cost_reasoning: f64,
    pub gen_ai_cost_total: f64,
    pub gen_ai_usage_details: Option<String>,
    pub metadata: Option<String>,
    pub attributes: Option<String>,
    pub input_preview: Option<String>,
    pub output_preview: Option<String>,
    pub raw_span: Option<String>,
    pub ingested_at: DateTime<Utc>,
    /// Instrumentation scope: the library that produced the span, versioned. `None` on rows written
    /// before schema v4, which genuinely did not record it.
    pub scope_name: Option<String>,
    pub scope_version: Option<String>,
}

/// Span counts (events and links) for bulk operations
#[derive(Debug, Clone, Default)]
pub struct SpanCounts {
    pub event_count: i64,
    pub link_count: i64,
}

/// Result row for span events (extracted from raw_span JSON)
#[derive(Debug, Clone)]
pub struct EventRow {
    pub span_id: String,
    pub event_index: i32,
    pub event_time: DateTime<Utc>,
    pub event_name: Option<String>,
    pub attributes: Option<String>,
}

/// Result row for span links (extracted from raw_span JSON)
#[derive(Debug, Clone)]
pub struct LinkRow {
    pub span_id: String,
    pub linked_trace_id: String,
    pub linked_span_id: String,
    pub attributes: Option<String>,
}

/// Result row for session queries
#[derive(Debug)]
pub struct SessionRow {
    pub session_id: String,
    pub user_id: Option<String>,
    pub environment: Option<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub trace_count: i64,
    pub span_count: i64,
    pub observation_count: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reasoning_tokens: i64,
    pub input_cost: f64,
    pub output_cost: f64,
    pub cache_read_cost: f64,
    pub cache_write_cost: f64,
    pub reasoning_cost: f64,
    pub total_cost: f64,
}

/// Observation token counts
#[derive(Debug, Clone, Default)]
pub struct ObservationTokens {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reasoning_tokens: i64,
}

// ============================================================================
// Query parameters
// ============================================================================

/// Parameters for list_traces query
#[derive(Debug, Default, Clone)]
pub struct ListTracesParams {
    pub project_id: String,
    pub page: u32,
    pub limit: u32,
    pub order_by: Option<OrderBy>,
    pub session_id: Option<String>,
    pub user_id: Option<String>,
    pub environment: Option<Vec<String>>,
    pub from_timestamp: Option<DateTime<Utc>>,
    pub to_timestamp: Option<DateTime<Utc>>,
    pub filters: Vec<Filter>,
    /// Include non-GenAI traces (default: false, showing only GenAI traces)
    pub include_nongenai: bool,
}

/// Parameters for list_spans query
#[derive(Debug, Default, Clone)]
pub struct ListSpansParams {
    pub project_id: String,
    pub page: u32,
    pub limit: u32,
    pub order_by: Option<OrderBy>,
    pub trace_id: Option<String>,
    pub session_id: Option<String>,
    pub user_id: Option<String>,
    pub environment: Option<Vec<String>>,
    pub span_category: Option<String>,
    pub observation_type: Option<String>,
    pub framework: Option<String>,
    pub gen_ai_request_model: Option<String>,
    pub status_code: Option<String>,
    pub from_timestamp: Option<DateTime<Utc>>,
    pub to_timestamp: Option<DateTime<Utc>>,
    pub filters: Vec<Filter>,
    /// Filter to observations only (spans with observation_type OR gen_ai_request_model)
    pub is_observation: Option<bool>,
}

/// Parameters for feed spans query (cursor-based pagination)
#[derive(Debug, Default, Clone)]
pub struct FeedSpansParams {
    pub project_id: String,
    /// Maximum number of spans to return
    pub limit: u32,
    /// Cursor for pagination: (ingested_at_us, span_id, trace_id).
    ///
    /// The trace id is part of the key because a span id is unique only within a trace.
    pub cursor: Option<(i64, String, String)>,
    /// Filter by event time >= start_time
    pub start_time: Option<DateTime<Utc>>,
    /// Filter by event time < end_time
    pub end_time: Option<DateTime<Utc>>,
    /// Filter to observations only (spans with observation_type OR gen_ai_request_model)
    pub is_observation: Option<bool>,
    /// Ignore spans ingested at or after this instant, in microseconds since the epoch.
    ///
    /// The traversal watermark, established on the first page and carried by the cursor. Without it a
    /// span ingested mid-traversal could appear on a later page after already having been counted in an
    /// earlier page's reconstruction context - and the same watermark has to bound the context load, or
    /// the two disagree about what exists.
    pub ingested_before_us: Option<i64>,
}

/// Parameters for list_sessions query
#[derive(Debug, Default, Clone)]
pub struct ListSessionsParams {
    pub project_id: String,
    pub page: u32,
    pub limit: u32,
    pub order_by: Option<OrderBy>,
    pub user_id: Option<String>,
    pub environment: Option<Vec<String>>,
    pub from_timestamp: Option<DateTime<Utc>>,
    pub to_timestamp: Option<DateTime<Utc>>,
    pub filters: Vec<Filter>,
}

// ============================================================================
// Helper functions
// ============================================================================

/// Find root span from a list of spans (parent_span_id is None)
pub fn find_root_span(spans: &[SpanRow]) -> Option<&SpanRow> {
    spans.iter().find(|s| s.parent_span_id.is_none())
}

/// Check if a span qualifies as an observation (GenAI span)
/// A span is an observation if it has a non-"span" observation_type
pub fn is_observation(span: &SpanRow) -> bool {
    span.observation_type.as_ref().is_some_and(|t| t != "span")
}

/// Extract observation type from span
pub fn get_observation_type(span: &SpanRow) -> Option<String> {
    span.observation_type.clone()
}

/// Calculate observation cost (gen_ai_cost_total)
pub fn get_observation_cost(span: &SpanRow) -> f64 {
    span.gen_ai_cost_total
}

/// Extract token usage from span
pub fn get_observation_tokens(span: &SpanRow) -> ObservationTokens {
    ObservationTokens {
        input_tokens: span.gen_ai_usage_input_tokens,
        output_tokens: span.gen_ai_usage_output_tokens,
        total_tokens: span.gen_ai_usage_total_tokens,
        cache_read_tokens: span.gen_ai_usage_cache_read_tokens,
        cache_write_tokens: span.gen_ai_usage_cache_write_tokens,
        reasoning_tokens: span.gen_ai_usage_reasoning_tokens,
    }
}

/// Filter spans that are observations
pub fn filter_observations(spans: &[SpanRow]) -> Vec<&SpanRow> {
    spans.iter().filter(|s| is_observation(s)).collect()
}

/// Trait for types that can be deduplicated by (trace_id, span_id).
pub trait SpanIdentity {
    fn trace_id(&self) -> &str;
    fn span_id(&self) -> &str;
    fn ordering_timestamp(&self) -> DateTime<Utc>;
}

impl SpanIdentity for SpanRow {
    fn trace_id(&self) -> &str {
        &self.trace_id
    }
    fn span_id(&self) -> &str {
        &self.span_id
    }
    fn ordering_timestamp(&self) -> DateTime<Utc> {
        self.timestamp_start
    }
}

/// Deduplicate items by (trace_id, span_id), keeping the first occurrence.
///
/// Preserves input order (from SQL ORDER BY). SQL queries already sort by
/// ingested_at or timestamp_start, so the first occurrence is the correct one.
pub fn deduplicate_by_span_identity<T: SpanIdentity>(items: Vec<T>) -> Vec<T> {
    let mut seen = rustc_hash::FxHashSet::default();
    items
        .into_iter()
        .filter(|item| seen.insert((item.trace_id().to_string(), item.span_id().to_string())))
        .collect()
}

/// Parse tags from JSON string
pub fn parse_tags(s: &Option<String>) -> Vec<String> {
    s.as_ref()
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default()
}

/// Parse finish reasons from JSON string
pub fn parse_finish_reasons(s: &Option<String>) -> Vec<String> {
    s.as_ref()
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_traces_params_default() {
        let params = ListTracesParams::default();
        assert_eq!(params.page, 0);
        assert_eq!(params.limit, 0);
    }

    #[test]
    fn test_parse_tags() {
        assert_eq!(parse_tags(&None), Vec::<String>::new());
        assert_eq!(parse_tags(&Some("[]".to_string())), Vec::<String>::new());
        assert_eq!(
            parse_tags(&Some(r#"["a", "b"]"#.to_string())),
            vec!["a".to_string(), "b".to_string()]
        );
    }
}
