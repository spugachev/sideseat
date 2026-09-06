//! Trace processing pipeline
//!
//! This module contains the trace processing pipeline:
//!
//! - `extract` - Stage 1: Parse protobuf, extract GenAI attributes and messages
//! - `enrich` - Stage 3: Cost calculation and preview extraction
//! - `persist` - Stage 4: Build raw span JSON, SSE publishing, DuckDB writes
//! - `pipeline` - Pipeline orchestrator
//!
//! Note: Stage 2 (SideML) is in the `domain::sideml` module.

mod enrich;
mod extract;
mod persist;
mod pipeline;

// Public API - only types needed by external modules
pub use extract::{MessageSource, RawMessage};
pub use persist::SseSpanEvent;
pub use pipeline::{DropReason, IngestOutcome, TracePipeline, strip_unstorable_spans};

// Internal re-exports for use within domain crate
pub(crate) use extract::SpanData;

// ============================================================================
// Test-only replay bridge
// ============================================================================

/// Run the real ingestion path over one OTLP request and return the rows the message API
/// would later read, paired with each span's name.
///
/// Exists so `message_goldens_tests` can replay captured sample payloads through the actual
/// extraction, SideML conversion and enrichment stages rather than a reimplementation of
/// them. Reimplementing would defeat the purpose: the point is to catch changes in this
/// pipeline, so the test has to go through it.
///
/// File extraction is disabled - it writes to disk and none of the message properties under
/// test depend on it.
#[cfg(test)]
pub(crate) fn normalize_for_test(
    request: &opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
    pricing: &crate::domain::pricing::PricingService,
) -> Vec<(String, crate::data::types::MessageSpanRow)> {
    normalize_for_test_with_mode(request, pricing, extract::ExtractionMode::PerCarrier)
}

/// As [`normalize_for_test`], with the extraction mode chosen by the caller, so a test can compare
/// what the two share out of a span's attributes.
#[cfg(test)]
pub(crate) fn normalize_for_test_with_mode(
    request: &opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
    pricing: &crate::domain::pricing::PricingService,
    mode: extract::ExtractionMode,
) -> Vec<(String, crate::data::types::MessageSpanRow)> {
    use crate::data::types::MessageSpanRow;

    let Some((spans, _pending)) =
        pipeline::process_request_for_test_with_mode(request, pricing, mode)
    else {
        return Vec::new();
    };

    spans
        .into_iter()
        .map(|s| {
            let row = MessageSpanRow {
                trace_id: s.trace_id.clone(),
                span_id: s.span_id.clone(),
                parent_span_id: s.parent_span_id.clone(),
                span_timestamp: s.timestamp_start,
                span_end_timestamp: s.timestamp_end,
                messages_json: s.messages.clone().unwrap_or_else(|| "[]".to_string()),
                tool_definitions_json: s
                    .tool_definitions
                    .clone()
                    .unwrap_or_else(|| "[]".to_string()),
                tool_names_json: s.tool_names.clone().unwrap_or_else(|| "[]".to_string()),
                model: s
                    .gen_ai_response_model
                    .clone()
                    .or_else(|| s.gen_ai_request_model.clone()),
                provider: s.gen_ai_system.clone(),
                status_code: s.status_code.clone(),
                exception_type: s.exception_type.clone(),
                exception_message: s.exception_message.clone(),
                exception_stacktrace: s.exception_stacktrace.clone(),
                input_tokens: s.gen_ai_usage_input_tokens,
                output_tokens: s.gen_ai_usage_output_tokens,
                total_tokens: s.gen_ai_usage_total_tokens,
                cost_total: s.gen_ai_cost_total,
                observation_type: s.observation_type.map(|o| o.as_str().to_string()),
                session_id: s.session_id.clone(),
                ingested_at: s.timestamp_start,
                scope_name: None,
                scope_version: None,
                span_name: None,
                framework: None,
                response_model: None,
                response_id: None,
                temperature: None,
                top_p: None,
                max_tokens: None,
                finish_reasons: None,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
                reasoning_tokens: 0,
                cost_input: 0.0,
                cost_output: 0.0,
            };
            (s.span_name.clone(), row)
        })
        .collect()
}

#[cfg(test)]
#[path = "message_goldens_tests.rs"]
mod message_goldens_tests;
#[cfg(test)]
pub(crate) mod source_program;
#[cfg(test)]
mod source_program_tests;
