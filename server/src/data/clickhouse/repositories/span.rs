//! ClickHouse span repository
//!
//! Provides high-throughput batch writes for normalized spans.

use chrono::Utc;
use clickhouse::Client;
use clickhouse::Row;
use serde::Serialize;

use crate::data::clickhouse::ClickhouseError;
use crate::data::types::NormalizedSpan;
use crate::utils::clickhouse::to_decimal64;

/// Row structure for inserting spans into ClickHouse
#[derive(Row, Serialize)]
struct SpanRow {
    project_id: String,
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    trace_state: Option<String>,
    session_id: Option<String>,
    user_id: Option<String>,
    environment: Option<String>,
    span_name: Option<String>,
    span_kind: Option<String>,
    status_code: Option<String>,
    status_message: Option<String>,
    exception_type: Option<String>,
    exception_message: Option<String>,
    exception_stacktrace: Option<String>,
    span_category: Option<String>,
    observation_type: Option<String>,
    framework: Option<String>,
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    timestamp_start: time::OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::micros::option")]
    timestamp_end: Option<time::OffsetDateTime>,
    duration_ms: Option<i64>,
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    ingested_at: time::OffsetDateTime,
    gen_ai_system: Option<String>,
    gen_ai_operation_name: Option<String>,
    gen_ai_request_model: Option<String>,
    gen_ai_response_model: Option<String>,
    gen_ai_response_id: Option<String>,
    gen_ai_temperature: Option<f64>,
    gen_ai_top_p: Option<f64>,
    gen_ai_top_k: Option<i64>,
    gen_ai_max_tokens: Option<i64>,
    gen_ai_frequency_penalty: Option<f64>,
    gen_ai_presence_penalty: Option<f64>,
    gen_ai_stop_sequences: Option<String>,
    gen_ai_finish_reasons: Option<String>,
    gen_ai_agent_id: Option<String>,
    gen_ai_agent_name: Option<String>,
    gen_ai_tool_name: Option<String>,
    gen_ai_tool_call_id: Option<String>,
    gen_ai_server_ttft_ms: Option<i64>,
    gen_ai_server_request_duration_ms: Option<i64>,
    gen_ai_usage_input_tokens: i64,
    gen_ai_usage_output_tokens: i64,
    gen_ai_usage_total_tokens: i64,
    gen_ai_usage_cache_read_tokens: i64,
    gen_ai_usage_cache_write_tokens: i64,
    gen_ai_usage_reasoning_tokens: i64,
    gen_ai_usage_details: Option<String>,
    gen_ai_cost_input: i64,
    gen_ai_cost_output: i64,
    gen_ai_cost_cache_read: i64,
    gen_ai_cost_cache_write: i64,
    gen_ai_cost_reasoning: i64,
    gen_ai_cost_total: i64,
    http_method: Option<String>,
    http_url: Option<String>,
    http_status_code: Option<i32>,
    db_system: Option<String>,
    db_name: Option<String>,
    db_operation: Option<String>,
    db_statement: Option<String>,
    storage_system: Option<String>,
    storage_bucket: Option<String>,
    storage_object: Option<String>,
    messaging_system: Option<String>,
    messaging_destination: Option<String>,
    tags: Option<String>,
    metadata: Option<String>,
    input_preview: Option<String>,
    output_preview: Option<String>,
    messages: String,
    tool_definitions: String,
    tool_names: String,
    raw_span: Option<String>,
    scope_name: Option<String>,
    scope_version: Option<String>,
}

impl From<&NormalizedSpan> for SpanRow {
    fn from(span: &NormalizedSpan) -> Self {
        let timestamp_start = chrono_to_time(span.timestamp_start);
        let timestamp_end = span.timestamp_end.map(chrono_to_time);
        let ingested_at = chrono_to_time(span.ingested_at.unwrap_or_else(Utc::now));

        let project_id = span.project_id.clone().unwrap_or_default();
        if project_id.is_empty() {
            tracing::warn!(
                trace_id = %span.trace_id,
                span_id = %span.span_id,
                "Inserting span with empty project_id - data isolation may be compromised"
            );
        }

        Self {
            project_id,
            trace_id: span.trace_id.clone(),
            span_id: span.span_id.clone(),
            parent_span_id: span.parent_span_id.clone(),
            trace_state: span.trace_state.clone(),
            session_id: span.session_id.clone(),
            user_id: span.user_id.clone(),
            environment: span.environment.clone(),
            span_name: Some(span.span_name.clone()),
            span_kind: span.span_kind.clone(),
            status_code: span.status_code.clone(),
            status_message: span.status_message.clone(),
            exception_type: span.exception_type.clone(),
            exception_message: span.exception_message.clone(),
            exception_stacktrace: span.exception_stacktrace.clone(),
            span_category: span.span_category.map(|c| c.as_str().to_string()),
            observation_type: span.observation_type.map(|o| o.as_str().to_string()),
            framework: span.framework.map(|f| f.as_str().to_string()),
            timestamp_start,
            timestamp_end,
            duration_ms: Some(span.duration_ms),
            ingested_at,
            gen_ai_system: span.gen_ai_system.clone(),
            gen_ai_operation_name: span.gen_ai_operation_name.clone(),
            gen_ai_request_model: span.gen_ai_request_model.clone(),
            gen_ai_response_model: span.gen_ai_response_model.clone(),
            gen_ai_response_id: span.gen_ai_response_id.clone(),
            gen_ai_temperature: span.gen_ai_temperature,
            gen_ai_top_p: span.gen_ai_top_p,
            gen_ai_top_k: span.gen_ai_top_k,
            gen_ai_max_tokens: span.gen_ai_max_tokens,
            gen_ai_frequency_penalty: span.gen_ai_frequency_penalty,
            gen_ai_presence_penalty: span.gen_ai_presence_penalty,
            gen_ai_stop_sequences: if span.gen_ai_stop_sequences.is_empty() {
                None
            } else {
                serde_json::to_string(&span.gen_ai_stop_sequences).ok()
            },
            gen_ai_finish_reasons: if span.gen_ai_finish_reasons.is_empty() {
                None
            } else {
                serde_json::to_string(&span.gen_ai_finish_reasons).ok()
            },
            gen_ai_agent_id: span.gen_ai_agent_id.clone(),
            gen_ai_agent_name: span.gen_ai_agent_name.clone(),
            gen_ai_tool_name: span.gen_ai_tool_name.clone(),
            gen_ai_tool_call_id: span.gen_ai_tool_call_id.clone(),
            gen_ai_server_ttft_ms: span.gen_ai_server_ttft_ms,
            gen_ai_server_request_duration_ms: span.gen_ai_server_request_duration_ms,
            gen_ai_usage_input_tokens: span.gen_ai_usage_input_tokens,
            gen_ai_usage_output_tokens: span.gen_ai_usage_output_tokens,
            gen_ai_usage_total_tokens: span.gen_ai_usage_total_tokens,
            gen_ai_usage_cache_read_tokens: span.gen_ai_usage_cache_read_tokens,
            gen_ai_usage_cache_write_tokens: span.gen_ai_usage_cache_write_tokens,
            gen_ai_usage_reasoning_tokens: span.gen_ai_usage_reasoning_tokens,
            gen_ai_usage_details: span.gen_ai_usage_details.clone(),
            gen_ai_cost_input: to_decimal64(span.gen_ai_cost_input),
            gen_ai_cost_output: to_decimal64(span.gen_ai_cost_output),
            gen_ai_cost_cache_read: to_decimal64(span.gen_ai_cost_cache_read),
            gen_ai_cost_cache_write: to_decimal64(span.gen_ai_cost_cache_write),
            gen_ai_cost_reasoning: to_decimal64(span.gen_ai_cost_reasoning),
            gen_ai_cost_total: to_decimal64(span.gen_ai_cost_total),
            http_method: span.http_method.clone(),
            http_url: span.http_url.clone(),
            http_status_code: span.http_status_code.map(|c| c as i32),
            db_system: span.db_system.clone(),
            db_name: span.db_name.clone(),
            db_operation: span.db_operation.clone(),
            db_statement: span.db_statement.clone(),
            storage_system: span.storage_system.clone(),
            storage_bucket: span.storage_bucket.clone(),
            storage_object: span.storage_object.clone(),
            messaging_system: span.messaging_system.clone(),
            messaging_destination: span.messaging_destination.clone(),
            tags: if span.tags.is_empty() {
                None
            } else {
                serde_json::to_string(&span.tags).ok()
            },
            metadata: span.metadata.clone(),
            input_preview: span.input_preview.clone(),
            output_preview: span.output_preview.clone(),
            messages: span.messages.clone().unwrap_or_else(|| "[]".to_string()),
            tool_definitions: span
                .tool_definitions
                .clone()
                .unwrap_or_else(|| "[]".to_string()),
            tool_names: span.tool_names.clone().unwrap_or_else(|| "[]".to_string()),
            raw_span: span.raw_span.clone(),
            scope_name: span.scope_name.clone(),
            scope_version: span.scope_version.clone(),
        }
    }
}

/// Convert chrono DateTime to time OffsetDateTime for a storage-row column.
///
/// Via seconds plus the subsecond part, never nanoseconds-since-epoch. `timestamp_nanos_opt` is `None`
/// outside 1677-2262 - narrower than the `DateTime64(6)` column it feeds - and the fallback was the
/// **epoch**, which the schema's 90-day TTL then deleted. Ingestion refuses an unstorable instant
/// (`utils::time::is_storable`); this backstops a value that reached here anyway by clamping to the nearest
/// representable bound (`clamp_to_storable`) and logging, rather than the epoch (the one value the TTL
/// destroys) or an out-of-range year the driver cannot encode.
fn chrono_to_time(dt: chrono::DateTime<chrono::Utc>) -> time::OffsetDateTime {
    let (dt, clamped) = crate::utils::time::clamp_to_storable(dt);
    if clamped {
        tracing::error!(
            timestamp = %dt,
            "A timestamp outside the storable range reached a storage-row conversion; clamped to the \
             representable bound. Ingestion should have refused it - a write path is missing the is_storable \
             check."
        );
    }
    // In range now, so the conversion cannot fail; the epoch fallback is unreachable and only satisfies the
    // type.
    time::OffsetDateTime::from_unix_timestamp(dt.timestamp())
        .map(|t| t + time::Duration::nanoseconds(i64::from(dt.timestamp_subsec_nanos())))
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH)
}

/// Insert a batch of spans into ClickHouse
///
/// For distributed mode, inserts go directly to the local table (`otel_spans_local`)
/// for better performance. In single-node mode, inserts go to `otel_spans`.
pub async fn insert_batch(
    client: &Client,
    table_name: &str,
    spans: &[NormalizedSpan],
) -> Result<(), ClickhouseError> {
    if spans.is_empty() {
        return Ok(());
    }

    let mut insert: clickhouse::insert::Insert<SpanRow> = client.insert(table_name).await?;

    for span in spans {
        let row = SpanRow::from(span);
        insert.write(&row).await?;
    }

    insert.end().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chrono_to_time_conversion() {
        let chrono_dt = chrono::Utc::now();
        let time_dt = chrono_to_time(chrono_dt);

        // Should be within 1 second
        let diff = (chrono_dt.timestamp() - time_dt.unix_timestamp()).abs();
        assert!(diff <= 1);
    }

    #[test]
    fn test_span_row_from_normalized_span() {
        let span = NormalizedSpan {
            project_id: Some("test".to_string()),
            trace_id: "trace1".to_string(),
            span_id: "span1".to_string(),
            span_name: "test-span".to_string(),
            timestamp_start: chrono::Utc::now(),
            ..Default::default()
        };

        let row = SpanRow::from(&span);
        assert_eq!(row.project_id, "test");
        assert_eq!(row.trace_id, "trace1");
        assert_eq!(row.span_id, "span1");
    }
}
