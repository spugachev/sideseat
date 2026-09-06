//! ClickHouse messages repository
//!
//! Provides message aggregation and query operations for the ClickHouse backend.

use chrono::DateTime;
use clickhouse::{Client, Row};
use serde::Deserialize;

use crate::data::clickhouse::ClickhouseError;
use crate::data::types::{
    FeedMessagesParams, MESSAGE_CONTENT_FILTER, MessageQueryParams, MessageQueryResult,
    MessageSpanRow,
};

/// Shared SELECT columns for all message queries.
const CH_MESSAGE_SELECT_COLUMNS: &str = r#"
    trace_id,
    span_id,
    parent_span_id,
    toInt64(toUnixTimestamp64Micro(timestamp_start)) AS span_timestamp_us,
    if(timestamp_end IS NULL, NULL, toInt64(toUnixTimestamp64Micro(timestamp_end))) AS span_end_timestamp_us,
    messages,
    gen_ai_request_model AS model,
    gen_ai_system AS provider,
    status_code,
    exception_type,
    exception_message,
    exception_stacktrace,
    gen_ai_usage_input_tokens AS input_tokens,
    gen_ai_usage_output_tokens AS output_tokens,
    gen_ai_usage_total_tokens AS total_tokens,
    toFloat64(gen_ai_cost_total) AS cost_total,
    tool_definitions,
    tool_names,
    observation_type,
    session_id,
    toInt64(toUnixTimestamp64Micro(ingested_at)) AS ingested_at_us,
    scope_name,
    scope_version,
    span_name,
    framework,
    gen_ai_response_model AS response_model,
    gen_ai_response_id AS response_id,
    gen_ai_temperature AS temperature,
    gen_ai_top_p AS top_p,
    gen_ai_max_tokens AS max_tokens,
    gen_ai_finish_reasons AS finish_reasons,
    gen_ai_usage_cache_read_tokens AS cache_read_tokens,
    gen_ai_usage_cache_write_tokens AS cache_write_tokens,
    gen_ai_usage_reasoning_tokens AS reasoning_tokens,
    toFloat64(gen_ai_cost_input) AS cost_input,
    toFloat64(gen_ai_cost_output) AS cost_output"#;

/// ClickHouse row for message span queries
#[derive(Row, Deserialize)]
struct ChMessageSpanRow {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    span_timestamp_us: i64,
    span_end_timestamp_us: Option<i64>,
    messages: String,
    model: Option<String>,
    provider: Option<String>,
    status_code: Option<String>,
    exception_type: Option<String>,
    exception_message: Option<String>,
    exception_stacktrace: Option<String>,
    input_tokens: i64,
    output_tokens: i64,
    total_tokens: i64,
    cost_total: f64,
    tool_definitions: String,
    tool_names: String,
    observation_type: Option<String>,
    session_id: Option<String>,
    ingested_at_us: i64,
    scope_name: Option<String>,
    scope_version: Option<String>,
    span_name: Option<String>,
    framework: Option<String>,
    response_model: Option<String>,
    response_id: Option<String>,
    temperature: Option<f64>,
    top_p: Option<f64>,
    max_tokens: Option<i64>,
    finish_reasons: Option<String>,
    cache_read_tokens: i64,
    cache_write_tokens: i64,
    reasoning_tokens: i64,
    cost_input: f64,
    cost_output: f64,
}

impl From<ChMessageSpanRow> for MessageSpanRow {
    fn from(row: ChMessageSpanRow) -> Self {
        Self {
            trace_id: row.trace_id,
            span_id: row.span_id,
            parent_span_id: row.parent_span_id,
            span_timestamp: DateTime::from_timestamp_micros(row.span_timestamp_us)
                .unwrap_or(DateTime::UNIX_EPOCH),
            span_end_timestamp: row
                .span_end_timestamp_us
                .and_then(DateTime::from_timestamp_micros),
            messages_json: row.messages,
            model: row.model,
            provider: row.provider,
            status_code: row.status_code,
            exception_type: row.exception_type,
            exception_message: row.exception_message,
            exception_stacktrace: row.exception_stacktrace,
            input_tokens: row.input_tokens,
            output_tokens: row.output_tokens,
            total_tokens: row.total_tokens,
            cost_total: row.cost_total,
            tool_definitions_json: row.tool_definitions,
            tool_names_json: row.tool_names,
            observation_type: row.observation_type,
            session_id: row.session_id,
            ingested_at: DateTime::from_timestamp_micros(row.ingested_at_us)
                .unwrap_or(DateTime::UNIX_EPOCH),
            scope_name: row.scope_name,
            scope_version: row.scope_version,
            span_name: row.span_name,
            framework: row.framework,
            response_model: row.response_model,
            response_id: row.response_id,
            temperature: row.temperature,
            top_p: row.top_p,
            max_tokens: row.max_tokens,
            finish_reasons: row.finish_reasons,
            cache_read_tokens: row.cache_read_tokens,
            cache_write_tokens: row.cache_write_tokens,
            reasoning_tokens: row.reasoning_tokens,
            cost_input: row.cost_input,
            cost_output: row.cost_output,
        }
    }
}

/// Get span rows for a span, trace, or session (unified query).
///
/// Priority: span_id > session_id > trace_id > trace_ids
pub async fn get_messages(
    client: &Client,
    params: &MessageQueryParams,
) -> Result<MessageQueryResult, ClickhouseError> {
    let mut conditions = vec!["project_id = ?".to_string()];
    let mut string_binds: Vec<String> = vec![params.project_id.clone()];
    let mut time_params: Vec<i64> = Vec::new();

    if let Some(span_id) = &params.span_id {
        conditions.push("span_id = ?".to_string());
        string_binds.push(span_id.clone());
        // Span ids are unique only within a trace; see the DuckDB backend for the same guard.
        if let Some(trace_id) = &params.trace_id {
            conditions.push("trace_id = ?".to_string());
            string_binds.push(trace_id.clone());
        }
    } else if let Some(session_id) = &params.session_id {
        // The shared definition, not a copy of it. ClickHouse ignores the traversal watermark here (`FINAL`
        // has no "as of" form), so the constant serves this call site unchanged - and keeping a second copy
        // of the SQL is the drift a single definition exists to prevent.
        conditions.push(format!(
            "trace_id IN ({})",
            crate::data::clickhouse::repositories::query::TRACES_OF_SESSION
        ));
        string_binds.extend(
            crate::data::clickhouse::repositories::query::traces_of_session_binds(
                &params.project_id,
                session_id,
            ),
        );
        conditions.push(MESSAGE_CONTENT_FILTER.to_string());
    } else if let Some(trace_id) = &params.trace_id {
        conditions.push("trace_id = ?".to_string());
        string_binds.push(trace_id.clone());
        conditions.push(MESSAGE_CONTENT_FILTER.to_string());
    } else if let Some(trace_ids) = &params.trace_ids {
        // An empty list matches nothing; see the DuckDB backend.
        if trace_ids.is_empty() {
            conditions.push("1 = 0".to_string());
        } else {
            let placeholders: Vec<&str> = trace_ids.iter().map(|_| "?").collect();
            conditions.push(format!("trace_id IN ({})", placeholders.join(", ")));
            string_binds.extend(trace_ids.iter().cloned());
        }
        conditions.push(MESSAGE_CONTENT_FILTER.to_string());
    }

    if let Some(from) = &params.from_timestamp {
        conditions.push("timestamp_start >= fromUnixTimestamp64Micro(?)".to_string());
        time_params.push(from.timestamp_micros());
    }
    if let Some(to) = &params.to_timestamp {
        conditions.push("timestamp_start < fromUnixTimestamp64Micro(?)".to_string());
        time_params.push(to.timestamp_micros());
    }
    // The watermark goes *inside* the deduplication, not into `conditions` - see
    // `ch_dedup_spans_as_of_watermark` for what bounding `FINAL`'s result instead of its choice did.
    let (source, watermark_bind) = match params.ingested_before_us {
        Some(watermark_us) => (
            crate::data::clickhouse::repositories::query::ch_dedup_spans_as_of_watermark()
                .to_string(),
            Some(watermark_us),
        ),
        None => ("otel_spans FINAL".to_string(), None),
    };

    let sql = format!(
        // span_id breaks ties, matching the DuckDB backend: timestamp alone is not a stable order.
        // `trace_id` is in the order key, not just `span_id`. A span id is 8 bytes and unique only
        // *within* a trace, so two traces reuse ids freely - and with only `(timestamp_start, span_id)`
        // the order of tied rows is whatever the engine returns. Reconstruction reads first-seen order
        // to decide which trace strips its history against which, so an undefined tie there is an
        // undefined answer.
        "SELECT {CH_MESSAGE_SELECT_COLUMNS} FROM {source} WHERE {} ORDER BY timestamp_start ASC, trace_id ASC, span_id ASC",
        conditions.join(" AND ")
    );

    let mut query = client.query(&sql);
    // The dedup subquery sits at the head of the FROM, so its placeholder precedes every condition's.
    if let Some(watermark_us) = watermark_bind {
        query = query.bind(watermark_us);
    }
    for s in &string_binds {
        query = query.bind(s);
    }
    for ts in &time_params {
        query = query.bind(ts);
    }
    let rows: Vec<ChMessageSpanRow> = query.fetch_all().await?;

    Ok(MessageQueryResult {
        rows: rows.into_iter().map(MessageSpanRow::from).collect(),
    })
}

/// Parameter type for mixed binding in get_project_messages
enum BindParam {
    String(String),
    Int64(i64),
}

/// Get span rows for entire project (feed API).
///
/// Uses cursor-based pagination on (ingested_at, span_id) for stable pagination.
pub async fn get_project_messages(
    client: &Client,
    params: &FeedMessagesParams,
) -> Result<MessageQueryResult, ClickhouseError> {
    let mut conditions = vec![
        "project_id = ?".to_string(),
        MESSAGE_CONTENT_FILTER.to_string(),
    ];
    let mut bind_params: Vec<BindParam> = vec![BindParam::String(params.project_id.clone())];

    // Cursor condition - both values bound as parameters
    // The trace id is part of the cursor key: a span id is unique only within a trace.
    if let Some((cursor_time_us, cursor_span_id, cursor_trace_id)) = &params.cursor {
        conditions.push(
            "(toInt64(toUnixTimestamp64Micro(ingested_at)), span_id, trace_id) < (?, ?, ?)"
                .to_string(),
        );
        bind_params.push(BindParam::Int64(*cursor_time_us));
        bind_params.push(BindParam::String(cursor_span_id.clone()));
        bind_params.push(BindParam::String(cursor_trace_id.clone()));
    }

    // The traversal watermark, applied *inside* the deduplication - see
    // `ch_dedup_spans_as_of_watermark` for the sequence that made a span vanish from every page.
    let (source, watermark_bind) = match params.ingested_before_us {
        Some(watermark_us) => (
            crate::data::clickhouse::repositories::query::ch_dedup_spans_as_of_watermark()
                .to_string(),
            Some(watermark_us),
        ),
        None => ("otel_spans FINAL".to_string(), None),
    };

    // Event time filters - use parameterized timestamps.
    //
    // The lower bound is on the span's *end*, so the page holds every span overlapping the window;
    // see the DuckDB backend for what comparing the start dropped.
    if let Some(start) = &params.start_time {
        conditions.push(
            "coalesce(timestamp_end, timestamp_start) >= fromUnixTimestamp64Micro(?)".to_string(),
        );
        bind_params.push(BindParam::Int64(start.timestamp_micros()));
    }
    if let Some(end) = &params.end_time {
        conditions.push("timestamp_start < fromUnixTimestamp64Micro(?)".to_string());
        bind_params.push(BindParam::Int64(end.timestamp_micros()));
    }

    let sql = format!(
        "SELECT {CH_MESSAGE_SELECT_COLUMNS} FROM {source} WHERE {} ORDER BY ingested_at DESC, span_id DESC, trace_id DESC LIMIT {}",
        conditions.join(" AND "),
        params.limit
    );

    let mut query = client.query(&sql);
    // The dedup subquery is at the head of the FROM, so its placeholder precedes every condition's.
    if let Some(watermark_us) = watermark_bind {
        query = query.bind(watermark_us);
    }
    for param in &bind_params {
        query = match param {
            BindParam::String(s) => query.bind(s),
            BindParam::Int64(i) => query.bind(i),
        };
    }

    let rows: Vec<ChMessageSpanRow> = query.fetch_all().await?;

    Ok(MessageQueryResult {
        rows: rows.into_iter().map(MessageSpanRow::from).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_query_result() {
        let result = MessageQueryResult { rows: vec![] };
        assert!(result.rows.is_empty());
    }
}
