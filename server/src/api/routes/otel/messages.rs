//! Messages API endpoints for conversation history

use std::collections::HashSet;

use axum::Json;
use axum::extract::State;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::OtelApiState;
use super::types::{BlockDto, MessagesMetadataDto, MessagesResponseDto, SpanEnvelopeDto};
use crate::api::auth::{SessionRead, SpanRead, TraceRead};
use crate::api::types::{ApiError, parse_timestamp_param};
use crate::data::types::MessageQueryParams;
use crate::domain::sideml::{
    ExtractedTools, FeedOptions, FeedResult, apply_time_window, extract_tools_from_rows,
    process_spans_cached,
};

#[derive(Debug, Deserialize)]
pub struct MessagesQuery {
    pub from_timestamp: Option<String>,
    pub to_timestamp: Option<String>,
    pub role: Option<String>,
}

impl MessagesQuery {
    fn to_feed_options(&self) -> FeedOptions {
        FeedOptions::new().with_role(self.role.clone())
    }
}

/// GET /traces/{trace_id}/spans/{span_id}/messages - Get conversation messages for a span
#[utoipa::path(
    get,
    path = "/api/v1/project/{project_id}/otel/traces/{trace_id}/spans/{span_id}/messages",
    tag = "spans",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("trace_id" = String, Path, description = "Trace ID"),
        ("span_id" = String, Path, description = "Span ID"),
        ("from_timestamp" = Option<String>, Query, description = "Filter from timestamp (ISO 8601)"),
        ("to_timestamp" = Option<String>, Query, description = "Filter to timestamp (ISO 8601)"),
        ("role" = Option<String>, Query, description = "Filter by role (user, assistant, etc.)")
    ),
    responses(
        (status = 200, description = "Messages for the span", body = MessagesResponseDto)
    )
)]
pub async fn get_span_messages(
    State(state): State<OtelApiState>,
    auth: SpanRead,
    axum::extract::Query(query): axum::extract::Query<MessagesQuery>,
) -> Result<Json<MessagesResponseDto>, ApiError> {
    let project_id = &auth.project_id;
    let span_id = &auth.span_id;
    let trace_id = &auth.trace_id;

    let from_timestamp = parse_timestamp_param(&query.from_timestamp)?;
    let to_timestamp = parse_timestamp_param(&query.to_timestamp)?;

    let options = query.to_feed_options();

    // Fetch raw span rows.
    //
    // Constrained by trace_id as well as span_id. The route carries both, but only span_id was
    // used: OTel span ids are 8 bytes and unique only within a trace, so a collision returned
    // another trace's span under this URL.
    let repo = state.analytics.repository();
    let params = MessageQueryParams {
        project_id: project_id.to_string(),
        span_id: Some(span_id.to_string()),
        trace_id: Some(trace_id.to_string()),
        // Only the upper bound is a query filter. See apply_time_window: filtering rows by the
        // lower bound removes the history the pipeline needs in order to recognise a re-send.
        from_timestamp: None,
        to_timestamp,
        ..Default::default()
    };
    let result = repo
        .get_messages(&params)
        .await
        .map_err(ApiError::from_data)?;

    // A span that exists always yields its row: unlike the trace and session queries, this one applies no
    // content filter, so "no rows" means the span is not there. Answering an empty 200 said the span exists
    // and holds no messages, which is a different fact - and the detail routes beside this one already 404.
    if result.rows.is_empty() {
        return Err(ApiError::not_found(
            "SPAN_NOT_FOUND",
            format!("Span not found: {span_id}"),
        ));
    }

    // Process through feed pipeline
    let envelopes: Vec<SpanEnvelopeDto> =
        result.rows.iter().map(SpanEnvelopeDto::from_row).collect();
    let processed = process_spans_cached(&state.reconstruction, result.rows, &options);
    let processed = apply_time_window(processed, from_timestamp, to_timestamp);

    let response = build_messages_response(processed, None, envelopes);
    Ok(Json(response))
}

/// GET /traces/{trace_id}/messages - Get conversation messages for a trace
#[utoipa::path(
    get,
    path = "/api/v1/project/{project_id}/otel/traces/{trace_id}/messages",
    tag = "traces",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("trace_id" = String, Path, description = "Trace ID"),
        ("from_timestamp" = Option<String>, Query, description = "Filter from timestamp (ISO 8601)"),
        ("to_timestamp" = Option<String>, Query, description = "Filter to timestamp (ISO 8601)"),
        ("role" = Option<String>, Query, description = "Filter by role (user, assistant, etc.)")
    ),
    responses(
        (status = 200, description = "Messages for the trace", body = MessagesResponseDto)
    )
)]
pub async fn get_trace_messages(
    State(state): State<OtelApiState>,
    auth: TraceRead,
    axum::extract::Query(query): axum::extract::Query<MessagesQuery>,
) -> Result<Json<MessagesResponseDto>, ApiError> {
    let project_id = &auth.project_id;
    let trace_id = &auth.trace_id;

    let from_timestamp = parse_timestamp_param(&query.from_timestamp)?;
    let to_timestamp = parse_timestamp_param(&query.to_timestamp)?;

    // History filtering is automatic (duplicates are detected and filtered)
    let options = query.to_feed_options();

    // Fetch trace metadata for session_id and totals
    let repo = state.analytics.repository();
    // Absent means absent, as on the trace detail route: an empty 200 said the trace exists and holds no
    // messages, so a stale URL after a deletion was indistinguishable from a trace that never spoke.
    let trace = repo
        .get_trace(project_id, trace_id)
        .await
        .map_err(ApiError::from_data)?
        .ok_or_else(|| {
            ApiError::not_found("TRACE_NOT_FOUND", format!("Trace not found: {trace_id}"))
        })?;

    // Session-aware loading: if trace belongs to a session, load ALL session spans
    // so cross-trace prefix stripping can remove history re-sent from prior traces
    let session_id = trace.session_id.as_ref().filter(|s| !s.is_empty());

    let result = if let Some(sid) = session_id {
        let params = MessageQueryParams {
            project_id: project_id.to_string(),
            session_id: Some(sid.to_string()),
            from_timestamp: None,
            to_timestamp,
            ..Default::default()
        };
        repo.get_messages(&params)
            .await
            .map_err(ApiError::from_data)?
    } else {
        let params = MessageQueryParams {
            project_id: project_id.to_string(),
            trace_id: Some(trace_id.to_string()),
            from_timestamp: None,
            to_timestamp,
            ..Default::default()
        };
        repo.get_messages(&params)
            .await
            .map_err(ApiError::from_data)?
    };

    // When session-loaded, scope tool extraction to the target trace's rows
    // BEFORE consuming rows into process_spans (which needs ownership).
    // Envelope scope follows the *view*, not the query: a trace view loads its whole session so
    // cross-trace stripping can run, and returning every session span's envelope here would leak
    // spans the caller did not ask about.
    let envelopes: Vec<SpanEnvelopeDto> = result
        .rows
        .iter()
        .filter(|row| row.trace_id == *trace_id)
        .map(SpanEnvelopeDto::from_row)
        .collect();
    let scoped_tools = if session_id.is_some() {
        Some(extract_tools_from_rows(
            result.rows.iter().filter(|r| r.trace_id == *trace_id),
        ))
    } else {
        None
    };

    // Process through feed pipeline (auto-routes to multi-trace if needed)
    let mut processed = process_spans_cached(&state.reconstruction, result.rows, &options);

    // If session-loaded, retain only the target trace's blocks and apply scoped tools.
    // scoped_tools is Some iff session_id.is_some(), so use it as the single guard.
    if let Some(scoped_tools) = scoped_tools {
        scope_feed_to_trace(&mut processed, scoped_tools, trace_id);
    }

    // The window applies to the answer, after the whole session has been seen and narrowed to
    // this trace.
    let processed = apply_time_window(processed, from_timestamp, to_timestamp);

    // Use trace-level totals for metadata (matches trace endpoint)
    let trace_totals = Some((trace.total_tokens, trace.total_cost));
    let response = build_messages_response(processed, trace_totals, envelopes);
    Ok(Json(response))
}

/// GET /sessions/{session_id}/messages - Get conversation messages for a session
#[utoipa::path(
    get,
    path = "/api/v1/project/{project_id}/otel/sessions/{session_id}/messages",
    tag = "sessions",
    params(
        ("project_id" = String, Path, description = "Project ID"),
        ("session_id" = String, Path, description = "Session ID"),
        ("from_timestamp" = Option<String>, Query, description = "Filter from timestamp (ISO 8601)"),
        ("to_timestamp" = Option<String>, Query, description = "Filter to timestamp (ISO 8601)"),
        ("role" = Option<String>, Query, description = "Filter by role (user, assistant, etc.)")
    ),
    responses(
        (status = 200, description = "Messages for the session", body = MessagesResponseDto)
    )
)]
pub async fn get_session_messages(
    State(state): State<OtelApiState>,
    auth: SessionRead,
    axum::extract::Query(query): axum::extract::Query<MessagesQuery>,
) -> Result<Json<MessagesResponseDto>, ApiError> {
    let project_id = &auth.project_id;
    let session_id = &auth.session_id;

    let from_timestamp = parse_timestamp_param(&query.from_timestamp)?;
    let to_timestamp = parse_timestamp_param(&query.to_timestamp)?;

    // History filtering is automatic (duplicates are detected and filtered)
    let options = query.to_feed_options();

    let repo = state.analytics.repository();

    // Before the rows, so a session that is not there is a 404 rather than an empty 200 - the same answer
    // the session detail route gives, and the same distinction: "no such session" is not "no messages".
    // Its totals are read here too; the pipeline's cover only the rows it was handed, and a message query is
    // handed only rows carrying messages, tools or an error, so a span billed with nothing to show counted as
    // free. They also lack the parent/child billing dedup the session summary applies.
    let session = repo
        .get_session(project_id, session_id)
        .await
        .map_err(ApiError::from_data)?
        .ok_or_else(|| {
            ApiError::not_found(
                "SESSION_NOT_FOUND",
                format!("Session not found: {session_id}"),
            )
        })?;
    let session_totals = Some((session.total_tokens, session.total_cost));

    let params = MessageQueryParams {
        project_id: project_id.to_string(),
        session_id: Some(session_id.to_string()),
        from_timestamp: None,
        to_timestamp,
        ..Default::default()
    };
    let result = repo
        .get_messages(&params)
        .await
        .map_err(ApiError::from_data)?;

    // Process through feed pipeline
    let envelopes: Vec<SpanEnvelopeDto> =
        result.rows.iter().map(SpanEnvelopeDto::from_row).collect();
    let processed = process_spans_cached(&state.reconstruction, result.rows, &options);
    let processed = apply_time_window(processed, from_timestamp, to_timestamp);

    let response = build_messages_response(processed, session_totals, envelopes);
    Ok(Json(response))
}

/// Scope a session-loaded FeedResult to a single trace.
pub(crate) fn scope_feed_to_trace(
    processed: &mut FeedResult,
    scoped_tools: ExtractedTools,
    trace_id: &str,
) {
    processed.messages.retain(|b| b.trace_id == trace_id);
    processed.metadata.block_count = processed.messages.len();
    processed.metadata.span_count = processed
        .messages
        .iter()
        .map(|b| (&b.trace_id, &b.span_id))
        .collect::<HashSet<_>>()
        .len();
    processed.tool_definitions = scoped_tools.tool_definitions;
    processed.tool_names = scoped_tools.tool_names;
}

/// Build messages response from processed messages.
///
/// If `trace_totals` is provided, use trace-level token/cost totals.
/// Otherwise, aggregate from message spans.
pub(crate) fn build_messages_response(
    processed: FeedResult,
    trace_totals: Option<(i64, f64)>,
    envelopes: Vec<SpanEnvelopeDto>,
) -> MessagesResponseDto {
    let mut messages_dto = Vec::new();
    let mut start_time: Option<DateTime<Utc>> = None;
    let mut end_time: Option<DateTime<Utc>> = None;

    for block in &processed.messages {
        if start_time.is_none_or(|t| block.timestamp < t) {
            start_time = Some(block.timestamp);
        }
        if end_time.is_none_or(|t| block.timestamp > t) {
            end_time = Some(block.timestamp);
        }

        messages_dto.push(BlockDto::from_block_entry(block));
    }

    let total_messages = messages_dto.len() as i64;
    // The pipeline's totals, which are sums over the spans in scope, not over the blocks returned.
    //
    // Summing the returned blocks made a billed span contribute nothing whenever its messages were
    // all dropped - as history, or by a role or time filter - so the reported cost of a conversation
    // fell when a filter was applied. The spans were still billed.
    let (total_tokens, total_cost) = trace_totals.unwrap_or((
        processed.metadata.total_tokens,
        processed.metadata.total_cost,
    ));

    MessagesResponseDto {
        envelopes,
        messages: messages_dto,
        metadata: MessagesMetadataDto {
            total_messages,
            total_tokens,
            total_cost,
            start_time: start_time.unwrap_or_else(Utc::now),
            end_time,
            // Carried from the pipeline, not recomputed: this is the one place a caller can learn that
            // the answer may repeat history.
            replay_matching_complete: processed.metadata.replay_matching_complete,
        },
        tool_definitions: processed.tool_definitions,
        tool_names: processed.tool_names,
    }
}
