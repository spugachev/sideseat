//! Message query operations
//!
//! Provides queries for extracting conversation messages from spans.
//! Messages are stored as raw JSON (SideML conversion happens at query time).
//!
//! This repository only handles data retrieval. All message processing
//! (filtering, deduplication, sorting, metadata harvesting) is done by
//! the feed pipeline (process_spans) in the domain layer.

use duckdb::Connection;

use crate::data::duckdb::DuckdbError;
use crate::data::duckdb::repositories::query::DEDUP_SPANS;
use crate::data::types::{
    FeedMessagesParams, MESSAGE_CONTENT_FILTER, MessageQueryParams, MessageQueryResult,
    MessageSpanRow,
};
use crate::utils::time::micros_to_datetime;

/// Shared SELECT columns for all message queries.
/// Column order must match `parse_span_row()` field extraction.
const MESSAGE_SELECT_COLUMNS: &str = r#"
    trace_id,
    span_id,
    parent_span_id,
    EPOCH_US(timestamp_start) AS span_timestamp_us,
    EPOCH_US(timestamp_end) AS span_end_timestamp_us,
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
    gen_ai_cost_total::DOUBLE AS cost_total,
    tool_definitions,
    tool_names,
    observation_type,
    session_id,
    EPOCH_US(ingested_at) AS ingested_at_us,
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
    gen_ai_cost_input::DOUBLE AS cost_input,
    gen_ai_cost_output::DOUBLE AS cost_output"#;

// ============================================================================
// Query functions - return raw unfiltered data
// ============================================================================

/// Get span rows for a span, trace, or session (unified query).
///
/// Priority: span_id > session_id > trace_id > trace_ids
pub fn get_messages(
    conn: &Connection,
    params: &MessageQueryParams,
) -> Result<MessageQueryResult, DuckdbError> {
    // Chosen before the conditions are built, because the session-membership subquery below needs the
    // same relation the outer query reads - see the comment on that branch.
    let (dedup, watermark_bind) = match params.ingested_before_us {
        Some(watermark_us) => (
            crate::data::duckdb::repositories::query::dedup_spans_as_of_watermark(),
            Some(watermark_us.to_string()),
        ),
        None => (DEDUP_SPANS, None),
    };

    // The dedup relation is at the head of the FROM, so its bind comes first.
    let mut conditions = vec!["project_id = ?".to_string()];
    let mut bind_values: Vec<String> = watermark_bind.iter().cloned().collect();
    bind_values.push(params.project_id.clone());

    if let Some(span_id) = &params.span_id {
        conditions.push("span_id = ?".to_string());
        bind_values.push(span_id.clone());
        // Span ids are 8 bytes and unique only within a trace, so a span query that ignores
        // the trace can return another trace's span. The span route supplies both.
        if let Some(trace_id) = &params.trace_id {
            conditions.push("trace_id = ?".to_string());
            bind_values.push(trace_id.clone());
        }
    } else if let Some(session_id) = &params.session_id {
        // Membership is resolved against the **deduplicated** relation, not the raw table.
        //
        // `otel_spans` is append-only, so it holds every delivery of a span. Reading membership from it
        // while the outer query reads the deduplicated view made the two disagree about the same trace: a
        // span re-delivered with a different (or absent) `session_id` left its old row behind, so the
        // subquery still found the trace for the *old* session while the outer query returned the trace's
        // current content. The session then reported a trace that no longer belongs to it - wrong count,
        // wrong content - and the trace appeared under two sessions at once. ClickHouse reads this
        // subquery with `FINAL` and was already correct, so this was also a silent backend disagreement.
        //
        // The same relation as the outer query, watermark included: a traversal must resolve membership as
        // of the instant it is reading, or it can load a trace whose context it will not select.
        // The trace's **canonical** session - its earliest span's - not "any span named it". A trace whose
        // spans name two sessions was returned in full by both, so one session's view showed content the UI
        // displays under another. `arg_min` over `(timestamp_start, span_id)` is the same total order the
        // display and the feed's grouping use.
        //
        // The shared definition, told this query's traversal instant so it can bound its own relation. The
        // watermark bind belongs to the subquery and is returned with the rest, so this call site no longer
        // manages it - it did, and that is one more place the order could drift.
        let traces = super::query::traces_of_session(params.ingested_before_us);
        conditions.push(format!("trace_id IN ({traces})"));
        bind_values.extend(super::query::traces_of_session_binds(
            params.ingested_before_us,
            &params.project_id,
            session_id,
        ));
        conditions.push(MESSAGE_CONTENT_FILTER.to_string());
    } else if let Some(trace_id) = &params.trace_id {
        conditions.push("trace_id = ?".to_string());
        bind_values.push(trace_id.clone());
        conditions.push(MESSAGE_CONTENT_FILTER.to_string());
    } else if let Some(trace_ids) = &params.trace_ids {
        // An empty list matches nothing, rather than falling through to every trace in the project.
        if trace_ids.is_empty() {
            conditions.push("1 = 0".to_string());
        } else {
            let placeholders: Vec<&str> = trace_ids.iter().map(|_| "?").collect();
            conditions.push(format!("trace_id IN ({})", placeholders.join(", ")));
            bind_values.extend(trace_ids.iter().cloned());
        }
        conditions.push(MESSAGE_CONTENT_FILTER.to_string());
    }

    if let Some(from) = &params.from_timestamp {
        conditions.push("timestamp_start >= ?".to_string());
        bind_values.push(from.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
    }
    if let Some(to) = &params.to_timestamp {
        conditions.push("timestamp_start < ?".to_string());
        bind_values.push(to.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
    }
    // The feed's traversal watermark is applied *inside* the deduplication (chosen above) - not as a
    // condition here. Bounding the dedup's *result* rather than its *choice* makes a span re-delivered
    // during the traversal vanish entirely: the newest row is rejected and the older one was never
    // selected. The page query learned that first; this is the context load beside it, and the two have to
    // agree about what exists or a page can select a span whose context holds no version of it.

    let sql = format!(
        // Deduplicated, like every other read: this query fed the pipeline *both* copies of a
        // re-delivered span while ClickHouse, which reads with FINAL, handed it one. The message
        // dedup usually hid that, but the two backends were reconstructing from different input, and
        // the totals had to be protected against it by hand.
        //
        // span_id breaks ties: ordering by timestamp alone leaves rows written in the same
        // microsecond to storage order, which is not stable between identical requests.
        // `trace_id` is in the order key, not just `span_id`. A span id is 8 bytes and unique only
        // *within* a trace, so two traces reuse ids freely - and with only `(timestamp_start, span_id)`
        // the order of tied rows is whatever the engine returns. Reconstruction reads first-seen order
        // to decide which trace strips its history against which, so an undefined tie there is an
        // undefined answer.
        "SELECT {MESSAGE_SELECT_COLUMNS} FROM {dedup} WHERE {} ORDER BY timestamp_start ASC, trace_id ASC, span_id ASC",
        conditions.join(" AND "),
    );

    let mut stmt = conn.prepare(&sql)?;
    let params_refs: Vec<&dyn duckdb::ToSql> = bind_values
        .iter()
        .map(|s| s as &dyn duckdb::ToSql)
        .collect();
    let rows: Vec<Result<MessageSpanRow, _>> =
        stmt.query_map(&*params_refs, parse_span_row)?.collect();
    let rows: Vec<MessageSpanRow> = rows.into_iter().collect::<Result<_, _>>()?;
    Ok(MessageQueryResult { rows })
}

/// Get span rows for entire project (feed API).
///
/// Uses cursor-based pagination on (ingested_at, span_id) for stable pagination.
pub fn get_project_messages(
    conn: &Connection,
    params: &FeedMessagesParams,
) -> Result<MessageQueryResult, DuckdbError> {
    let mut conditions = vec![
        "project_id = ?".to_string(),
        MESSAGE_CONTENT_FILTER.to_string(),
    ];
    let mut bind_values: Vec<String> = vec![params.project_id.clone()];

    // Cursor condition, on the same total key the ORDER BY uses. The trace id is in the key
    // because a span id is unique only within a trace: two traces sharing one in the same
    // ingestion microsecond had identical cursors, and a page boundary between them skipped the
    // row that had not been returned.
    if let Some((cursor_time_us, cursor_span_id, cursor_trace_id)) = &params.cursor {
        conditions
            .push("(EPOCH_US(ingested_at), span_id, trace_id) < (?::BIGINT, ?, ?)".to_string());
        bind_values.push(cursor_time_us.to_string());
        bind_values.push(cursor_span_id.clone());
        bind_values.push(cursor_trace_id.clone());
    }

    // The traversal watermark is applied *inside* the deduplication, below - not as a condition here.
    //
    // A page is chosen by ingestion time, so without an upper bound on it a span ingested *during* the
    // traversal appears on a later page, and may already have been read into an earlier page's
    // reconstruction context where it can win deduplication against a span still to be paged. But a bound
    // applied outside the dedup is worse than none for a *re-delivered* span: the dedup picks the newest
    // row over the whole table, the bound then rejects it, and the older row was never selected - so the
    // span is missing from every page. `dedup_spans_as_of_watermark` chooses the newest row that existed
    // when the traversal began instead, which is what a watermark is for.

    // Event time filters.
    //
    // The lower bound is on the span's *end*, so the page holds every span that overlaps the window.
    // A completed response carries its span's end time, so a span that began before the window and
    // finished inside it produces a message dated inside the window - and comparing the span's start
    // dropped it before reconstruction ever saw it. The upper bound stays on the start: a span that
    // begins after the window is irrelevant to it. `apply_time_window` then decides per message,
    // which is where the window belongs.
    if let Some(start) = &params.start_time {
        conditions.push("COALESCE(timestamp_end, timestamp_start) >= ?".to_string());
        bind_values.push(start.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
    }
    if let Some(end) = &params.end_time {
        conditions.push("timestamp_start < ?".to_string());
        bind_values.push(end.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
    }

    // The bounded dedup takes one bind of its own, and it comes *first* - the subquery is at the head of
    // the FROM, so its placeholders precede every condition's.
    let (dedup, watermark_binds) = match params.ingested_before_us {
        Some(watermark_us) => (
            crate::data::duckdb::repositories::query::dedup_spans_as_of_watermark(),
            vec![watermark_us.to_string()],
        ),
        None => (DEDUP_SPANS, Vec::new()),
    };
    let bind_values = {
        let mut all = watermark_binds;
        all.extend(bind_values);
        all
    };

    let sql = format!(
        "SELECT {MESSAGE_SELECT_COLUMNS} FROM {dedup} WHERE {} ORDER BY ingested_at DESC, span_id DESC, trace_id DESC LIMIT {}",
        conditions.join(" AND "),
        params.limit,
    );

    let mut stmt = conn.prepare(&sql)?;
    let params_refs: Vec<&dyn duckdb::ToSql> = bind_values
        .iter()
        .map(|s| s as &dyn duckdb::ToSql)
        .collect();
    let rows: Vec<Result<MessageSpanRow, _>> =
        stmt.query_map(&*params_refs, parse_span_row)?.collect();
    let rows: Vec<MessageSpanRow> = rows.into_iter().collect::<Result<_, _>>()?;
    Ok(MessageQueryResult { rows })
}

// ============================================================================
// Helper functions
// ============================================================================

/// Parse a span row from database - just extracts fields, no transformation.
fn parse_span_row(row: &duckdb::Row) -> Result<MessageSpanRow, duckdb::Error> {
    Ok(MessageSpanRow {
        trace_id: row.get(0)?,
        span_id: row.get(1)?,
        parent_span_id: row.get(2)?,
        span_timestamp: micros_to_datetime(row.get::<_, i64>(3)?),
        span_end_timestamp: row.get::<_, Option<i64>>(4)?.map(micros_to_datetime),
        messages_json: row.get::<_, Option<String>>(5)?.unwrap_or_default(),
        model: row.get(6)?,
        provider: row.get(7)?,
        status_code: row.get(8)?,
        exception_type: row.get(9)?,
        exception_message: row.get(10)?,
        exception_stacktrace: row.get(11)?,
        input_tokens: row.get(12)?,
        output_tokens: row.get(13)?,
        total_tokens: row.get(14)?,
        cost_total: row.get(15)?,
        tool_definitions_json: row.get::<_, Option<String>>(16)?.unwrap_or_default(),
        tool_names_json: row.get::<_, Option<String>>(17)?.unwrap_or_default(),
        observation_type: row.get(18)?,
        session_id: row.get(19)?,
        ingested_at: micros_to_datetime(row.get::<_, i64>(20)?),
        scope_name: row.get(21)?,
        scope_version: row.get(22)?,
        span_name: row.get(23)?,
        framework: row.get(24)?,
        response_model: row.get(25)?,
        response_id: row.get(26)?,
        temperature: row.get(27)?,
        top_p: row.get(28)?,
        max_tokens: row.get(29)?,
        finish_reasons: row.get(30)?,
        cache_read_tokens: row.get(31)?,
        cache_write_tokens: row.get(32)?,
        reasoning_tokens: row.get(33)?,
        cost_input: row.get(34)?,
        cost_output: row.get(35)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::storage::AppStorage;
    use crate::data::duckdb::repositories::span::insert_batch;
    use crate::data::duckdb::{DuckdbService, NormalizedSpan};
    use chrono::{Duration, Utc};
    use tempfile::TempDir;

    async fn create_test_service() -> (TempDir, DuckdbService) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let duckdb_dir = temp_dir.path().join("duckdb");
        tokio::fs::create_dir_all(&duckdb_dir)
            .await
            .expect("Failed to create duckdb dir");
        let storage = AppStorage::init_for_test(temp_dir.path().to_path_buf());
        let service = DuckdbService::init(&storage)
            .await
            .expect("Failed to init analytics service");
        (temp_dir, service)
    }

    fn make_span_with_messages(
        project_id: &str,
        trace_id: &str,
        span_id: &str,
        messages_json: &str,
    ) -> NormalizedSpan {
        NormalizedSpan {
            project_id: Some(project_id.to_string()),
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            span_name: "test".to_string(),
            timestamp_start: Utc::now(),
            messages: Some(messages_json.to_string()),
            ..Default::default()
        }
    }

    /// The scope and the envelope facts survive the round trip: written by the positional appender,
    /// read back by the message projection. This is the write-and-read pair the schema-v2 columns
    /// exist for, and it is the test that fails if a projection and the appender ever disagree about
    /// a column's position - the failure mode of a positional writer.
    #[tokio::test]
    async fn scope_and_envelope_facts_survive_the_round_trip() {
        let (_temp_dir, analytics) = create_test_service().await;
        let project_id = "test-project";

        let mut span = make_span_with_messages(
            project_id,
            "trace-env",
            "span-env",
            r#"[{"role": "user", "content": "Hello"}]"#,
        );
        span.scope_name = Some("opentelemetry.instrumentation.langchain".to_string());
        span.scope_version = Some("0.3.1".to_string());
        span.gen_ai_response_model = Some("model-x-20260101".to_string());
        span.gen_ai_response_id = Some("resp_abc".to_string());
        span.gen_ai_temperature = Some(0.7);
        span.gen_ai_max_tokens = Some(1024);
        span.gen_ai_usage_cache_read_tokens = 17;
        span.gen_ai_cost_input = 0.001;
        span.gen_ai_cost_output = 0.002;

        {
            let conn = analytics.conn();
            insert_batch(&conn, &[span]).expect("insert");
        }

        let conn = analytics.conn();
        let params = FeedMessagesParams {
            project_id: project_id.to_string(),
            limit: 10,
            ..Default::default()
        };
        let result = get_project_messages(&conn, &params).expect("query");
        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(
            row.scope_name.as_deref(),
            Some("opentelemetry.instrumentation.langchain"),
            "the instrumentation scope must survive the round trip - it is what makes a rule keyed \
             on a producer's identity-and-version expressible at read time"
        );
        assert_eq!(row.scope_version.as_deref(), Some("0.3.1"));
        assert_eq!(row.response_model.as_deref(), Some("model-x-20260101"));
        assert_eq!(row.response_id.as_deref(), Some("resp_abc"));
        assert_eq!(row.temperature, Some(0.7));
        assert_eq!(row.max_tokens, Some(1024));
        assert_eq!(row.cache_read_tokens, 17);
        assert!((row.cost_input - 0.001).abs() < 1e-9);
        assert!((row.cost_output - 0.002).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_get_project_messages_basic() {
        let (_temp_dir, analytics) = create_test_service().await;
        let project_id = "test-project";

        let messages_json = r#"[{"role": "user", "content": "Hello"}]"#;
        let span = make_span_with_messages(project_id, "trace-1", "span-1", messages_json);

        {
            let conn = analytics.conn();
            insert_batch(&conn, &[span]).expect("Insert should succeed");
        }

        let conn = analytics.conn();
        let params = FeedMessagesParams {
            project_id: project_id.to_string(),
            limit: 10,
            ..Default::default()
        };
        let result = get_project_messages(&conn, &params).expect("Query should succeed");

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].trace_id, "trace-1");
    }

    #[tokio::test]
    async fn test_get_project_messages_filters_empty_spans() {
        let (_temp_dir, analytics) = create_test_service().await;
        let project_id = "test-project";

        // Span with messages
        let span_with_messages =
            make_span_with_messages(project_id, "trace-1", "span-1", r#"[{"role": "user"}]"#);

        // Span without messages (empty array)
        let span_empty = NormalizedSpan {
            project_id: Some(project_id.to_string()),
            trace_id: "trace-2".to_string(),
            span_id: "span-2".to_string(),
            span_name: "empty".to_string(),
            timestamp_start: Utc::now(),
            messages: Some("[]".to_string()),
            ..Default::default()
        };

        {
            let conn = analytics.conn();
            insert_batch(&conn, &[span_with_messages, span_empty]).expect("Insert should succeed");
        }

        let conn = analytics.conn();
        let params = FeedMessagesParams {
            project_id: project_id.to_string(),
            limit: 10,
            ..Default::default()
        };
        let result = get_project_messages(&conn, &params).expect("Query should succeed");

        // Should only return the span with messages
        assert_eq!(
            result.rows.len(),
            1,
            "Should filter out empty message spans"
        );
        assert_eq!(result.rows[0].span_id, "span-1");
    }

    #[tokio::test]
    async fn test_get_project_messages_cursor_pagination() {
        let (_temp_dir, analytics) = create_test_service().await;
        let project_id = "test-project";

        // Create spans with different ingested_at times
        let base_time = Utc::now();
        let spans: Vec<_> = (0..5)
            .map(|i| {
                let mut span = make_span_with_messages(
                    project_id,
                    &format!("trace-{}", i),
                    &format!("span-{}", i),
                    r#"[{"role": "user", "content": "test"}]"#,
                );
                span.timestamp_start = base_time + Duration::seconds(i as i64);
                span.ingested_at = Some(base_time + Duration::seconds(i as i64));
                span
            })
            .collect();

        {
            let conn = analytics.conn();
            insert_batch(&conn, &spans).expect("Insert should succeed");
        }

        let conn = analytics.conn();

        // First page
        let params = FeedMessagesParams {
            project_id: project_id.to_string(),
            limit: 2,
            ..Default::default()
        };
        let page1 = get_project_messages(&conn, &params).expect("Query should succeed");
        assert_eq!(page1.rows.len(), 2, "First page should have 2 rows");

        // Second page with cursor
        let last_row = page1.rows.last().unwrap();
        let cursor_time_us = last_row.ingested_at.timestamp_micros();
        let params = FeedMessagesParams {
            project_id: project_id.to_string(),
            limit: 2,
            cursor: Some((
                cursor_time_us,
                last_row.span_id.clone(),
                last_row.trace_id.clone(),
            )),
            ..Default::default()
        };
        let page2 = get_project_messages(&conn, &params).expect("Query should succeed");
        assert_eq!(page2.rows.len(), 2, "Second page should have 2 rows");

        // Verify no overlap
        let page1_ids: Vec<_> = page1.rows.iter().map(|r| &r.span_id).collect();
        for row in &page2.rows {
            assert!(
                !page1_ids.contains(&&row.span_id),
                "Pages should not overlap"
            );
        }
    }

    #[tokio::test]
    async fn test_get_project_messages_time_filter() {
        let (_temp_dir, analytics) = create_test_service().await;
        let project_id = "test-project";

        let base_time = Utc::now();
        let mut old_span =
            make_span_with_messages(project_id, "trace-old", "span-old", r#"[{"role": "user"}]"#);
        old_span.timestamp_start = base_time - Duration::hours(2);

        let mut new_span =
            make_span_with_messages(project_id, "trace-new", "span-new", r#"[{"role": "user"}]"#);
        new_span.timestamp_start = base_time;

        {
            let conn = analytics.conn();
            insert_batch(&conn, &[old_span, new_span]).expect("Insert should succeed");
        }

        let conn = analytics.conn();

        // Filter to only recent spans
        let params = FeedMessagesParams {
            project_id: project_id.to_string(),
            limit: 10,
            start_time: Some(base_time - Duration::hours(1)),
            ..Default::default()
        };
        let result = get_project_messages(&conn, &params).expect("Query should succeed");

        assert_eq!(result.rows.len(), 1, "Should filter by start_time");
        assert_eq!(result.rows[0].span_id, "span-new");
    }

    #[tokio::test]
    async fn test_get_project_messages_with_session_id() {
        let (_temp_dir, analytics) = create_test_service().await;
        let project_id = "test-project";

        let mut span = make_span_with_messages(
            project_id,
            "trace-1",
            "span-1",
            r#"[{"role": "user", "content": "Hello"}]"#,
        );
        span.session_id = Some("session-123".to_string());

        {
            let conn = analytics.conn();
            insert_batch(&conn, &[span]).expect("Insert should succeed");
        }

        let conn = analytics.conn();
        let params = FeedMessagesParams {
            project_id: project_id.to_string(),
            limit: 10,
            ..Default::default()
        };
        let result = get_project_messages(&conn, &params).expect("Query should succeed");

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].session_id, Some("session-123".to_string()));
    }

    #[tokio::test]
    async fn test_get_project_messages_empty_project() {
        let (_temp_dir, analytics) = create_test_service().await;

        let conn = analytics.conn();
        let params = FeedMessagesParams {
            project_id: "nonexistent".to_string(),
            limit: 10,
            ..Default::default()
        };
        let result = get_project_messages(&conn, &params).expect("Query should succeed");

        assert!(result.rows.is_empty());
    }
}
