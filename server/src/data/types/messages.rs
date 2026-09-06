//! Shared message types for all database backends
//!
//! This module contains message query result types and parameters.

use chrono::{DateTime, Utc};

use super::analytics::SpanIdentity;

/// Columns a filter-options request may ask for, shared by both analytics backends.
///
/// These were declared separately in the DuckDB and ClickHouse repositories and had drifted in
/// both directions: ClickHouse omitted `gen_ai_agent_name`, so the Agent filter dropdown was
/// empty there, while DuckDB omitted `span_name`, `session_id` and `user_id`, so those were
/// empty on DuckDB. Which filters a user sees depended on the storage backend.
///
/// Every name here is a column on `otel_spans` in both schemas. The allowlist exists to keep
/// a caller-supplied column name out of the SQL, so adding a name that is not a real column
/// turns a filter into an error rather than an injection.
/// The trace name a list row displays: the root span's name, else the earliest named span's.
///
/// The same expression in both dialects, because three places have to agree on it - the projection
/// that displays it, the filter options that offer it, and the filter that matches it. `alias`
/// qualifies the column for queries that name their table.
///
/// A `trace_name` filter has to be evaluated against *this*, per trace. Matching the raw
/// `span_name` of any span meant selecting "agent" also returned traces displayed under another
/// name that merely contained an agent span.
pub fn trace_display_name(alias: &str, dialect_first: DisplayNameDialect) -> String {
    let prefix = if alias.is_empty() {
        String::new()
    } else {
        format!("{alias}.")
    };
    match dialect_first {
        // `span_id` breaks the tie, for the same reason it does in `trace_display_first`: two roots
        // stamped with the same start instant left the choice to the engine, so the name the list
        // displayed, the name a filter matched and the name the detail view showed could be three
        // different spans' - and could differ between two runs of the same query.
        DisplayNameDialect::DuckDb => format!(
            "COALESCE(\
             FIRST({prefix}span_name ORDER BY {prefix}timestamp_start, {prefix}span_id) \
             FILTER (WHERE {prefix}parent_span_id IS NULL AND {prefix}span_name IS NOT NULL), \
             FIRST({prefix}span_name ORDER BY {prefix}timestamp_start, {prefix}span_id) \
             FILTER (WHERE {prefix}span_name IS NOT NULL))"
        ),
        DisplayNameDialect::ClickHouse => format!(
            "coalesce(\
             argMinIf({prefix}span_name, ({prefix}timestamp_start, {prefix}span_id), \
             {prefix}parent_span_id IS NULL AND {prefix}span_name IS NOT NULL), \
             argMinIf({prefix}span_name, ({prefix}timestamp_start, {prefix}span_id), \
             {prefix}span_name IS NOT NULL))"
        ),
    }
}

/// Which aggregate syntax to render the display name in.
#[derive(Clone, Copy)]
pub enum DisplayNameDialect {
    DuckDb,
    ClickHouse,
}

/// The single value a trace row displays for a column that only some of its spans carry: the
/// earliest span that has one.
///
/// A session id, a user id and an environment are recorded on the spans that know them - often the
/// root alone - so the row shows one value chosen this way. A filter on such a column has to be
/// evaluated against that value, not against "some span": a trace whose root names a session and
/// whose children do not was returned by `session IS NULL` while displaying the session, and
/// excluded by nothing.
///
/// The column name comes from this crate's allowlists, never from a request.
pub fn trace_display_first(column: &str, alias: &str, dialect_first: DisplayNameDialect) -> String {
    let prefix = if alias.is_empty() {
        String::new()
    } else {
        format!("{alias}.")
    };
    match dialect_first {
        // `span_id` breaks the tie, making this a **total** order. Ordering by the timestamp alone left
        // same-instant spans to the engine, so a filter could match a different span's value than the one
        // the row displays and than the one session membership resolves to - three answers about one trace.
        DisplayNameDialect::DuckDb => format!(
            "FIRST({prefix}{column} ORDER BY {prefix}timestamp_start, {prefix}span_id) \
             FILTER (WHERE {prefix}{column} IS NOT NULL)"
        ),
        DisplayNameDialect::ClickHouse => format!(
            "argMinIf({prefix}{column}, ({prefix}timestamp_start, {prefix}span_id), \
             {prefix}{column} IS NOT NULL)"
        ),
    }
}

/// What makes a span a GenAI span, for the "GenAI only" trace and session lists.
///
/// A span qualifies through its observation type or through any GenAI attribute. Recognising only
/// the provider and the request model missed transport-level instrumentation that records an
/// operation name, a response model, an agent or tool name, or token usage and nothing else - those
/// traces vanished from the default list.
///
/// One definition, used by both backends, because it read differently in each and the same project
/// showed a different trace list depending on which one served it. `alias` qualifies the columns for
/// queries that name their table; pass "" when they do not.
pub fn genai_span_predicate(alias: &str) -> String {
    let prefix = if alias.is_empty() {
        String::new()
    } else {
        format!("{alias}.")
    };
    [
        format!("{prefix}observation_type != 'span'"),
        format!("{prefix}gen_ai_system IS NOT NULL"),
        format!("{prefix}gen_ai_operation_name IS NOT NULL"),
        format!("{prefix}gen_ai_request_model IS NOT NULL"),
        format!("{prefix}gen_ai_response_model IS NOT NULL"),
        format!("{prefix}gen_ai_agent_name IS NOT NULL"),
        format!("{prefix}gen_ai_tool_name IS NOT NULL"),
        format!("{prefix}gen_ai_usage_total_tokens > 0"),
        // Cache and reasoning usage on their own. Extraction accepts each independently, so a span
        // can report cache reads or reasoning tokens and nothing else.
        format!("{prefix}gen_ai_usage_cache_read_tokens > 0"),
        format!("{prefix}gen_ai_usage_cache_write_tokens > 0"),
        format!("{prefix}gen_ai_usage_reasoning_tokens > 0"),
        // Cost as well as tokens. OpenInference reports `llm.cost.*` directly, and extraction keeps
        // it, so a span can carry what a call cost without carrying the usage it was computed from.
        // Listing only tokens left such a span a plain span, hidden from every view that filters to
        // GenAI - which the commit adding this predicate claimed was fixed, and was not.
        format!("{prefix}gen_ai_cost_total > 0"),
    ]
    .join(" OR ")
}

pub const SPAN_FILTER_OPTION_COLUMNS: &[&str] = &[
    "environment",
    "framework",
    "gen_ai_agent_name",
    "gen_ai_request_model",
    "gen_ai_system",
    "observation_type",
    "session_id",
    "span_category",
    "span_name",
    "status_code",
    "user_id",
];

/// Trace filter options, as (view column, underlying span column).
pub const TRACE_FILTER_OPTION_COLUMNS: &[(&str, &str)] = &[
    ("environment", "environment"),
    ("session_id", "session_id"),
    ("trace_name", "span_name"),
    ("user_id", "user_id"),
];

/// Session filter options.
pub const SESSION_FILTER_OPTION_COLUMNS: &[&str] = &["environment", "user_id"];

/// Which rows a trace or session message query returns.
///
/// One definition shared by both backends: it was declared identically in the DuckDB and
/// ClickHouse repositories, so a change to one silently changed what the other returned. The
/// predicate is plain SQL that both dialects accept; if one ever needs to diverge, that
/// divergence should be visible here rather than by two constants drifting apart.
///
/// Rows with no messages, no tools and no error are never returned, which is why the
/// message-parsing harness applies the same filter when it builds its row sets.
pub const MESSAGE_CONTENT_FILTER: &str =
    "(messages != '[]' OR tool_definitions != '[]' OR tool_names != '[]' OR status_code = 'ERROR')";

// ============================================================================
// Row types
// ============================================================================

/// Raw span row from database for message queries.
///
/// Messages are stored as raw JSON at ingestion time.
/// The feed pipeline (process_spans) handles parsing, SideML conversion, and all processing.
#[derive(Debug, Clone)]
pub struct MessageSpanRow {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub span_timestamp: DateTime<Utc>,
    /// Span end time (for OUTPUT message ordering)
    pub span_end_timestamp: Option<DateTime<Utc>>,
    /// Raw messages (JSON string, converted to SideML at query time)
    pub messages_json: String,
    /// Tool definitions (JSON string)
    pub tool_definitions_json: String,
    /// Tool names (JSON string)
    pub tool_names_json: String,
    /// Span metadata
    pub model: Option<String>,
    pub provider: Option<String>,
    pub status_code: Option<String>,
    pub exception_type: Option<String>,
    pub exception_message: Option<String>,
    pub exception_stacktrace: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    pub cost_total: f64,
    /// Observation type for query-time role derivation (e.g., "Tool", "Generation")
    pub observation_type: Option<String>,
    /// Session ID for conversation grouping in feed API
    pub session_id: Option<String>,
    /// Ingestion time for cursor-based pagination in feed API
    pub ingested_at: DateTime<Utc>,
    /// Instrumentation scope: the library that produced the span, versioned. What makes a rule keyed
    /// on a producer's identity-and-version expressible at read time - the fact the design record's
    /// persistence audit found was never captured for spans. `None` on pre-v4 rows.
    pub scope_name: Option<String>,
    pub scope_version: Option<String>,
    /// The compact envelope: the facts a debugging caller needs beside the messages, loaded in the
    /// same query because a second span-sized request doubles the measured p50 and introduces a
    /// snapshot-consistency problem between the two reads. One envelope per span in the response,
    /// never repeated per block.
    pub span_name: Option<String>,
    pub framework: Option<String>,
    pub response_model: Option<String>,
    pub response_id: Option<String>,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub max_tokens: Option<i64>,
    /// Span-level finish reasons, as stored (a JSON array rendered to text).
    pub finish_reasons: Option<String>,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reasoning_tokens: i64,
    pub cost_input: f64,
    pub cost_output: f64,
}

impl SpanIdentity for MessageSpanRow {
    fn trace_id(&self) -> &str {
        &self.trace_id
    }
    fn span_id(&self) -> &str {
        &self.span_id
    }
    fn ordering_timestamp(&self) -> DateTime<Utc> {
        self.span_timestamp
    }
}

// ============================================================================
// Query results
// ============================================================================

/// Query result containing raw span rows.
///
/// Use process_spans() to process into messages.
#[derive(Debug)]
pub struct MessageQueryResult {
    pub rows: Vec<MessageSpanRow>,
}

// ============================================================================
// Query parameters
// ============================================================================

/// Parameters for project-wide message feed query.
#[derive(Debug, Default, Clone)]
pub struct FeedMessagesParams {
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
    /// Ignore spans ingested at or after this instant, in microseconds since the epoch.
    ///
    /// The traversal watermark, established on the first page and carried by the cursor, so a page and the
    /// reconstruction context loaded around it describe the same instant. See
    /// [`MessageQueryParams::ingested_before_us`] for the sequence that made a span vanish from every page.
    pub ingested_before_us: Option<i64>,
}

/// Unified parameters for message queries (trace, span, or session).
///
/// Priority: span_id > session_id > trace_id
#[derive(Debug, Default, Clone)]
pub struct MessageQueryParams {
    pub project_id: String,
    pub span_id: Option<String>,
    pub trace_id: Option<String>,
    pub session_id: Option<String>,
    /// Several traces at once, for the project feed: a page of the feed holds spans from many
    /// traces, and reconstruction has to see each of those traces whole. Lower priority than the
    /// single-entity selectors.
    ///
    /// An `Option` so that "no trace list given" and "a list that happens to be empty" cannot be
    /// confused. As a bare `Vec`, an empty one read as *unused*, and a caller whose only selector
    /// was this list then asked for the whole project with no content filter - an empty feed page
    /// turning into an unbounded read. `Some(empty)` matches nothing.
    pub trace_ids: Option<Vec<String>>,
    pub from_timestamp: Option<DateTime<Utc>>,
    pub to_timestamp: Option<DateTime<Utc>>,
    /// Ignore rows ingested at or after this instant, in microseconds since the epoch.
    ///
    /// The project feed's traversal watermark. A page is chosen by ingestion time, but the
    /// *reconstruction context* loaded around it was unbounded in that dimension - so a span ingested
    /// after the traversal began could enter the context, win deduplication against a span still to be
    /// paged, and then be scoped off the page it was not selected for. Neither copy was ever returned:
    /// the older one suppressed, the newer one filtered out.
    ///
    /// Bounding the context by the same watermark that bounds page selection makes a traversal a view of
    /// one instant. Only the feed sets it; the span, trace and session views are not paginated and read
    /// whatever is there.
    pub ingested_before_us: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_query_result() {
        let result = MessageQueryResult { rows: vec![] };
        assert!(result.rows.is_empty());
    }
}
