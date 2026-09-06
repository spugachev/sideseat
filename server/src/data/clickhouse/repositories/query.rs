//! Query repository for OTEL API queries (ClickHouse backend)
//!
//! Provides the same interface as DuckDB query repository but uses ClickHouse SQL.

use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use serde::Deserialize;

use crate::data::duckdb::filters::{Filter, columns};
use crate::data::types::{
    DisplayNameDialect, genai_span_predicate, trace_display_first, trace_display_name,
};

// ============================================================================
// Token Dedup SQL Fragments
// ============================================================================
// ClickHouse does not support correlated NOT EXISTS subqueries on the same table
// (always evaluates EXISTS as true). Use materialized CTE + NOT IN instead.

/// Build the dedup_lookup CTE with optional extra WHERE conditions.
///
/// Always requires 1 bind parameter: project_id.
/// `extra_where` is appended to narrow the materialized set:
/// - `""` for list queries (full project scan — required for correctness)
/// - `"trace_id = ?"` for single-trace queries (+1 bind param)
/// - `"trace_id IN (SELECT trace_id FROM session_traces)"` for session queries (no extra binds)
pub(super) fn build_dedup_lookup_cte(extra_where: &str) -> String {
    let extra = if extra_where.is_empty() {
        String::new()
    } else {
        format!("\n          AND {extra_where}")
    };
    format!(
        r#"dedup_lookup AS (
        SELECT span_id, parent_span_id, trace_id, observation_type
        FROM otel_spans FINAL
        WHERE project_id = ?
          AND ((gen_ai_usage_input_tokens + gen_ai_usage_output_tokens + gen_ai_usage_total_tokens + gen_ai_usage_cache_read_tokens + gen_ai_usage_cache_write_tokens + gen_ai_usage_reasoning_tokens) > 0 OR gen_ai_cost_total > 0){extra}
    )"#
    )
}

/// Anti-join condition for gen_totals, replacing 3 correlated NOT EXISTS subqueries.
/// Requires dedup_lookup CTE to be defined earlier in the WITH clause.
///
/// Each subquery is scoped to the same trace as the outer row via tuple NOT IN
/// (e.g. `(g.trace_id, g.span_id) NOT IN (SELECT trace_id, parent_span_id ...)`)
/// to maintain correctness without correlation: generation spans in trace A must
/// not affect token dedup for unrelated trace B. ClickHouse does not support
/// correlated subqueries inside IN/NOT IN (Code 48).
pub(super) const TOKEN_DEDUP_CONDITION: &str = r#"(
                  (g.observation_type = 'generation'
                   AND ((g.gen_ai_usage_input_tokens + g.gen_ai_usage_output_tokens + g.gen_ai_usage_total_tokens + g.gen_ai_usage_cache_read_tokens + g.gen_ai_usage_cache_write_tokens + g.gen_ai_usage_reasoning_tokens) > 0 OR g.gen_ai_cost_total > 0)
                   AND (g.trace_id, g.span_id) NOT IN (
                       SELECT trace_id, parent_span_id FROM dedup_lookup
                       WHERE observation_type = 'generation'
                         AND parent_span_id IS NOT NULL
                   ))
                  OR
                  ((g.observation_type IS NULL OR g.observation_type != 'generation')
                   AND ((g.gen_ai_usage_input_tokens + g.gen_ai_usage_output_tokens + g.gen_ai_usage_total_tokens + g.gen_ai_usage_cache_read_tokens + g.gen_ai_usage_cache_write_tokens + g.gen_ai_usage_reasoning_tokens) > 0 OR g.gen_ai_cost_total > 0)
                   AND g.trace_id NOT IN (
                       SELECT DISTINCT trace_id FROM dedup_lookup
                       WHERE observation_type = 'generation'
                   )
                   AND (g.parent_span_id IS NULL OR (g.trace_id, g.parent_span_id) NOT IN (
                       SELECT trace_id, span_id FROM dedup_lookup
                   )))
              )"#;

/// Build a dedup CTE scoped to traces that have at least one span in the
/// given time range. The CTE still loads ALL spans for those traces (no time
/// filter on the CTE itself) so that generation spans outside the time window
/// are still considered for token dedup.
///
/// Returns `(cte_sql, extra_bind_params)`. The extra params must be bound
/// between the base CTE project_id param and the rest of the query params.
///
/// When no time filters are provided, falls back to the unscoped CTE.
pub(super) fn build_time_scoped_dedup(
    project_id: &str,
    from: Option<&DateTime<Utc>>,
    to: Option<&DateTime<Utc>>,
) -> (String, Vec<QueryParam>) {
    if from.is_none() && to.is_none() {
        return (build_dedup_lookup_cte(""), vec![]);
    }

    let mut time_conds = vec!["project_id = ?".to_string()];
    let mut extra_binds = vec![QueryParam::String(project_id.to_string())];

    if let Some(f) = from {
        time_conds.push("timestamp_start >= fromUnixTimestamp64Micro(?)".to_string());
        extra_binds.push(QueryParam::Int64(f.timestamp_micros()));
    }
    if let Some(t) = to {
        time_conds.push("timestamp_start <= fromUnixTimestamp64Micro(?)".to_string());
        extra_binds.push(QueryParam::Int64(t.timestamp_micros()));
    }

    let scope = format!(
        "trace_id IN (SELECT DISTINCT trace_id FROM otel_spans WHERE {})",
        time_conds.join(" AND ")
    );

    (build_dedup_lookup_cte(&scope), extra_binds)
}

// ============================================================================
// Shared Projections
// ============================================================================

/// The token and cost sums every `gen_totals` CTE computes. Five queries repeated these twelve
/// aggregates verbatim, so adding a token kind meant finding all five.
const GEN_TOTALS_SUMS: &str = r#"sum(gen_ai_usage_input_tokens) AS input_tokens,
                sum(gen_ai_usage_output_tokens) AS output_tokens,
                sum(gen_ai_usage_total_tokens) AS total_tokens,
                sum(gen_ai_usage_cache_read_tokens) AS cache_read_tokens,
                sum(gen_ai_usage_cache_write_tokens) AS cache_write_tokens,
                sum(gen_ai_usage_reasoning_tokens) AS reasoning_tokens,
                sum(toFloat64(gen_ai_cost_input)) AS input_cost,
                sum(toFloat64(gen_ai_cost_output)) AS output_cost,
                sum(toFloat64(gen_ai_cost_cache_read)) AS cache_read_cost,
                sum(toFloat64(gen_ai_cost_cache_write)) AS cache_write_cost,
                sum(toFloat64(gen_ai_cost_reasoning)) AS reasoning_cost,
                sum(toFloat64(gen_ai_cost_total)) AS total_cost"#;

/// The columns a token/cost total exposes, in the order [`GEN_TOTALS_SUMS`] defines them.
const GEN_TOTALS_COLUMNS: [&str; 12] = [
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
    "input_cost",
    "output_cost",
    "cache_read_cost",
    "cache_write_cost",
    "reasoning_cost",
    "total_cost",
];

/// Body of the `gen_totals` CTE: deduplicated token and cost totals.
///
/// `key` is the column to group by (`g.trace_id`, `g.session_id`); `None` produces a single
/// row, which callers cross-join. `scope` is the WHERE predicate selecting the rows, and
/// carries whatever bind parameters it names. Requires `dedup_lookup` earlier in the WITH.
fn gen_totals_cte(key: Option<&str>, scope: &str) -> String {
    gen_totals_cte_joined(key, "", scope)
}

/// As [`gen_totals_cte`], with a JOIN.
///
/// The session list needs one: a session's totals cover the spans of its traces, and only a mapping
/// CTE knows which traces those are. ClickHouse rejects a correlated subquery inside a join
/// (Code 48), so the relation has to be joined rather than referenced from the WHERE.
fn gen_totals_cte_joined(key: Option<&str>, join: &str, scope: &str) -> String {
    // Aliased to the bare column name: a qualified expression keeps its qualifier as the output
    // column name in ClickHouse, so `SELECT st.session_id` produced a CTE whose column could not be
    // referenced as `gt.session_id`.
    let select_key = key
        .map(|k| {
            let bare = k.rsplit('.').next().unwrap_or(k);
            format!("{k} AS {bare},\n                ")
        })
        .unwrap_or_default();
    let group_by = key
        .map(|k| format!("\n            GROUP BY {k}"))
        .unwrap_or_default();
    let join = if join.is_empty() {
        String::new()
    } else {
        format!("\n            {join}")
    };
    format!(
        r#"gen_totals AS (
            SELECT
                {select_key}{GEN_TOTALS_SUMS}
            FROM otel_spans g FINAL{join}
            WHERE {scope}
              AND {dedup_condition}{group_by}
        )"#,
        dedup_condition = TOKEN_DEDUP_CONDITION,
    )
}

/// The twelve totals columns projected from a joined `gen_totals` row, each defaulted to 0.
/// Tokens and costs are never NULL in the API, so a trace or session with no generation span
/// reports zeros rather than nulls.
fn totals_projection(alias: &str, totals: &Totals) -> String {
    GEN_TOTALS_COLUMNS
        .iter()
        .map(|col| {
            let reference = match totals {
                Totals::Scalar => format!("{alias}.{col}"),
                Totals::Grouped => format!("max({alias}.{col})"),
            };
            format!("            coalesce({reference}, 0) AS {col},")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Every totals column, qualified by `alias`, for a GROUP BY that carries a cross-joined
/// `gen_totals` row through an aggregation.
fn totals_group_by(alias: &str) -> String {
    GEN_TOTALS_COLUMNS
        .iter()
        .map(|col| format!("{alias}.{col}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// How a query reaches its `gen_totals` row.
enum Totals {
    /// One row cross-joined in, already scalar — aggregating it would be a type error.
    Scalar,
    /// A per-key row joined into a grouped result, so each reference needs collapsing.
    Grouped,
}

/// The trace-row projection shared by the trace list, single-trace and session-traces queries.
///
/// Those three differ only in their row set and in how they reach `gen_totals`; the 30-column
/// projection was copied verbatim, so a fix to the tags union or the `trace_name` fallback had
/// to be applied three times or the three views disagreed. Must stay in sync with
/// [`ChTraceRow`] and with DuckDB's equivalent projection, which the ClickHouse parity test
/// (`server/tests/clickhouse_parity.rs`) checks against a live server.
fn trace_projection(trace_id_expr: &str, totals_alias: &str, totals: Totals) -> String {
    let totals_columns = totals_projection(totals_alias, &totals);

    format!(
        r#"{trace_id_expr} as trace_id,
            -- One definition of the displayed name, shared with the filter that has to match it and
            -- with DuckDB. It falls back to the earliest named span when no root span is present:
            -- without that a partial trace - root not yet ingested, or lost - had a name under DuckDB
            -- and none here.
            {trace_name} as trace_name,
            toInt64(toUnixTimestamp64Micro(min(s.timestamp_start))) as start_time,
            toInt64(toUnixTimestamp64Micro(max(coalesce(s.timestamp_end, s.timestamp_start)))) as end_time,
            dateDiff('millisecond', min(s.timestamp_start), max(coalesce(s.timestamp_end, s.timestamp_start))) as duration_ms,
            -- `(timestamp_start, span_id)`, a total order: see the DuckDB twin for why a tie left to the
            -- engine lets the displayed session and the feed's grouping disagree about one trace.
            argMinIf(s.session_id, (s.timestamp_start, s.span_id), s.session_id IS NOT NULL) as session_id,
            argMinIf(s.user_id, (s.timestamp_start, s.span_id), s.user_id IS NOT NULL) as user_id,
            argMinIf(s.environment, (s.timestamp_start, s.span_id), s.environment IS NOT NULL) as environment,
            count() AS span_count,
{totals_columns}
            -- Union and de-duplicate across the trace's spans, matching DuckDB's
            -- LIST_DISTINCT(FLATTEN(LIST(...))). any() returned one arbitrary span's tags, so a
            -- trace could show a subset of its tags, and which subset was nondeterministic.
            -- ifNull before JSONExtract: extracting an array from Nullable(String) yields
            -- Nullable(Array(String)), which ClickHouse rejects outright with
            -- "Nested type Array(String) cannot be inside Nullable type" - verified against a
            -- real server, it fails the whole query rather than degrading. JSONExtract('') is
            -- already [] so the empty case needs no branch. toNullable keeps the column
            -- Nullable(String), matching ChTraceRow::tags.
            toNullable(toJSONString(arrayDistinct(arrayFlatten(groupArray(
                JSONExtract(ifNull(s.tags, '[]'), 'Array(String)')
            ))))) AS tags,
            countIf(s.observation_type != 'span') AS observation_count,
            argMinIf(s.metadata, (s.timestamp_start, s.span_id), s.parent_span_id IS NULL) AS metadata,
            COALESCE(
                argMinIf(s.input_preview, (s.timestamp_start, s.span_id), s.parent_span_id IS NULL AND s.input_preview IS NOT NULL AND s.input_preview != ''),
                argMinIf(s.input_preview, (s.timestamp_start, s.span_id), s.input_preview IS NOT NULL AND s.input_preview != '')
            ) AS input_preview,
            COALESCE(
                -- argMax, like the fallback beside it: an output preview is the *last* thing produced.
                argMaxIf(s.output_preview, (s.timestamp_start, s.span_id), s.parent_span_id IS NULL AND s.output_preview IS NOT NULL AND s.output_preview != ''),
                argMaxIf(s.output_preview, (s.timestamp_start, s.span_id), s.output_preview IS NOT NULL AND s.output_preview != '')
            ) AS output_preview,
            coalesce(max(s.status_code = 'ERROR'), 0) AS has_error"#,
        trace_name = trace_display_name("s", DisplayNameDialect::ClickHouse)
    )
}

// ============================================================================
// Parameterized Query Builder
// ============================================================================

/// Query parameter that can be bound to ClickHouse queries.
/// All user-controllable values MUST go through this enum for SQL injection safety.
#[derive(Clone)]
pub(crate) enum QueryParam {
    /// String parameter (bound as-is)
    String(String),
    /// Integer parameter (used for timestamps as microseconds)
    Int64(i64),
    /// Floating point parameter. Numeric filter values need a numeric bind: ClickHouse compares a
    /// string literal against a numeric column by raising, not by coercing.
    Float64(f64),
}

/// Bind a sequence of collected parameters onto a query, in order.
///
/// Every call site had its own copy of this three-line match, so adding a parameter type meant a
/// compile error in each of them - which is how it was found.
pub(crate) fn bind_params<'a>(
    mut query: clickhouse::query::Query,
    params: impl IntoIterator<Item = &'a QueryParam>,
) -> clickhouse::query::Query {
    for param in params {
        query = match param {
            QueryParam::String(s) => query.bind(s.as_str()),
            QueryParam::Int64(i) => query.bind(i),
            QueryParam::Float64(f) => query.bind(f),
        };
    }
    query
}

/// The expression a trace list row displays for a filterable column, where that value is an
/// aggregate over the trace's spans. The ClickHouse counterpart of the DuckDB map, column for
/// column - a column treated as an aggregate on one backend and as a span attribute on the other
/// would make the same filter mean two things.
///
/// `None` means a plain span attribute, where "the trace has a span with this value" is the reading.
/// Aliases: `n` for the span rows, `gtf` for the joined totals.
fn ch_trace_aggregate_expression(view_column: &str) -> Option<String> {
    // coalesce, because a LEFT JOIN that found no totals row yields NULL and a comparison against
    // NULL is NULL rather than false - a trace with no usage would drop out of `< n` instead of
    // counting as the zero the list shows.
    let totals = |col: &str| Some(format!("coalesce(max(gtf.{col}), 0)"));
    match view_column {
        "trace_name" => Some(trace_display_name("n", DisplayNameDialect::ClickHouse)),
        // Displayed values too, though they are stored per span: the row shows the earliest span
        // that carries one. See `trace_display_first`.
        "session_id" | "user_id" | "environment" => Some(trace_display_first(
            view_column,
            "n",
            DisplayNameDialect::ClickHouse,
        )),
        "start_time" => Some("min(n.timestamp_start)".to_string()),
        "end_time" => Some("max(coalesce(n.timestamp_end, n.timestamp_start))".to_string()),
        "duration_ms" => Some(
            "dateDiff('millisecond', min(n.timestamp_start), \
             max(coalesce(n.timestamp_end, n.timestamp_start)))"
                .to_string(),
        ),
        "input_tokens" | "output_tokens" | "total_tokens" | "cache_read_tokens"
        | "cache_write_tokens" | "reasoning_tokens" | "input_cost" | "output_cost"
        | "cache_read_cost" | "cache_write_cost" | "reasoning_cost" | "total_cost" => {
            totals(view_column)
        }
        _ => None,
    }
}

/// Builder for constructing parameterized SQL WHERE clauses.
///
/// Collects conditions and their parameter values, then allows binding
/// all parameters to a ClickHouse query in order.
///
/// # SQL Injection Safety
/// All values that could potentially come from user input are parameterized.
/// Table names and column names are NOT parameterized but are validated
/// against whitelists before use.
#[derive(Default)]
struct ConditionBuilder {
    /// SQL conditions (public for special cases like tuple comparisons)
    pub conditions: Vec<String>,
    /// Parameter values to bind (public for special cases)
    pub params: Vec<QueryParam>,
}

impl ConditionBuilder {
    fn new() -> Self {
        Self::default()
    }

    /// Add an equality condition: `column = ?`
    fn add_eq(&mut self, column: &str, value: &str) {
        self.conditions.push(format!("{} = ?", column));
        self.params.push(QueryParam::String(value.to_string()));
    }

    /// Add an IN condition: `column IN (?, ?, ...)`
    fn add_in(&mut self, column: &str, values: &[String]) {
        if values.is_empty() {
            return;
        }
        let placeholders: Vec<&str> = values.iter().map(|_| "?").collect();
        self.conditions
            .push(format!("{} IN ({})", column, placeholders.join(", ")));
        for v in values {
            self.params.push(QueryParam::String(v.clone()));
        }
    }

    /// Add a raw condition without parameters (for static conditions only)
    ///
    /// # Safety
    /// The condition string must NOT contain any user input.
    fn add_raw(&mut self, condition: &str) {
        self.conditions.push(condition.to_string());
    }

    /// Add a timestamp >= condition using parameterized microseconds
    ///
    /// Uses `fromUnixTimestamp64Micro(?)` for type-safe binding.
    fn add_timestamp_gte(&mut self, column: &str, ts: &DateTime<Utc>) {
        self.conditions
            .push(format!("{} >= fromUnixTimestamp64Micro(?)", column));
        self.params.push(QueryParam::Int64(ts.timestamp_micros()));
    }

    /// Add a timestamp <= condition using parameterized microseconds
    fn add_timestamp_lte(&mut self, column: &str, ts: &DateTime<Utc>) {
        self.conditions
            .push(format!("{} <= fromUnixTimestamp64Micro(?)", column));
        self.params.push(QueryParam::Int64(ts.timestamp_micros()));
    }

    /// Add a timestamp < condition using parameterized microseconds
    fn add_timestamp_lt(&mut self, column: &str, ts: &DateTime<Utc>) {
        self.conditions
            .push(format!("{} < fromUnixTimestamp64Micro(?)", column));
        self.params.push(QueryParam::Int64(ts.timestamp_micros()));
    }

    /// Add the advanced filters a list request carries.
    ///
    /// `mapper` translates the request's view column names to span columns, the same mapping the
    /// The `WITH` prelude, the join and the binds a subquery needs to compare against a trace's totals.
    ///
    /// Scoped over the same rows the projection sums, time window included: every other condition here is
    /// trace-level and so cannot change a total, while the window still selects spans - and a filter compared
    /// against an all-time total would select traces by a number the row does not show.
    ///
    /// Self-contained, deliberately: the clause is embedded in the count query, which has no CTEs at all, and
    /// twice in the data query, whose own `gen_totals` is scoped differently.
    fn gen_totals_prelude(
        &self,
        project_id: &str,
        from: Option<&DateTime<Utc>>,
        to: Option<&DateTime<Utc>>,
    ) -> (String, String, Vec<QueryParam>) {
        let mut scope = "g.project_id = ?".to_string();
        let mut scope_params = vec![QueryParam::String(project_id.to_string())];
        if let Some(from) = from {
            scope.push_str(" AND g.timestamp_start >= fromUnixTimestamp64Micro(?)");
            scope_params.push(QueryParam::Int64(from.timestamp_micros()));
        }
        if let Some(to) = to {
            scope.push_str(" AND g.timestamp_start <= fromUnixTimestamp64Micro(?)");
            scope_params.push(QueryParam::Int64(to.timestamp_micros()));
        }
        let prelude = format!(
            "WITH {}, {} ",
            build_dedup_lookup_cte(""),
            gen_totals_cte(Some("g.trace_id"), &scope)
        );
        // Bound in the order the SQL names them: dedup_lookup's project, then gen_totals' scope.
        let mut params = vec![QueryParam::String(project_id.to_string())];
        params.extend(scope_params);
        (
            prelude,
            "LEFT JOIN gen_totals gtf ON gtf.trace_id = n.trace_id".to_string(),
            params,
        )
    }

    /// A condition on the **session**: it has a span satisfying `inner`, whatever span that is.
    ///
    /// The mirror of DuckDB's `session_scope_condition`, and the reasoning is recorded there: asked as a row
    /// predicate, a filter on a column only the session's *children* carry matched nothing (a child names no
    /// session and the row predicate requires one), a negation matched a session that used the value in one
    /// span and something else in the next, and it dropped every session with no value at all - exactly the
    /// ones a negation is asking for.
    ///
    /// Binds: the canonical relation's project, the span subquery's project, then the condition's own.
    fn push_session_scope(
        &mut self,
        session_col: &str,
        project_id: &str,
        inner: &str,
        negated: bool,
        inner_params: Vec<QueryParam>,
    ) {
        let quantifier = if negated { "NOT IN" } else { "IN" };
        self.conditions.push(format!(
            "{session_col} {quantifier} (SELECT cts.canonical_session FROM ({CANONICAL}) cts \
             WHERE cts.project_id = ? AND cts.trace_id IN \
             (SELECT n.trace_id FROM otel_spans n FINAL WHERE n.project_id = ? AND {inner}))",
            CANONICAL = CANONICAL_TRACE_SESSIONS,
        ));
        self.params.push(QueryParam::String(project_id.to_string()));
        self.params.push(QueryParam::String(project_id.to_string()));
        self.params.extend(inner_params);
    }

    /// The session list's filters, each a predicate on the session - see `push_session_scope`.
    fn add_session_filters<F>(&mut self, filters: &[Filter], mapper: F, project_id: &str)
    where
        F: for<'a> Fn(&'a str) -> &'a str + Copy,
    {
        for filter in filters {
            // A filter that states nothing contributes no condition: see `Filter::is_vacuous`.
            if filter.is_vacuous() {
                continue;
            }
            // A `session_id` filter is already a statement about the trace's canonical session.
            if self.push_canonical_session_filter(filter, "", project_id) {
                continue;
            }
            let twin = filter.positive_twin();
            let rendered = twin.as_ref().unwrap_or(filter);
            let mut inner_params: Vec<QueryParam> = Vec::new();
            let inner = crate::data::clickhouse::filters::to_clickhouse_sql(
                rendered,
                &mut inner_params,
                mapper,
                "n",
            );
            self.push_session_scope(
                "session_id",
                project_id,
                &inner,
                twin.is_some(),
                inner_params,
            );
        }
    }

    /// A `session_id` filter, as the condition on the **trace** it is. Returns whether it handled the filter.
    ///
    /// Every surface routes it here - the span list, the session list and the trace list - because the same
    /// question must not have three answers. Mapping it to the raw column made a session that only a later
    /// span named return that child, though every view displays its trace under a different session; and
    /// matching the trace list's *displayed* aggregate answered a negation with NULL, so a trace with no
    /// session was absent from "session is not A" there while its spans were present in the span list.
    ///
    /// A negated operator becomes `NOT IN` around the *positive* form: "not this session" means "its session
    /// is not this one", and the negation as written also drops traces with no session at all.
    fn push_canonical_session_filter(
        &mut self,
        filter: &Filter,
        alias: &str,
        project_id: &str,
    ) -> bool {
        if filter.column() != "session_id" {
            return false;
        }
        let twin = filter.positive_twin();
        let rendered = twin.as_ref().unwrap_or(filter);
        let quantifier = if twin.is_some() { "NOT IN" } else { "IN" };
        let condition = crate::data::clickhouse::filters::to_clickhouse_sql_against(
            rendered,
            &mut self.params,
            "cts.canonical_session",
        );
        // The project predicate comes *after* the condition, so its bind simply follows - no
        // insertion into the middle of a parameter list, which is where bind-order mistakes live.
        self.params.push(QueryParam::String(project_id.to_string()));
        let column = if alias.is_empty() {
            "trace_id".to_string()
        } else {
            format!("{alias}.trace_id")
        };
        self.conditions.push(format!(
            "{column} {quantifier} (SELECT cts.trace_id FROM ({CANONICAL}) cts \
             WHERE {condition} AND cts.project_id = ?)",
            CANONICAL = CANONICAL_TRACE_SESSIONS,
        ));
        true
    }

    /// DuckDB backend applies, so a filter means the same thing on both.
    fn add_filters<'a, F>(
        &mut self,
        filters: &'a [Filter],
        mapper: F,
        alias: &str,
        project_id: &str,
    ) where
        F: Fn(&'a str) -> &'a str + Copy,
    {
        self.add_filters_except(filters, mapper, alias, "", project_id);
    }

    /// Add the filters, skipping one column.
    ///
    /// The trace list holds `trace_name` back: it has to be matched against the name the list
    /// *displays*, which is an aggregate over the trace's spans, so it belongs in a HAVING rather
    /// than in a row predicate - matching the raw span name of any span meant selecting "agent" also
    /// returned traces displayed under another name that merely contained an agent span.
    fn add_filters_except<'a, F>(
        &mut self,
        filters: &'a [Filter],
        mapper: F,
        alias: &str,
        skip_column: &str,
        project_id: &str,
    ) where
        F: Fn(&'a str) -> &'a str + Copy,
    {
        for filter in filters {
            // A filter that states nothing contributes no condition: see `Filter::is_vacuous`.
            if filter.is_vacuous() {
                continue;
            }
            if !skip_column.is_empty() && filter.column() == skip_column {
                continue;
            }
            // A `session_id` filter is a statement about the **trace**, not about a span row.
            if self.push_canonical_session_filter(filter, alias, project_id) {
                continue;
            }
            let condition = crate::data::clickhouse::filters::to_clickhouse_sql(
                filter,
                &mut self.params,
                mapper,
                alias,
            );
            self.conditions.push(condition);
        }
    }

    /// Add the trace list's filters, each as a predicate on the *trace* rather than on a span row.
    ///
    /// The reasoning is in the DuckDB copy of this: a trace's tokens, cost and duration are
    /// aggregates, so comparing one span row against them contradicts what the list displays, and
    /// two filters ANDed on one row ask for a span carrying both values, which the usual
    /// root-span/generation-child split does not have. Both backends have to mean the same thing
    /// here, and the parity test holds them to it.
    fn add_trace_filters(
        &mut self,
        filters: &[Filter],
        project_id: &str,
        from: Option<&DateTime<Utc>>,
        to: Option<&DateTime<Utc>>,
    ) {
        let mut aggregate_conditions: Vec<String> = Vec::new();
        let mut aggregate_params: Vec<QueryParam> = Vec::new();
        let mut join_totals = false;

        for filter in filters {
            // A filter that states nothing contributes no condition: see `Filter::is_vacuous`.
            if filter.is_vacuous() {
                continue;
            }
            // Through the canonical relation here too - see `push_canonical_session_filter` for why the
            // displayed aggregate is the wrong basis for a negation.
            if self.push_canonical_session_filter(filter, "", project_id) {
                continue;
            }
            match ch_trace_aggregate_expression(filter.column()) {
                Some(expression) => {
                    // A negated filter is the **complement** of its positive form - see the DuckDB
                    // copy: the negation rendered against the aggregate is NULL for a trace with no
                    // value at all, so "none of x" dropped exactly the traces that are not x.
                    if let Some(twin) = filter.positive_twin() {
                        let mut inner_params: Vec<QueryParam> = Vec::new();
                        let condition = crate::data::clickhouse::filters::to_clickhouse_sql_against(
                            &twin,
                            &mut inner_params,
                            &expression,
                        );
                        let (prelude, totals_join, join_params) = if expression.contains("gtf.") {
                            self.gen_totals_prelude(project_id, from, to)
                        } else {
                            (String::new(), String::new(), Vec::new())
                        };
                        self.conditions.push(format!(
                            "trace_id NOT IN ({prelude}SELECT n.trace_id FROM otel_spans n FINAL \
                             {totals_join} WHERE n.project_id = ? \
                             GROUP BY n.project_id, n.trace_id HAVING {condition})"
                        ));
                        self.params.extend(join_params);
                        self.params.push(QueryParam::String(project_id.to_string()));
                        self.params.extend(inner_params);
                        continue;
                    }
                    aggregate_conditions.push(
                        crate::data::clickhouse::filters::to_clickhouse_sql_against(
                            filter,
                            &mut aggregate_params,
                            &expression,
                        ),
                    );
                    join_totals |= expression.contains("gtf.");
                }
                None => {
                    // A column no row displays on its own. "Some span" for the positive form, "no
                    // span" for the negative - the complement of the positive, not the negated
                    // predicate. See the DuckDB copy for what that cost.
                    let twin = filter.positive_twin();
                    let rendered = twin.as_ref().unwrap_or(filter);
                    let quantifier = if twin.is_some() { "NOT IN" } else { "IN" };
                    let mut inner_params: Vec<QueryParam> = Vec::new();
                    let condition = crate::data::clickhouse::filters::to_clickhouse_sql(
                        rendered,
                        &mut inner_params,
                        columns::map_trace_column_to_spans,
                        "n",
                    );
                    self.conditions.push(format!(
                        "trace_id {quantifier} (SELECT n.trace_id FROM otel_spans n FINAL \
                         WHERE n.project_id = ? AND {condition})"
                    ));
                    self.params.push(QueryParam::String(project_id.to_string()));
                    self.params.extend(inner_params);
                }
            }
        }

        if aggregate_conditions.is_empty() {
            return;
        }

        // The subquery carries its own WITH. `where_clause` is embedded in the count query, which
        // has no CTEs at all, and twice in the data query, whose own `gen_totals` is scoped
        // differently - so referencing the enclosing query's CTEs would work in one place and fail
        // in the other. Self-contained means one string that means the same thing everywhere.
        //
        // The cost of that: the clause appears twice per statement, so this scan can run twice.
        // Hoisting it into a named CTE per statement would fix it and means reworking the bind
        // scheme - `bind_to_n` binds the whole parameter set N times, which is what makes the
        // repetition work at all. Measured on DuckDB, where the shape is the same, an aggregate
        // filter roughly doubles an unfiltered list query and stays linear (219->446 ms at 4k
        // traces, 680->1447 ms at 20k), so this is a constant factor rather than a cliff.
        let (prelude, totals_join, join_params) = if join_totals {
            self.gen_totals_prelude(project_id, from, to)
        } else {
            (String::new(), String::new(), Vec::new())
        };
        self.params.extend(join_params);
        self.conditions.push(format!(
            "trace_id IN ({prelude}SELECT n.trace_id FROM otel_spans n FINAL {totals_join} \
             WHERE n.project_id = ? GROUP BY n.project_id, n.trace_id HAVING {})",
            aggregate_conditions.join(" AND ")
        ));
        self.params.push(QueryParam::String(project_id.to_string()));
        self.params.extend(aggregate_params);
    }

    /// Build the WHERE clause (without "WHERE" keyword)
    fn build(&self) -> String {
        self.conditions.join(" AND ")
    }

    /// Bind all collected parameters to a query.
    /// Returns a query ready for execution.
    fn bind_to(&self, mut query: clickhouse::query::Query) -> clickhouse::query::Query {
        for param in &self.params {
            query = match param {
                QueryParam::String(s) => query.bind(s),
                QueryParam::Int64(i) => query.bind(i),
                QueryParam::Float64(f) => query.bind(f),
            };
        }
        query
    }

    /// Bind parameters multiple times (for queries with repeated WHERE clauses in CTEs)
    fn bind_to_n(
        &self,
        mut query: clickhouse::query::Query,
        times: usize,
    ) -> clickhouse::query::Query {
        for _ in 0..times {
            for param in &self.params {
                query = match param {
                    QueryParam::String(s) => query.bind(s),
                    QueryParam::Int64(i) => query.bind(i),
                    QueryParam::Float64(f) => query.bind(f),
                };
            }
        }
        query
    }
}

use crate::core::constants::{QUERY_MAX_FILTER_SUGGESTIONS, QUERY_MAX_SPANS_PER_TRACE};
use crate::data::clickhouse::ClickhouseError;
use crate::data::types::{
    EventRow, FeedSpansParams, LinkRow, ListSessionsParams, ListSpansParams, ListTracesParams,
    SESSION_FILTER_OPTION_COLUMNS, SPAN_FILTER_OPTION_COLUMNS, SessionRow, SpanRow,
    TRACE_FILTER_OPTION_COLUMNS, TraceRow, parse_finish_reasons, parse_tags,
};
use crate::utils::time::parse_iso_timestamp;

/// ClickHouse row for trace queries
#[derive(Row, Deserialize)]
struct ChTraceRow {
    trace_id: String,
    trace_name: Option<String>,
    start_time: i64,
    end_time: i64,
    duration_ms: i64,
    session_id: Option<String>,
    user_id: Option<String>,
    environment: Option<String>,
    span_count: u64,
    input_tokens: i64,
    output_tokens: i64,
    total_tokens: i64,
    cache_read_tokens: i64,
    cache_write_tokens: i64,
    reasoning_tokens: i64,
    input_cost: f64,
    output_cost: f64,
    cache_read_cost: f64,
    cache_write_cost: f64,
    reasoning_cost: f64,
    total_cost: f64,
    tags: Option<String>,
    observation_count: u64,
    metadata: Option<String>,
    input_preview: Option<String>,
    output_preview: Option<String>,
    has_error: u8,
}

impl From<ChTraceRow> for TraceRow {
    fn from(row: ChTraceRow) -> Self {
        Self {
            trace_id: row.trace_id,
            trace_name: row.trace_name,
            start_time: DateTime::from_timestamp_micros(row.start_time)
                .unwrap_or(DateTime::UNIX_EPOCH),
            end_time: Some(
                DateTime::from_timestamp_micros(row.end_time).unwrap_or(DateTime::UNIX_EPOCH),
            ),
            duration_ms: Some(row.duration_ms),
            session_id: row.session_id,
            user_id: row.user_id,
            environment: row.environment,
            span_count: row.span_count as i64,
            input_tokens: row.input_tokens,
            output_tokens: row.output_tokens,
            total_tokens: row.total_tokens,
            cache_read_tokens: row.cache_read_tokens,
            cache_write_tokens: row.cache_write_tokens,
            reasoning_tokens: row.reasoning_tokens,
            input_cost: row.input_cost,
            output_cost: row.output_cost,
            cache_read_cost: row.cache_read_cost,
            cache_write_cost: row.cache_write_cost,
            reasoning_cost: row.reasoning_cost,
            total_cost: row.total_cost,
            tags: parse_tags(&row.tags),
            observation_count: row.observation_count as i64,
            metadata: row.metadata,
            input_preview: row.input_preview,
            output_preview: row.output_preview,
            has_error: row.has_error != 0,
        }
    }
}

/// ClickHouse row for span queries
#[derive(Row, Deserialize)]
struct ChSpanRow {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    span_name: Option<String>,
    span_kind: Option<String>,
    span_category: Option<String>,
    observation_type: Option<String>,
    framework: Option<String>,
    status_code: Option<String>,
    start_time: i64,
    end_time: Option<i64>,
    duration_ms: Option<i64>,
    environment: Option<String>,
    resource_attributes: Option<String>,
    session_id: Option<String>,
    user_id: Option<String>,
    gen_ai_system: Option<String>,
    gen_ai_request_model: Option<String>,
    gen_ai_agent_name: Option<String>,
    gen_ai_finish_reasons: Option<String>,
    gen_ai_usage_input_tokens: i64,
    gen_ai_usage_output_tokens: i64,
    gen_ai_usage_total_tokens: i64,
    gen_ai_usage_cache_read_tokens: i64,
    gen_ai_usage_cache_write_tokens: i64,
    gen_ai_usage_reasoning_tokens: i64,
    gen_ai_cost_input: f64,
    gen_ai_cost_output: f64,
    gen_ai_cost_cache_read: f64,
    gen_ai_cost_cache_write: f64,
    gen_ai_cost_reasoning: f64,
    gen_ai_cost_total: f64,
    gen_ai_usage_details: Option<String>,
    metadata: Option<String>,
    attributes: Option<String>,
    input_preview: Option<String>,
    output_preview: Option<String>,
    raw_span: Option<String>,
    ingested_at_us: i64,
    scope_name: Option<String>,
    scope_version: Option<String>,
}

impl From<ChSpanRow> for SpanRow {
    fn from(row: ChSpanRow) -> Self {
        Self {
            trace_id: row.trace_id,
            span_id: row.span_id,
            parent_span_id: row.parent_span_id,
            span_name: row.span_name,
            span_kind: row.span_kind,
            span_category: row.span_category,
            observation_type: row.observation_type,
            framework: row.framework,
            status_code: row.status_code,
            timestamp_start: DateTime::from_timestamp_micros(row.start_time)
                .unwrap_or(DateTime::UNIX_EPOCH),
            timestamp_end: row.end_time.and_then(DateTime::from_timestamp_micros),
            duration_ms: row.duration_ms,
            environment: row.environment,
            resource_attributes: row.resource_attributes,
            session_id: row.session_id,
            user_id: row.user_id,
            gen_ai_system: row.gen_ai_system,
            gen_ai_request_model: row.gen_ai_request_model,
            gen_ai_agent_name: row.gen_ai_agent_name,
            gen_ai_finish_reasons: parse_finish_reasons(&row.gen_ai_finish_reasons),
            gen_ai_usage_input_tokens: row.gen_ai_usage_input_tokens,
            gen_ai_usage_output_tokens: row.gen_ai_usage_output_tokens,
            gen_ai_usage_total_tokens: row.gen_ai_usage_total_tokens,
            gen_ai_usage_cache_read_tokens: row.gen_ai_usage_cache_read_tokens,
            gen_ai_usage_cache_write_tokens: row.gen_ai_usage_cache_write_tokens,
            gen_ai_usage_reasoning_tokens: row.gen_ai_usage_reasoning_tokens,
            gen_ai_cost_input: row.gen_ai_cost_input,
            gen_ai_cost_output: row.gen_ai_cost_output,
            gen_ai_cost_cache_read: row.gen_ai_cost_cache_read,
            gen_ai_cost_cache_write: row.gen_ai_cost_cache_write,
            gen_ai_cost_reasoning: row.gen_ai_cost_reasoning,
            gen_ai_cost_total: row.gen_ai_cost_total,
            gen_ai_usage_details: row.gen_ai_usage_details,
            metadata: row.metadata,
            attributes: row.attributes,
            input_preview: row.input_preview,
            output_preview: row.output_preview,
            raw_span: row.raw_span,
            scope_name: row.scope_name,
            scope_version: row.scope_version,
            ingested_at: DateTime::from_timestamp_micros(row.ingested_at_us)
                .unwrap_or(DateTime::UNIX_EPOCH),
        }
    }
}

/// ClickHouse row for session queries
#[derive(Row, Deserialize)]
struct ChSessionRow {
    session_id: Option<String>,
    user_id: Option<String>,
    environment: Option<String>,
    start_time: i64,
    end_time: i64,
    trace_count: u64,
    span_count: u64,
    observation_count: u64,
    input_tokens: i64,
    output_tokens: i64,
    total_tokens: i64,
    cache_read_tokens: i64,
    cache_write_tokens: i64,
    reasoning_tokens: i64,
    input_cost: f64,
    output_cost: f64,
    cache_read_cost: f64,
    cache_write_cost: f64,
    reasoning_cost: f64,
    total_cost: f64,
}

impl From<ChSessionRow> for SessionRow {
    fn from(row: ChSessionRow) -> Self {
        Self {
            session_id: row.session_id.unwrap_or_default(),
            user_id: row.user_id,
            environment: row.environment,
            start_time: DateTime::from_timestamp_micros(row.start_time)
                .unwrap_or(DateTime::UNIX_EPOCH),
            end_time: Some(
                DateTime::from_timestamp_micros(row.end_time).unwrap_or(DateTime::UNIX_EPOCH),
            ),
            trace_count: row.trace_count as i64,
            span_count: row.span_count as i64,
            observation_count: row.observation_count as i64,
            input_tokens: row.input_tokens,
            output_tokens: row.output_tokens,
            total_tokens: row.total_tokens,
            cache_read_tokens: row.cache_read_tokens,
            cache_write_tokens: row.cache_write_tokens,
            reasoning_tokens: row.reasoning_tokens,
            input_cost: row.input_cost,
            output_cost: row.output_cost,
            cache_read_cost: row.cache_read_cost,
            cache_write_cost: row.cache_write_cost,
            reasoning_cost: row.reasoning_cost,
            total_cost: row.total_cost,
        }
    }
}

/// ClickHouse row for event queries
#[derive(Row, Deserialize)]
struct ChEventRow {
    span_id: String,
    event_index: i32,
    event_timestamp: String,
    event_name: Option<String>,
    attributes: Option<String>,
}

impl From<ChEventRow> for EventRow {
    fn from(row: ChEventRow) -> Self {
        Self {
            span_id: row.span_id,
            event_index: row.event_index,
            event_time: parse_iso_timestamp(&row.event_timestamp),
            event_name: row.event_name,
            attributes: row.attributes,
        }
    }
}

/// ClickHouse row for link queries
#[derive(Row, Deserialize)]
struct ChLinkRow {
    span_id: String,
    linked_trace_id: String,
    linked_span_id: String,
    attributes: Option<String>,
}

impl From<ChLinkRow> for LinkRow {
    fn from(row: ChLinkRow) -> Self {
        Self {
            span_id: row.span_id,
            linked_trace_id: row.linked_trace_id,
            linked_span_id: row.linked_span_id,
            attributes: row.attributes,
        }
    }
}

/// List traces with pagination and filtering
pub async fn list_traces(
    client: &Client,
    params: &ListTracesParams,
) -> Result<(Vec<TraceRow>, u64), ClickhouseError> {
    // Build WHERE conditions using parameterized queries
    let mut cb = ConditionBuilder::new();
    cb.add_eq("project_id", &params.project_id);

    if let Some(ref sid) = params.session_id {
        // session_id is only on root spans; use trace_id subquery to include all spans
        cb.conditions
            .push(format!("trace_id IN ({TRACES_OF_SESSION})"));
        cb.params.extend(
            traces_of_session_binds(&params.project_id, sid)
                .into_iter()
                .map(QueryParam::String),
        );
    }
    // Trace-level, like the session above: a user id is on the spans that carry one, so a row
    // predicate selected the same traces but summed only those spans, reporting a trace's tokens as
    // whatever the matching spans held. The DuckDB copy says the same.
    if let Some(ref uid) = params.user_id {
        cb.conditions.push(
            "trace_id IN (SELECT DISTINCT trace_id FROM otel_spans FINAL \
             WHERE project_id = ? AND user_id = ?)"
                .to_string(),
        );
        cb.params
            .push(QueryParam::String(params.project_id.clone()));
        cb.params.push(QueryParam::String(uid.clone()));
    }
    if let Some(ref envs) = params.environment
        && !envs.is_empty()
    {
        let placeholders: Vec<&str> = envs.iter().map(|_| "?").collect();
        cb.conditions.push(format!(
            "trace_id IN (SELECT DISTINCT trace_id FROM otel_spans FINAL \
             WHERE project_id = ? AND environment IN ({}))",
            placeholders.join(", ")
        ));
        cb.params
            .push(QueryParam::String(params.project_id.clone()));
        for env in envs {
            cb.params.push(QueryParam::String(env.clone()));
        }
    }
    if let Some(ref from) = params.from_timestamp {
        cb.add_timestamp_gte("timestamp_start", from);
    }
    if let Some(ref to) = params.to_timestamp {
        cb.add_timestamp_lte("timestamp_start", to);
    }

    // The UI's filter bar. Ignored entirely until recently: a trace list filtered by model, token
    // count, cost or error status came back unfiltered on this backend. Every filter selects
    // traces - see `add_trace_filters`.
    cb.add_trace_filters(
        &params.filters,
        &params.project_id,
        params.from_timestamp.as_ref(),
        params.to_timestamp.as_ref(),
    );

    let where_clause = cb.build();

    // The name filter is part of `where_clause`, so the count gets it for free.

    let (count_sql, needs_double_bind) = if !params.include_nongenai {
        (
            format!(
                // A GenAI trace is one with an observation *or* a span carrying GenAI
                // attributes. Requiring an observation hid traces from transport-level
                // instrumentation, which records gen_ai.* on a plain span - and hid them only on
                // this backend, because DuckDB accepts either.
                r#"SELECT count() as cnt FROM (
                       SELECT trace_id FROM otel_spans FINAL
                       WHERE {} AND trace_id IN (
                           SELECT trace_id FROM otel_spans FINAL WHERE {}
                             AND ({genai})
                       )
                       GROUP BY trace_id
                   )"#,
                where_clause,
                where_clause,
                genai = genai_span_predicate("")
            ),
            true,
        )
    } else {
        (
            format!(
                r#"SELECT count() as cnt FROM (
                       SELECT trace_id FROM otel_spans FINAL WHERE {}
                       GROUP BY trace_id
                   )"#,
                where_clause
            ),
            false,
        )
    };

    // Bind parameters (twice if subquery used), then the name filter's, which the HAVING names last.
    let bind_times = if needs_double_bind { 2 } else { 1 };
    let total: u64 = cb
        .bind_to_n(client.query(&count_sql), bind_times)
        .fetch_one()
        .await?;

    // Determine sort and pagination
    let (sort_field, sort_dir) = params
        .order_by
        .as_ref()
        .map(|o| {
            let dir = match o.direction {
                crate::api::types::OrderDirection::Desc => "DESC",
                crate::api::types::OrderDirection::Asc => "ASC",
            };
            (o.column.as_str(), dir)
        })
        .unwrap_or(("timestamp_start", "DESC"));

    // Every column TRACE_SORTABLE accepts is mapped; an unmapped one falls through to min_ts and
    // silently sorts by time, which is the same defect DuckDB had.
    let ch_sort_field = match sort_field {
        "start_time" => "min_ts",
        "end_time" => "max_ts",
        "duration_ms" => "duration_ms",
        "total_cost" => "total_cost",
        "total_tokens" => "total_tokens",
        "observation_count" => "observation_count",
        _ => "min_ts",
    };

    let offset = (params.page.saturating_sub(1)) * params.limit;

    // The HAVING clause: the GenAI predicate, and any filter on the displayed trace name.
    //
    // Its parameters bind after the WHERE's, in the order the SQL names them - the name filter sits
    // in the CTE that also carries `where_clause`, so its values follow that occurrence's.
    let mut having_parts: Vec<String> = Vec::new();
    if !params.include_nongenai {
        // Matches the count query and DuckDB: an observation, or a span carrying GenAI attributes.
        having_parts.push("observation_count > 0 OR genai_span_count > 0".to_string());
    }
    let having_clause = if having_parts.is_empty() {
        String::new()
    } else {
        format!("HAVING {}", having_parts.join(" AND "))
    };

    // Scope dedup CTE to traces in the time range when available.
    // The CTE loads ALL spans for matching traces (no time filter on CTE itself)
    // so generation spans outside the window still affect token dedup.
    let dedup = build_time_scoped_dedup(
        &params.project_id,
        params.from_timestamp.as_ref(),
        params.to_timestamp.as_ref(),
    );

    // Data query with CTEs
    let data_sql = format!(
        r#"
        WITH {dedup_cte},
        {gen_totals},
        filtered_traces AS (
            SELECT
                sp.project_id,
                sp.trace_id,
                min(sp.timestamp_start) as min_ts,
                max(coalesce(sp.timestamp_end, sp.timestamp_start)) as max_ts,
                dateDiff('millisecond', min(sp.timestamp_start), max(coalesce(sp.timestamp_end, sp.timestamp_start))) as duration_ms,
                coalesce(max(gt.total_cost), 0) as total_cost,
                -- Sortable, so computed here rather than falling through to min_ts.
                coalesce(max(gt.total_tokens), 0) as total_tokens,
                countIf(sp.observation_type != 'span') as observation_count,
                countIf({genai_sp}) as genai_span_count
            FROM otel_spans sp FINAL
            LEFT JOIN gen_totals gt ON sp.trace_id = gt.trace_id
            WHERE {where_clause}
            GROUP BY sp.project_id, sp.trace_id
            {having_clause}
            -- The same total key the outer query orders by; see the DuckDB copy.
            ORDER BY {ch_sort_field} {sort_dir}, min_ts {sort_dir}, sp.trace_id ASC
            LIMIT {limit} OFFSET {offset}
        )
        SELECT
            {projection}
        FROM filtered_traces t
        JOIN otel_spans s FINAL ON t.project_id = s.project_id AND t.trace_id = s.trace_id
        LEFT JOIN gen_totals gt2 ON t.trace_id = gt2.trace_id
        GROUP BY t.trace_id, t.min_ts, t.{ch_sort_field}
        -- Ordered by the column that was asked for; the outer query used to re-sort by min_ts, so
        -- only the direction of the requested sort survived. min_ts breaks ties so a page is
        -- deterministic.
        ORDER BY t.{ch_sort_field} {sort_dir}, t.min_ts {sort_dir}, t.trace_id ASC
        "#,
        dedup_cte = dedup.0,
        gen_totals = gen_totals_cte(Some("g.trace_id"), &where_clause),
        // The same definition the count query uses, qualified for this CTE's alias.
        genai_sp = genai_span_predicate("sp"),
        projection = trace_projection("t.trace_id", "gt2", Totals::Grouped),
        where_clause = where_clause,
        having_clause = having_clause,
        ch_sort_field = ch_sort_field,
        sort_dir = sort_dir,
        limit = params.limit,
        offset = offset
    );

    // Bind: dedup_lookup(project_id + time-scope params) + where_clause x2
    let query = client.query(&data_sql).bind(params.project_id.as_str());
    let query = bind_params(query, &dedup.1);
    let rows: Vec<ChTraceRow> = cb.bind_to_n(query, 2).fetch_all().await?;

    Ok((rows.into_iter().map(TraceRow::from).collect(), total))
}

/// Get spans for a specific trace
pub async fn get_spans_for_trace(
    client: &Client,
    project_id: &str,
    trace_id: &str,
) -> Result<Vec<SpanRow>, ClickhouseError> {
    let sql = format!(
        r#"
        SELECT
            trace_id,
            span_id,
            parent_span_id,
            span_name,
            span_kind,
            span_category,
            observation_type,
            framework,
            status_code,
            toInt64(toUnixTimestamp64Micro(timestamp_start)) as start_time,
            if(timestamp_end IS NOT NULL, toInt64(toUnixTimestamp64Micro(timestamp_end)), NULL) as end_time,
            duration_ms,
            environment,
            JSONExtractRaw(raw_span, 'resource', 'attributes') as resource_attributes,
            session_id,
            user_id,
            gen_ai_system,
            gen_ai_request_model,
            gen_ai_agent_name,
            gen_ai_finish_reasons,
            gen_ai_usage_input_tokens,
            gen_ai_usage_output_tokens,
            gen_ai_usage_total_tokens,
            gen_ai_usage_cache_read_tokens,
            gen_ai_usage_cache_write_tokens,
            gen_ai_usage_reasoning_tokens,
            toFloat64(gen_ai_cost_input) as gen_ai_cost_input,
            toFloat64(gen_ai_cost_output) as gen_ai_cost_output,
            toFloat64(gen_ai_cost_cache_read) as gen_ai_cost_cache_read,
            toFloat64(gen_ai_cost_cache_write) as gen_ai_cost_cache_write,
            toFloat64(gen_ai_cost_reasoning) as gen_ai_cost_reasoning,
            toFloat64(gen_ai_cost_total) as gen_ai_cost_total,
            gen_ai_usage_details,
            metadata,
            JSONExtractRaw(raw_span, 'attributes') as attributes,
            input_preview,
            output_preview,
            raw_span,
            toInt64(toUnixTimestamp64Micro(ingested_at)) as ingested_at_us,
            scope_name, scope_version
        FROM otel_spans FINAL
        WHERE project_id = ? AND trace_id = ?
        ORDER BY timestamp_start
        LIMIT {}
    "#,
        QUERY_MAX_SPANS_PER_TRACE
    );

    let rows: Vec<ChSpanRow> = client
        .query(&sql)
        .bind(project_id)
        .bind(trace_id)
        .fetch_all()
        .await?;

    let spans: Vec<SpanRow> = rows.into_iter().map(SpanRow::from).collect();
    Ok(spans)
}

/// Get a single span
pub async fn get_span(
    client: &Client,
    project_id: &str,
    trace_id: &str,
    span_id: &str,
) -> Result<Option<SpanRow>, ClickhouseError> {
    let sql = r#"
        SELECT
            trace_id,
            span_id,
            parent_span_id,
            span_name,
            span_kind,
            span_category,
            observation_type,
            framework,
            status_code,
            toInt64(toUnixTimestamp64Micro(timestamp_start)) as start_time,
            if(timestamp_end IS NOT NULL, toInt64(toUnixTimestamp64Micro(timestamp_end)), NULL) as end_time,
            duration_ms,
            environment,
            JSONExtractRaw(raw_span, 'resource', 'attributes') as resource_attributes,
            session_id,
            user_id,
            gen_ai_system,
            gen_ai_request_model,
            gen_ai_agent_name,
            gen_ai_finish_reasons,
            gen_ai_usage_input_tokens,
            gen_ai_usage_output_tokens,
            gen_ai_usage_total_tokens,
            gen_ai_usage_cache_read_tokens,
            gen_ai_usage_cache_write_tokens,
            gen_ai_usage_reasoning_tokens,
            toFloat64(gen_ai_cost_input) as gen_ai_cost_input,
            toFloat64(gen_ai_cost_output) as gen_ai_cost_output,
            toFloat64(gen_ai_cost_cache_read) as gen_ai_cost_cache_read,
            toFloat64(gen_ai_cost_cache_write) as gen_ai_cost_cache_write,
            toFloat64(gen_ai_cost_reasoning) as gen_ai_cost_reasoning,
            toFloat64(gen_ai_cost_total) as gen_ai_cost_total,
            gen_ai_usage_details,
            metadata,
            JSONExtractRaw(raw_span, 'attributes') as attributes,
            input_preview,
            output_preview,
            raw_span,
            toInt64(toUnixTimestamp64Micro(ingested_at)) as ingested_at_us,
            scope_name, scope_version
        FROM otel_spans FINAL
        WHERE project_id = ? AND trace_id = ? AND span_id = ?
        LIMIT 1
    "#;

    let row: Option<ChSpanRow> = client
        .query(sql)
        .bind(project_id)
        .bind(trace_id)
        .bind(span_id)
        .fetch_optional()
        .await?;

    Ok(row.map(SpanRow::from))
}

/// List spans with pagination and filtering
pub async fn list_spans(
    client: &Client,
    params: &ListSpansParams,
) -> Result<(Vec<SpanRow>, u64), ClickhouseError> {
    // Build WHERE conditions using parameterized queries
    let mut cb = ConditionBuilder::new();
    cb.add_eq("project_id", &params.project_id);

    if let Some(ref tid) = params.trace_id {
        cb.add_eq("trace_id", tid);
    }
    if let Some(ref sid) = params.session_id {
        // session_id is only on root spans; use trace_id subquery to include all spans
        cb.conditions
            .push(format!("trace_id IN ({TRACES_OF_SESSION})"));
        cb.params.extend(
            traces_of_session_binds(&params.project_id, sid)
                .into_iter()
                .map(QueryParam::String),
        );
    }
    if let Some(ref uid) = params.user_id {
        cb.add_eq("user_id", uid);
    }
    if let Some(ref envs) = params.environment
        && !envs.is_empty()
    {
        cb.add_in("environment", envs);
    }
    if let Some(ref cat) = params.span_category {
        cb.add_eq("span_category", cat);
    }
    if let Some(ref obs) = params.observation_type {
        cb.add_eq("observation_type", obs);
    }
    if let Some(ref fw) = params.framework {
        cb.add_eq("framework", fw);
    }
    if let Some(ref model) = params.gen_ai_request_model {
        cb.add_eq("gen_ai_request_model", model);
    }
    if let Some(ref status) = params.status_code {
        cb.add_eq("status_code", status);
    }
    if let Some(ref from) = params.from_timestamp {
        cb.add_timestamp_gte("timestamp_start", from);
    }
    if let Some(ref to) = params.to_timestamp {
        cb.add_timestamp_lte("timestamp_start", to);
    }
    // "GenAI only", the same definition the trace and session lists use; see the DuckDB copy for
    // what testing observation_type alone hid.
    if params.is_observation == Some(true) {
        cb.add_raw(&format!("({})", genai_span_predicate("")));
    }

    // The UI's filter bar, mapped through the span view's column names.
    cb.add_filters(
        &params.filters,
        columns::map_span_column,
        "",
        &params.project_id,
    );

    let where_clause = cb.build();

    // Count query
    let count_sql = format!(
        "SELECT count() as cnt FROM otel_spans FINAL WHERE {}",
        where_clause
    );
    let total: u64 = cb.bind_to(client.query(&count_sql)).fetch_one().await?;

    // Order - use safe whitelist mapping for defense in depth
    let order = params
        .order_by
        .as_ref()
        .map(|o| {
            // Whitelist mapping for span columns (matches API validation in SPAN_SORTABLE)
            let col = match o.column.as_str() {
                "start_time" | "timestamp_start" => "timestamp_start",
                "end_time" | "timestamp_end" => "timestamp_end",
                "duration_ms" => "duration_ms",
                "span_name" => "span_name",
                _ => "timestamp_start", // Safe default for unknown columns
            };
            let dir = match o.direction {
                crate::api::types::OrderDirection::Desc => "DESC",
                crate::api::types::OrderDirection::Asc => "ASC",
            };
            format!("{} {}", col, dir)
        })
        .unwrap_or_else(|| "timestamp_start DESC".to_string())
        // (trace_id, span_id) breaks ties so LIMIT/OFFSET paginate a total order; see the DuckDB
        // copy.
        + ", trace_id ASC, span_id ASC";

    let offset = (params.page.saturating_sub(1)) * params.limit;

    let data_sql = format!(
        r#"
        SELECT
            trace_id,
            span_id,
            parent_span_id,
            span_name,
            span_kind,
            span_category,
            observation_type,
            framework,
            status_code,
            toInt64(toUnixTimestamp64Micro(timestamp_start)) as start_time,
            if(timestamp_end IS NOT NULL, toInt64(toUnixTimestamp64Micro(timestamp_end)), NULL) as end_time,
            duration_ms,
            environment,
            JSONExtractRaw(raw_span, 'resource', 'attributes') as resource_attributes,
            session_id,
            user_id,
            gen_ai_system,
            gen_ai_request_model,
            gen_ai_agent_name,
            gen_ai_finish_reasons,
            gen_ai_usage_input_tokens,
            gen_ai_usage_output_tokens,
            gen_ai_usage_total_tokens,
            gen_ai_usage_cache_read_tokens,
            gen_ai_usage_cache_write_tokens,
            gen_ai_usage_reasoning_tokens,
            toFloat64(gen_ai_cost_input) as gen_ai_cost_input,
            toFloat64(gen_ai_cost_output) as gen_ai_cost_output,
            toFloat64(gen_ai_cost_cache_read) as gen_ai_cost_cache_read,
            toFloat64(gen_ai_cost_cache_write) as gen_ai_cost_cache_write,
            toFloat64(gen_ai_cost_reasoning) as gen_ai_cost_reasoning,
            toFloat64(gen_ai_cost_total) as gen_ai_cost_total,
            gen_ai_usage_details,
            metadata,
            JSONExtractRaw(raw_span, 'attributes') as attributes,
            input_preview,
            output_preview,
            raw_span,
            toInt64(toUnixTimestamp64Micro(ingested_at)) as ingested_at_us,
            scope_name, scope_version
        FROM otel_spans FINAL
        WHERE {}
        ORDER BY {}
        LIMIT {} OFFSET {}
        "#,
        where_clause, order, params.limit, offset
    );

    let rows: Vec<ChSpanRow> = cb.bind_to(client.query(&data_sql)).fetch_all().await?;

    Ok((rows.into_iter().map(SpanRow::from).collect(), total))
}

/// Get feed spans (cursor-based pagination for real-time updates)
pub async fn get_feed_spans(
    client: &Client,
    params: &FeedSpansParams,
) -> Result<Vec<SpanRow>, ClickhouseError> {
    // Build WHERE conditions using parameterized queries
    let mut cb = ConditionBuilder::new();
    cb.add_eq("project_id", &params.project_id);

    // Cursor condition - use parameterized comparison for both timestamp and span_id
    // The trace id is part of the cursor key: a span id is unique only within a trace.
    if let Some((cursor_time_us, cursor_span_id, cursor_trace_id)) = &params.cursor {
        cb.conditions.push(
            "(toInt64(toUnixTimestamp64Micro(ingested_at)), span_id, trace_id) < (?, ?, ?)"
                .to_string(),
        );
        cb.params.push(QueryParam::Int64(*cursor_time_us));
        cb.params.push(QueryParam::String(cursor_span_id.clone()));
        cb.params.push(QueryParam::String(cursor_trace_id.clone()));
    }

    // The traversal watermark, applied *inside* the deduplication - see
    // `ch_dedup_spans_as_of_watermark` for what bounding `FINAL`'s result instead of its choice did.
    let (source, watermark_bind) = match params.ingested_before_us {
        Some(watermark_us) => (
            ch_dedup_spans_as_of_watermark().to_string(),
            Some(watermark_us),
        ),
        None => ("otel_spans FINAL".to_string(), None),
    };

    // Time filters
    if let Some(ref start) = params.start_time {
        cb.add_timestamp_gte("timestamp_start", start);
    }
    if let Some(ref end) = params.end_time {
        cb.add_timestamp_lt("timestamp_start", end);
    }

    // "GenAI only", the same definition the trace and session lists use; see the DuckDB copy for
    // what testing observation_type alone hid.
    if params.is_observation == Some(true) {
        cb.add_raw(&format!("({})", genai_span_predicate("")));
    }

    let where_clause = cb.build();

    let sql = format!(
        r#"
        SELECT
            trace_id,
            span_id,
            parent_span_id,
            span_name,
            span_kind,
            span_category,
            observation_type,
            framework,
            status_code,
            toInt64(toUnixTimestamp64Micro(timestamp_start)) as start_time,
            if(timestamp_end IS NOT NULL, toInt64(toUnixTimestamp64Micro(timestamp_end)), NULL) as end_time,
            duration_ms,
            environment,
            JSONExtractRaw(raw_span, 'resource', 'attributes') as resource_attributes,
            session_id,
            user_id,
            gen_ai_system,
            gen_ai_request_model,
            gen_ai_agent_name,
            gen_ai_finish_reasons,
            gen_ai_usage_input_tokens,
            gen_ai_usage_output_tokens,
            gen_ai_usage_total_tokens,
            gen_ai_usage_cache_read_tokens,
            gen_ai_usage_cache_write_tokens,
            gen_ai_usage_reasoning_tokens,
            toFloat64(gen_ai_cost_input) as gen_ai_cost_input,
            toFloat64(gen_ai_cost_output) as gen_ai_cost_output,
            toFloat64(gen_ai_cost_cache_read) as gen_ai_cost_cache_read,
            toFloat64(gen_ai_cost_cache_write) as gen_ai_cost_cache_write,
            toFloat64(gen_ai_cost_reasoning) as gen_ai_cost_reasoning,
            toFloat64(gen_ai_cost_total) as gen_ai_cost_total,
            gen_ai_usage_details,
            metadata,
            JSONExtractRaw(raw_span, 'attributes') as attributes,
            input_preview,
            output_preview,
            raw_span,
            toInt64(toUnixTimestamp64Micro(ingested_at)) as ingested_at_us,
            scope_name, scope_version
        FROM {source}
        WHERE {}
        ORDER BY ingested_at DESC, span_id DESC, trace_id DESC
        LIMIT {}
        "#,
        where_clause, params.limit
    );

    // The dedup subquery is at the head of the FROM, so its placeholder precedes every condition's.
    let mut query = client.query(&sql);
    if let Some(watermark_us) = watermark_bind {
        query = query.bind(watermark_us);
    }
    let rows: Vec<ChSpanRow> = cb.bind_to(query).fetch_all().await?;

    Ok(rows.into_iter().map(SpanRow::from).collect())
}

/// `otel_spans`, deduplicated **as of** a watermark: one row per span, the newest that existed when the
/// traversal began.
///
/// The ClickHouse counterpart of `duckdb::repositories::query::dedup_spans_as_of_watermark`, and needed for
/// the same reason. `FROM otel_spans FINAL` collapses to the newest row over the *whole* table, so a
/// watermark applied as an outer condition rejects that choice without promoting the older row - and a span
/// first ingested before the traversal began and re-delivered during it vanished from every page. All three
/// watermarked queries here did that, so after the DuckDB side was fixed the two backends would have
/// disagreed about the same table.
///
/// `LIMIT 1 BY` after `ORDER BY ingested_at DESC` is the idiom: it keeps the first row per key in the ordered
/// stream, which is exactly "the newest below the bound". No `FINAL` is needed on top - this *is* the
/// deduplication, done explicitly and with the bound inside it.
///
/// It costs a scan of everything below the watermark, as the DuckDB form does; the outer query's own filters
/// are applied afterwards. Mirroring the reference backend is deliberate: a cheaper shape that dedups over a
/// filtered set would have to prove no filter can change which copy of a span wins, and a re-delivery may
/// carry a corrected `timestamp_start`, so it can.
///
/// # The one residual, which is a property of the storage engine, not a bug to fix here
///
/// `ReplacingMergeTree(ingested_at)` keeps only the newest version of a span after a background merge. So
/// this is faithfully "as of the watermark" only while the pre-watermark version physically survives as an
/// unmerged part. If a span had v1 (below the watermark) and is re-delivered as v2 (above it), and a merge
/// then discards v1, this query - like *any* query, `FINAL` included - can no longer see v1, and the span
/// moves to v2's position (past the watermark, so a later page rather than this traversal's). It is the
/// best any query can do on this engine: DuckDB's append-only table retains every version and so is exact,
/// but ClickHouse cannot be made to answer an as-of query for a version it has merged away without a
/// different engine (a versioned/collapsing tree, or a separate append-only ingestion log). The failure is
/// bounded - a *reorder within a paging session that straddles a merge*, never a duplicate (the `LIMIT 1 BY`
/// still yields one row) and never a permanent loss (the span is still queryable at v2). Stated so a reader
/// does not mistake the DuckDB-exact behaviour for a cross-backend guarantee.
pub(crate) fn ch_dedup_spans_as_of_watermark() -> &'static str {
    "(SELECT * FROM otel_spans WHERE toInt64(toUnixTimestamp64Micro(ingested_at)) < ? \
      ORDER BY ingested_at DESC LIMIT 1 BY project_id, trace_id, span_id)"
}

/// List sessions with pagination and filtering
pub async fn list_sessions(
    client: &Client,
    params: &ListSessionsParams,
) -> Result<(Vec<SessionRow>, u64), ClickhouseError> {
    // Build WHERE conditions using parameterized queries
    let mut cb = ConditionBuilder::new();
    cb.add_eq("project_id", &params.project_id);
    cb.add_raw("session_id IS NOT NULL");

    // Each a predicate on the session, not on a span row - see `push_session_scope`.
    if let Some(ref uid) = params.user_id {
        cb.push_session_scope(
            "session_id",
            &params.project_id,
            "n.user_id = ?",
            false,
            vec![QueryParam::String(uid.clone())],
        );
    }
    if let Some(ref envs) = params.environment
        && !envs.is_empty()
    {
        let placeholders: Vec<&str> = envs.iter().map(|_| "?").collect();
        cb.push_session_scope(
            "session_id",
            &params.project_id,
            &format!("n.environment IN ({})", placeholders.join(", ")),
            false,
            envs.iter().map(|e| QueryParam::String(e.clone())).collect(),
        );
    }
    if let Some(ref from) = params.from_timestamp {
        cb.add_timestamp_gte("timestamp_start", from);
    }
    if let Some(ref to) = params.to_timestamp {
        cb.add_timestamp_lte("timestamp_start", to);
    }

    // The UI's filter bar, mapped through the session view's column names.
    cb.add_session_filters(
        &params.filters,
        columns::map_session_column_to_spans,
        &params.project_id,
    );

    let where_clause = cb.build();

    let count_sql = format!(
        // Distinct **canonical** sessions among the traces the filter selects; see
        // `CANONICAL_TRACE_SESSIONS`. Counting distinct `session_id` over spans made the total exceed the
        // number of rows the list can return.
        // The filter stays in its own subquery over `otel_spans` alone, so its unqualified columns cannot
        // become ambiguous against the canonical relation beside them.
        "SELECT count(DISTINCT ts.canonical_session) as cnt FROM ({CANONICAL_TRACE_SESSIONS}) ts \
         WHERE (ts.project_id, ts.trace_id) IN ( \
           SELECT project_id, trace_id FROM otel_spans FINAL WHERE {})",
        where_clause
    );
    let total: u64 = cb.bind_to(client.query(&count_sql)).fetch_one().await?;

    // Determine sort
    let (sort_field, sort_dir) = params
        .order_by
        .as_ref()
        .map(|o| {
            let dir = match o.direction {
                crate::api::types::OrderDirection::Desc => "DESC",
                crate::api::types::OrderDirection::Asc => "ASC",
            };
            (o.column.as_str(), dir)
        })
        .unwrap_or(("timestamp_start", "DESC"));

    // Every column SESSION_SORTABLE accepts is mapped.
    let ch_sort_field = match sort_field {
        "start_time" => "min_ts",
        "end_time" => "max_ts",
        "total_cost" => "total_cost",
        "trace_count" => "trace_count",
        "span_count" => "span_count",
        "observation_count" => "observation_count",
        _ => "min_ts",
    };

    let offset = (params.page.saturating_sub(1)) * params.limit;

    let dedup = build_time_scoped_dedup(
        &params.project_id,
        params.from_timestamp.as_ref(),
        params.to_timestamp.as_ref(),
    );

    let data_sql = format!(
        r#"
        WITH {dedup_cte},
        trace_sessions AS ({CANONICAL_TRACE_SESSIONS}),
        matching_sessions AS (
            -- Which sessions the request selects, counted exactly as the count query counts them. The
            -- filter runs on spans, but the session it selects is the *canonical* one of that span's
            -- trace - see the DuckDB copy.
            SELECT DISTINCT ts.project_id as project_id, ts.canonical_session as session_id
            FROM otel_spans sp FINAL
            JOIN trace_sessions ts
              ON ts.project_id = sp.project_id AND ts.trace_id = sp.trace_id
            WHERE {where_clause}
        ),
        session_traces AS (
            -- Every trace of those sessions, not only the ones whose naming rows passed the filter:
            -- selection and membership are separate questions, and one predicate for both returned a
            -- partial session. Taken from `trace_sessions`, so each trace appears under exactly one
            -- session. See the DuckDB copy.
            SELECT ts.project_id as project_id, ts.canonical_session as session_id,
                   ts.trace_id as trace_id
            FROM trace_sessions ts
            JOIN matching_sessions ms
              ON ms.project_id = ts.project_id AND ms.session_id = ts.canonical_session
        ),
        {gen_totals},
        filtered_sessions AS (
            SELECT
                -- Aliased explicitly: a qualified column keeps its qualifier as the output name, so
                -- `stf.project_id` was not addressable as `f.project_id` outside.
                stf.project_id as project_id,
                stf.session_id as session_id,
                min(sp.timestamp_start) as min_ts,
                -- Sortable, so computed here rather than falling through to min_ts.
                max(coalesce(sp.timestamp_end, sp.timestamp_start)) as max_ts,
                coalesce(max(gt.total_cost), 0) as total_cost,
                count(DISTINCT sp.trace_id) as trace_count,
                count() as span_count,
                countIf(sp.observation_type != 'span') as observation_count
            FROM session_traces stf
            JOIN otel_spans sp FINAL
              ON sp.project_id = stf.project_id AND sp.trace_id = stf.trace_id
            LEFT JOIN gen_totals gt ON gt.session_id = stf.session_id
            GROUP BY stf.project_id, stf.session_id
            -- The same total key the outer query orders by; see the DuckDB copy.
            ORDER BY {ch_sort_field} {sort_dir}, min_ts {sort_dir}, stf.session_id ASC
            LIMIT {limit} OFFSET {offset}
        )
        SELECT
            -- `toNullable`: the canonical session comes through `assumeNotNull`, so it is a plain `String`
            -- here while the row type is `Option<String>` (the column is Nullable in the schema).
            toNullable(f.session_id) as session_id,
            argMinIf(s.user_id, (s.timestamp_start, s.span_id), s.user_id IS NOT NULL) as user_id,
            argMinIf(s.environment, (s.timestamp_start, s.span_id), s.environment IS NOT NULL) as environment,
            toInt64(toUnixTimestamp64Micro(min(s.timestamp_start))) as start_time,
            toInt64(toUnixTimestamp64Micro(max(coalesce(s.timestamp_end, s.timestamp_start)))) as end_time,
            count(DISTINCT s.trace_id) AS trace_count,
            count() AS span_count,
            countIf(s.observation_type != 'span') AS observation_count,
{totals}
        FROM filtered_sessions f
        -- A distinct alias per scope: ClickHouse leaks a CTE's internal aliases into the outer
        -- query, so reusing one made the join condition ambiguous.
        JOIN session_traces sto ON sto.project_id = f.project_id AND sto.session_id = f.session_id
        JOIN otel_spans s FINAL ON s.project_id = sto.project_id AND s.trace_id = sto.trace_id
        LEFT JOIN gen_totals gt2 ON gt2.session_id = f.session_id
        GROUP BY f.session_id, f.min_ts, f.{ch_sort_field}
        -- See the trace list: the requested column, not just its direction.
        ORDER BY f.{ch_sort_field} {sort_dir}, f.min_ts {sort_dir}, f.session_id ASC
        "#,
        dedup_cte = dedup.0,
        // A distinct alias inside the CTE: reusing `st` here made `st.project_id` ambiguous in the
        // outer query, which joins the same relation under that name.
        gen_totals = gen_totals_cte_joined(
            Some("stg.session_id"),
            "JOIN session_traces stg ON stg.project_id = g.project_id AND stg.trace_id = g.trace_id",
            "1 = 1"
        ),
        totals = totals_projection("gt2", &Totals::Grouped).trim_end_matches(','),
        where_clause = where_clause,
        ch_sort_field = ch_sort_field,
        sort_dir = sort_dir,
        limit = params.limit,
        offset = offset
    );

    // Bind: dedup_lookup(project_id + time-scope params) + where_clause once. It appears once now:
    // the conditions live in session_traces, and the aggregates read the traces it selected.
    let query = client.query(&data_sql).bind(params.project_id.as_str());
    let query = bind_params(query, &dedup.1);
    let rows: Vec<ChSessionRow> = cb.bind_to(query).fetch_all().await?;

    Ok((rows.into_iter().map(SessionRow::from).collect(), total))
}

/// Get session details
/// session_id is only on root spans; uses session_traces CTE to find all traces,
/// then queries all spans from those traces.
pub async fn get_session(
    client: &Client,
    project_id: &str,
    session_id: &str,
) -> Result<Option<SessionRow>, ClickhouseError> {
    let sql = format!(
        r#"
        WITH session_traces AS ({TRACES_OF_SESSION}),
        {dedup_cte},
        gen_totals AS (
            SELECT
                sum(gen_ai_usage_input_tokens) AS input_tokens,
                sum(gen_ai_usage_output_tokens) AS output_tokens,
                sum(gen_ai_usage_total_tokens) AS total_tokens,
                sum(gen_ai_usage_cache_read_tokens) AS cache_read_tokens,
                sum(gen_ai_usage_cache_write_tokens) AS cache_write_tokens,
                sum(gen_ai_usage_reasoning_tokens) AS reasoning_tokens,
                sum(toFloat64(gen_ai_cost_input)) AS input_cost,
                sum(toFloat64(gen_ai_cost_output)) AS output_cost,
                sum(toFloat64(gen_ai_cost_cache_read)) AS cache_read_cost,
                sum(toFloat64(gen_ai_cost_cache_write)) AS cache_write_cost,
                sum(toFloat64(gen_ai_cost_reasoning)) AS reasoning_cost,
                sum(toFloat64(gen_ai_cost_total)) AS total_cost
            FROM otel_spans g FINAL
            WHERE g.project_id = ?
              AND g.trace_id IN (SELECT trace_id FROM session_traces)
              AND {dedup_condition}
        )
        SELECT
            toNullable(?) as session_id,
            argMinIf(s.user_id, (s.timestamp_start, s.span_id), s.user_id IS NOT NULL) as user_id,
            argMinIf(s.environment, (s.timestamp_start, s.span_id), s.environment IS NOT NULL) as environment,
            toInt64(toUnixTimestamp64Micro(min(s.timestamp_start))) as start_time,
            toInt64(toUnixTimestamp64Micro(max(coalesce(s.timestamp_end, s.timestamp_start)))) as end_time,
            count(DISTINCT s.trace_id) AS trace_count,
            count() AS span_count,
            countIf(s.observation_type != 'span') AS observation_count,
            coalesce(gt.input_tokens, 0) AS input_tokens,
            coalesce(gt.output_tokens, 0) AS output_tokens,
            coalesce(gt.total_tokens, 0) AS total_tokens,
            coalesce(gt.cache_read_tokens, 0) AS cache_read_tokens,
            coalesce(gt.cache_write_tokens, 0) AS cache_write_tokens,
            coalesce(gt.reasoning_tokens, 0) AS reasoning_tokens,
            coalesce(gt.input_cost, 0) AS input_cost,
            coalesce(gt.output_cost, 0) AS output_cost,
            coalesce(gt.cache_read_cost, 0) AS cache_read_cost,
            coalesce(gt.cache_write_cost, 0) AS cache_write_cost,
            coalesce(gt.reasoning_cost, 0) AS reasoning_cost,
            coalesce(gt.total_cost, 0) AS total_cost
        FROM otel_spans s FINAL
        CROSS JOIN gen_totals gt
        WHERE s.project_id = ?
          AND s.trace_id IN (SELECT trace_id FROM session_traces)
        GROUP BY gt.input_tokens, gt.output_tokens, gt.total_tokens,
                 gt.cache_read_tokens, gt.cache_write_tokens, gt.reasoning_tokens,
                 gt.input_cost, gt.output_cost, gt.cache_read_cost, gt.cache_write_cost,
                 gt.reasoning_cost, gt.total_cost
    "#,
        dedup_cte = build_dedup_lookup_cte("trace_id IN (SELECT trace_id FROM session_traces)"),
        dedup_condition = TOKEN_DEDUP_CONDITION,
    );

    // Bind order: session_traces (four - see `traces_of_session_binds`), dedup_lookup(project_id),
    //             gen_totals(project_id), SELECT(session_id), main(project_id)
    let mut query = client.query(&sql);
    for bind in traces_of_session_binds(project_id, session_id) {
        query = query.bind(bind);
    }
    let row: Option<ChSessionRow> = query
        .bind(project_id)
        .bind(project_id)
        .bind(session_id)
        .bind(project_id)
        .fetch_optional()
        .await?;

    Ok(row.map(SessionRow::from))
}

/// Get events for a span (extracted from raw_span JSON)
pub async fn get_events_for_span(
    client: &Client,
    project_id: &str,
    trace_id: &str,
    span_id: &str,
) -> Result<Vec<EventRow>, ClickhouseError> {
    // Use JSONExtractArrayRaw to get events array, then parse
    // LIMIT prevents memory exhaustion with pathological data
    let sql = format!(
        r#"
        SELECT
            span_id,
            toInt32(arrayJoin(range(JSONLength(raw_span, 'events')))) as event_index,
            -- ifNull strips the Nullable that extracting from a Nullable(String) column
            -- introduces. ChEventRow declares event_timestamp as String, and the crate refuses
            -- Nullable(String) -> String, so this endpoint failed outright on ClickHouse. The
            -- WHERE below already restricts to spans whose raw JSON has events, so the default is
            -- unreachable; it exists to make the column's type say so.
            ifNull(JSONExtractString(JSONExtractRaw(raw_span, 'events', arrayJoin(range(JSONLength(raw_span, 'events'))) + 1), 'timestamp'), '') as event_timestamp,
            JSONExtractString(JSONExtractRaw(raw_span, 'events', arrayJoin(range(JSONLength(raw_span, 'events'))) + 1), 'name') as event_name,
            JSONExtractRaw(JSONExtractRaw(raw_span, 'events', arrayJoin(range(JSONLength(raw_span, 'events'))) + 1), 'attributes') as attributes
        FROM otel_spans FINAL
        WHERE project_id = ? AND trace_id = ? AND span_id = ?
          AND JSONLength(raw_span, 'events') > 0
        ORDER BY event_index
        LIMIT {}
    "#,
        QUERY_MAX_SPANS_PER_TRACE
    );

    let rows: Vec<ChEventRow> = client
        .query(&sql)
        .bind(project_id)
        .bind(trace_id)
        .bind(span_id)
        .fetch_all()
        .await?;

    Ok(rows.into_iter().map(EventRow::from).collect())
}

/// Get links for a span (extracted from raw_span JSON)
pub async fn get_links_for_span(
    client: &Client,
    project_id: &str,
    trace_id: &str,
    span_id: &str,
) -> Result<Vec<LinkRow>, ClickhouseError> {
    // LIMIT prevents memory exhaustion with pathological data
    let sql = format!(
        r#"
        SELECT
            span_id,
            -- ifNull for the same reason as the event query: ChLinkRow declares these as String.
            ifNull(JSONExtractString(JSONExtractRaw(raw_span, 'links', arrayJoin(range(JSONLength(raw_span, 'links'))) + 1), 'trace_id'), '') as linked_trace_id,
            ifNull(JSONExtractString(JSONExtractRaw(raw_span, 'links', arrayJoin(range(JSONLength(raw_span, 'links'))) + 1), 'span_id'), '') as linked_span_id,
            JSONExtractRaw(JSONExtractRaw(raw_span, 'links', arrayJoin(range(JSONLength(raw_span, 'links'))) + 1), 'attributes') as attributes
        FROM otel_spans FINAL
        WHERE project_id = ? AND trace_id = ? AND span_id = ?
          AND JSONLength(raw_span, 'links') > 0
        LIMIT {}
    "#,
        QUERY_MAX_SPANS_PER_TRACE
    );

    let rows: Vec<ChLinkRow> = client
        .query(&sql)
        .bind(project_id)
        .bind(trace_id)
        .bind(span_id)
        .fetch_all()
        .await?;

    Ok(rows.into_iter().map(LinkRow::from).collect())
}

/// Get a single trace by ID
pub async fn get_trace(
    client: &Client,
    project_id: &str,
    trace_id: &str,
) -> Result<Option<TraceRow>, ClickhouseError> {
    let sql = format!(
        r#"
        WITH {dedup_cte},
        {gen_totals}
        SELECT
            {projection}
        FROM otel_spans s FINAL
        CROSS JOIN gen_totals gt
        WHERE s.project_id = ? AND s.trace_id = ?
        GROUP BY s.trace_id, {totals_group_by}
    "#,
        dedup_cte = build_dedup_lookup_cte("trace_id = ?"),
        gen_totals = gen_totals_cte(None, "g.project_id = ? AND g.trace_id = ?"),
        projection = trace_projection("s.trace_id", "gt", Totals::Scalar),
        totals_group_by = totals_group_by("gt"),
    );

    // Bind order: dedup_lookup(project_id, trace_id), gen_totals(project_id, trace_id), main(project_id, trace_id)
    let row: Option<ChTraceRow> = client
        .query(&sql)
        .bind(project_id)
        .bind(trace_id)
        .bind(project_id)
        .bind(trace_id)
        .bind(project_id)
        .bind(trace_id)
        .fetch_optional()
        .await?;

    Ok(row.map(TraceRow::from))
}

/// Get traces for a session
/// session_id is only on root spans; uses session_traces CTE to find all traces,
/// then queries all spans from those traces.
pub async fn get_traces_for_session(
    client: &Client,
    project_id: &str,
    session_id: &str,
) -> Result<Vec<TraceRow>, ClickhouseError> {
    let sql = format!(
        r#"
        WITH session_traces AS ({TRACES_OF_SESSION}),
        {dedup_cte},
        {gen_totals}
        SELECT
            {projection}
        FROM otel_spans s FINAL
        LEFT JOIN gen_totals gt ON s.trace_id = gt.trace_id
        WHERE s.project_id = ?
          AND s.trace_id IN (SELECT trace_id FROM session_traces)
        GROUP BY s.trace_id
        ORDER BY min(s.timestamp_start) DESC
    "#,
        dedup_cte = build_dedup_lookup_cte("trace_id IN (SELECT trace_id FROM session_traces)"),
        gen_totals = gen_totals_cte(
            Some("g.trace_id"),
            "g.project_id = ?\n              AND g.trace_id IN (SELECT trace_id FROM session_traces)"
        ),
        projection = trace_projection("s.trace_id", "gt", Totals::Grouped),
    );

    // Bind order: session_traces (four - see `traces_of_session_binds`), dedup_lookup(project_id),
    //             gen_totals(project_id), main(project_id)
    let mut query = client.query(&sql);
    for bind in traces_of_session_binds(project_id, session_id) {
        query = query.bind(bind);
    }
    let rows: Vec<ChTraceRow> = query
        .bind(project_id)
        .bind(project_id)
        .bind(project_id)
        .fetch_all()
        .await?;

    Ok(rows.into_iter().map(TraceRow::from).collect())
}

/// The traces of one session. Four `?` in the order [`traces_of_session_binds`] returns them.
///
/// The DuckDB twin is `TRACES_OF_SESSION` there, and the reasoning is recorded on it: a trace belongs to the
/// session on its **earliest** span (what the row displays and the feed groups by), and `FINAL` is narrowed to
/// the candidate traces rather than applied to the whole project, which on DuckDB was the difference between
/// 1.73x and 1.26x the cost of the loose predicate.
pub(crate) const TRACES_OF_SESSION: &str = "SELECT trace_id FROM ( \
       SELECT trace_id, argMin(assumeNotNull(session_id), (timestamp_start, span_id)) AS canonical_session \
       FROM otel_spans FINAL \
       WHERE project_id = ? \
       AND trace_id IN (SELECT trace_id FROM otel_spans WHERE project_id = ? AND session_id = ?) \
       AND session_id IS NOT NULL AND session_id != '' \
       GROUP BY trace_id \
     ) WHERE canonical_session = ?";

/// The canonical session of every trace that has one: `project_id, trace_id, session_id`.
///
/// The DuckDB twin is `CANONICAL_TRACE_SESSIONS`, and the reasoning is recorded there: a trace belongs to the
/// session on its earliest span, so selecting distinct `session_id` from spans attributed a trace whose spans
/// name A and B to both - two sessions in the list, each claiming that trace's full spans, tokens and cost.
/// The output column is `canonical_session`, not `session_id`: a SELECT alias is visible in WHERE in
/// ClickHouse and shadows the column, so `AS session_id` made the predicate read the aggregate and the query
/// failed outright with ILLEGAL_AGGREGATION - on ClickHouse only. CLAUDE.md gotcha 17, caught by the parity
/// comparison for the second time in two days.
pub(crate) const CANONICAL_TRACE_SESSIONS: &str = "SELECT project_id, trace_id, \
       argMin(assumeNotNull(session_id), (timestamp_start, span_id)) AS canonical_session \
     FROM otel_spans FINAL \
     WHERE session_id IS NOT NULL AND session_id != '' \
     GROUP BY project_id, trace_id";

/// The binds [`TRACES_OF_SESSION`] needs, in the order its placeholders appear.
pub(crate) fn traces_of_session_binds(project_id: &str, session_id: &str) -> [String; 4] {
    [
        project_id.to_string(),
        project_id.to_string(),
        session_id.to_string(),
        session_id.to_string(),
    ]
}

/// Which session each of the given traces belongs to; traces with none are absent.
///
/// The DuckDB twin. `FINAL` for the same reason it deduplicates there, and `argMin` over
/// `(timestamp_start, span_id)` so the session is the one on the trace's earliest span - which is what the
/// trace and session views display. `min(session_id)` picked the lexicographically smallest instead, so a
/// trace could be displayed under one session and grouped under another.
/// The relation a membership query reads: deduplicated as of a watermark, or plain `FINAL`.
///
/// The three membership methods used to accept `as_of_us` and ignore it, on the grounds that "`FINAL` has no
/// as-of form" - which stopped being true when `ch_dedup_spans_as_of_watermark` was written for the message
/// rows. Ignoring it made a feed traversal read watermark-era *rows* and current *membership*: a trace
/// re-delivered into another session mid-traversal was reconstructed as its old version while its session,
/// and therefore the context loaded around it, came from the new one - so the traversal was not a view of one
/// instant, which is the whole point of the watermark. The residual documented on
/// `ch_dedup_spans_as_of_watermark` applies here too: exact only while the pre-watermark version has not been
/// merged away.
fn ch_membership_source(as_of_us: Option<i64>) -> (String, Option<i64>) {
    match as_of_us {
        Some(us) => (ch_dedup_spans_as_of_watermark().to_string(), Some(us)),
        None => ("otel_spans FINAL".to_string(), None),
    }
}

pub async fn get_trace_session_pairs(
    client: &Client,
    project_id: &str,
    trace_ids: &[String],
    as_of_us: Option<i64>,
) -> Result<Vec<(String, String)>, ClickhouseError> {
    if trace_ids.is_empty() {
        return Ok(vec![]);
    }

    let placeholders: Vec<&str> = trace_ids.iter().map(|_| "?").collect();
    let (source, watermark) = ch_membership_source(as_of_us);
    let sql = format!(
        // Two ClickHouse traps in one statement.
        //
        // `assumeNotNull` inside the aggregate, because an expression on a Nullable column stays Nullable
        // and the crate refuses to deserialise `Nullable(String)` into `String` - as in the sibling method.
        //
        // And the alias is `session`, not `session_id`: a SELECT alias is visible in WHERE in ClickHouse and
        // shadows the column, so `AS session_id` made the predicate read the aggregate and the query failed
        // outright with ILLEGAL_AGGREGATION - on ClickHouse only. The parity comparison caught it; nothing
        // else would have until a user ran the feed on ClickHouse.
        "SELECT trace_id, argMin(assumeNotNull(session_id), (timestamp_start, span_id)) AS session \
         FROM {source} \
         WHERE project_id = ? AND trace_id IN ({}) \
         AND session_id IS NOT NULL AND session_id != '' GROUP BY trace_id",
        placeholders.join(", ")
    );

    #[derive(Row, Deserialize)]
    struct PairRow {
        trace_id: String,
        session: String,
    }

    // The relation's own placeholder sits at the head of the FROM, so it binds before the project.
    let mut query = client.query(&sql);
    if let Some(us) = watermark {
        query = query.bind(us);
    }
    query = query.bind(project_id);
    for tid in trace_ids {
        query = query.bind(tid);
    }
    let rows: Vec<PairRow> = query.fetch_all().await?;

    let mut pairs: Vec<(String, String)> =
        rows.into_iter().map(|r| (r.trace_id, r.session)).collect();
    pairs.sort();
    Ok(pairs)
}

/// Get trace IDs for given session IDs
/// The distinct sessions the given traces belong to.
pub async fn get_session_ids_for_traces(
    client: &Client,
    project_id: &str,
    trace_ids: &[String],
    as_of_us: Option<i64>,
) -> Result<Vec<String>, ClickhouseError> {
    if trace_ids.is_empty() {
        return Ok(vec![]);
    }

    let placeholders: Vec<&str> = trace_ids.iter().map(|_| "?").collect();
    let (source, watermark) = ch_membership_source(as_of_us);
    let sql = format!(
        // `assumeNotNull`, because `session_id` is Nullable here and an expression on a Nullable column
        // stays Nullable - the crate then refuses to deserialise `Nullable(String)` into `String`. Safe
        // because the predicate has already excluded null; without it this method failed outright on
        // ClickHouse while working on DuckDB, which is the exact class the parity suite exists to catch.
        // The canonical session per trace - see the DuckDB twin: a trace has one session, the one on its
        // earliest span, and reporting every session any of its spans named made a trace belong to two.
        "SELECT DISTINCT canonical_session FROM ( \
           SELECT argMin(assumeNotNull(session_id), (timestamp_start, span_id)) AS canonical_session \
           FROM {source} \
           WHERE project_id = ? AND trace_id IN ({}) AND session_id IS NOT NULL AND session_id != '' \
           GROUP BY trace_id \
         )",
        placeholders.join(", ")
    );

    #[derive(Row, Deserialize)]
    struct SessionIdRow {
        canonical_session: String,
    }

    let mut query = client.query(&sql);
    if let Some(us) = watermark {
        query = query.bind(us);
    }
    query = query.bind(project_id);
    for tid in trace_ids {
        query = query.bind(tid);
    }
    let rows: Vec<SessionIdRow> = query.fetch_all().await?;

    let mut session_ids: Vec<String> = rows.into_iter().map(|r| r.canonical_session).collect();
    session_ids.sort();
    session_ids.dedup();
    Ok(session_ids)
}

pub async fn get_trace_ids_for_sessions(
    client: &Client,
    project_id: &str,
    session_ids: &[String],
    as_of_us: Option<i64>,
) -> Result<Vec<String>, ClickhouseError> {
    if session_ids.is_empty() {
        return Ok(vec![]);
    }

    let placeholders: Vec<&str> = session_ids.iter().map(|_| "?").collect();
    let in_clause = placeholders.join(", ");
    let (source, watermark) = ch_membership_source(as_of_us);

    let sql = format!(
        // The trace's **canonical** session, as the DuckDB twin does and as every read does. `session_id IN
        // (...)` asked whether *any* span of the trace named one of these sessions, so deleting session B
        // resolved to - and then deleted every span of - a trace the whole product displays under session A.
        // That is data loss, and this backend was missed when the reads were fixed because the regression
        // test covered DuckDB only.
        "SELECT trace_id FROM ( \
           SELECT trace_id, argMin(assumeNotNull(session_id), (timestamp_start, span_id)) \
             AS canonical_session \
           FROM {source} \
           WHERE project_id = ? AND session_id IS NOT NULL AND session_id != '' \
           GROUP BY trace_id \
         ) WHERE canonical_session IN ({})",
        in_clause
    );

    #[derive(Row, Deserialize)]
    struct TraceIdRow {
        trace_id: String,
    }

    let mut query = client.query(&sql);
    if let Some(us) = watermark {
        query = query.bind(us);
    }
    query = query.bind(project_id);
    for sid in session_ids {
        query = query.bind(sid);
    }

    let rows: Vec<TraceIdRow> = query.fetch_all().await?;

    Ok(rows.into_iter().map(|r| r.trace_id).collect())
}

/// Get span counts (events and links) in bulk
pub async fn get_span_counts_bulk(
    client: &Client,
    project_id: &str,
    spans: &[(String, String)],
) -> Result<
    std::collections::HashMap<(String, String), crate::data::types::SpanCounts>,
    ClickhouseError,
> {
    use crate::data::types::SpanCounts;
    use std::collections::HashMap;

    if spans.is_empty() {
        return Ok(HashMap::new());
    }

    let mut counts: HashMap<(String, String), SpanCounts> = HashMap::with_capacity(spans.len());

    // Build IN clause for (trace_id, span_id) pairs with parameterized placeholders
    let pairs: Vec<&str> = spans.iter().map(|_| "(?, ?)").collect();
    let in_clause = pairs.join(", ");

    let sql = format!(
        r#"SELECT
            trace_id,
            span_id,
            coalesce(JSONLength(raw_span, 'events'), 0) as event_count,
            coalesce(JSONLength(raw_span, 'links'), 0) as link_count
         FROM otel_spans FINAL
         WHERE project_id = ? AND (trace_id, span_id) IN ({})"#,
        in_clause
    );

    #[derive(Row, Deserialize)]
    struct CountRow {
        trace_id: String,
        span_id: String,
        event_count: u64,
        link_count: u64,
    }

    let mut query = client.query(&sql).bind(project_id);
    for (tid, sid) in spans {
        query = query.bind(tid).bind(sid);
    }

    let rows: Vec<CountRow> = query.fetch_all().await?;

    for row in rows {
        counts.insert(
            (row.trace_id, row.span_id),
            SpanCounts {
                event_count: row.event_count as i64,
                link_count: row.link_count as i64,
            },
        );
    }

    Ok(counts)
}

/// Appended to every `ALTER ... DELETE`, so a deletion has happened by the time it returns.
///
/// ClickHouse mutations are asynchronous by default: the statement schedules the work and returns. The
/// trace-deletion route then removed the files those spans referenced and answered 204 - while the spans
/// were still readable. For as long as the mutation took, a read returned spans whose content had already
/// been deleted, and a mutation that failed left them that way permanently.
///
/// `2` waits for every replica, not just the one that accepted the statement, because a read may be served
/// by any of them and "deleted" has to mean deleted everywhere. The cost is that a deletion blocks while a
/// replica catches up, which is the right trade for an operation a user asked for and expects to be able
/// to trust; the alternative is a 204 that means "scheduled".
const AWAIT_MUTATION: &str = " SETTINGS mutations_sync = 2";

/// Delete traces by IDs
///
/// In distributed mode, `table` should be the local table name (e.g., `otel_spans_local`)
/// and `on_cluster` should be the ON CLUSTER clause (e.g., ` ON CLUSTER cluster_name`).
pub async fn delete_traces(
    client: &Client,
    table: &str,
    on_cluster: &str,
    project_id: &str,
    trace_ids: &[String],
) -> Result<u64, ClickhouseError> {
    if trace_ids.is_empty() {
        return Ok(0);
    }

    let placeholders: Vec<&str> = trace_ids.iter().map(|_| "?").collect();
    let in_clause = placeholders.join(", ");

    // ClickHouse uses lightweight deletes (mutations)
    // In distributed mode, must use local table with ON CLUSTER
    let sql = format!(
        "ALTER TABLE {}{} DELETE WHERE project_id = ? AND trace_id IN ({}){}",
        table, on_cluster, in_clause, AWAIT_MUTATION
    );

    let mut query = client.query(&sql).bind(project_id);
    for tid in trace_ids {
        query = query.bind(tid);
    }
    query.execute().await?;

    // Return count - mutations are async in ClickHouse so we estimate
    Ok(trace_ids.len() as u64)
}

/// Delete spans by (trace_id, span_id) pairs
///
/// In distributed mode, `table` should be the local table name and
/// `on_cluster` should be the ON CLUSTER clause.
pub async fn delete_spans(
    client: &Client,
    table: &str,
    on_cluster: &str,
    project_id: &str,
    spans: &[(String, String)],
) -> Result<u64, ClickhouseError> {
    if spans.is_empty() {
        return Ok(0);
    }

    // Build IN clause for (trace_id, span_id) pairs with parameterized placeholders
    let pairs: Vec<&str> = spans.iter().map(|_| "(?, ?)").collect();
    let in_clause = pairs.join(", ");

    let sql = format!(
        "ALTER TABLE {}{} DELETE WHERE project_id = ? AND (trace_id, span_id) IN ({}){}",
        table, on_cluster, in_clause, AWAIT_MUTATION
    );

    let mut query = client.query(&sql).bind(project_id);
    for (tid, sid) in spans {
        query = query.bind(tid).bind(sid);
    }
    query.execute().await?;

    Ok(spans.len() as u64)
}

/// Delete sessions (all spans in the sessions)
///
/// In distributed mode, `table` should be the local table name and
/// `on_cluster` should be the ON CLUSTER clause.
pub async fn delete_sessions(
    client: &Client,
    table: &str,
    on_cluster: &str,
    project_id: &str,
    session_ids: &[String],
) -> Result<Vec<String>, ClickhouseError> {
    if session_ids.is_empty() {
        return Ok(vec![]);
    }

    // Resolve the sessions to their traces and delete those, exactly as the DuckDB backend does.
    //
    // Deleting the rows that *name* a session leaves the rest of its spans behind: a session id is
    // recorded on the spans that know it, often the root alone, so a root-only session lost its root
    // and kept its children - and the call returned success. The orphaned spans then show up as a
    // trace with no session and cannot be deleted by session again, because nothing names it any
    // more. Every read path already resolves a session through its traces; deletion has to agree.
    // No watermark: a deletion acts on what is stored *now*, not as of some past instant.
    let trace_ids = get_trace_ids_for_sessions(client, project_id, session_ids, None).await?;
    if trace_ids.is_empty() {
        return Ok(vec![]);
    }
    delete_traces(client, table, on_cluster, project_id, &trace_ids).await?;
    Ok(trace_ids)
}

/// Delete all data for a project
///
/// In distributed mode, `spans_table` and `metrics_table` should be local table names
/// and `on_cluster` should be the ON CLUSTER clause.
pub async fn delete_project_data(
    client: &Client,
    spans_table: &str,
    metrics_table: &str,
    on_cluster: &str,
    project_id: &str,
) -> Result<u64, ClickhouseError> {
    // Count rows first (approximate) - use distributed table for count
    let count_sql = "SELECT count() FROM otel_spans FINAL WHERE project_id = ?";
    let count: u64 = client.query(count_sql).bind(project_id).fetch_one().await?;

    // Delete all spans for the project (parameterized query for safety)
    // In distributed mode, must use local table with ON CLUSTER
    let sql = format!(
        "ALTER TABLE {}{} DELETE WHERE project_id = ?{}",
        spans_table, on_cluster, AWAIT_MUTATION
    );
    client.query(&sql).bind(project_id).execute().await?;

    // Metrics too, and a failure here is reported rather than swallowed.
    //
    // This used to log at debug and continue, on the reasoning that the table "may not exist in all
    // deployments" - but the schema in this repository always creates it, so the only thing that
    // rationale bought was hiding real failures. A project's metrics are not reachable through the
    // project row either, so metrics left behind by a swallowed error are the same class of orphan as
    // spans left behind, and the caller's verification would never look at them.
    //
    // The one case the old comment was right about is asked directly instead of inferred from an error:
    // if the table is genuinely absent there is nothing to delete.
    let table_exists: u64 = client
        .query("SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?")
        .bind(metrics_table)
        .fetch_one()
        .await?;
    if table_exists > 0 {
        let metrics_sql = format!(
            "ALTER TABLE {}{} DELETE WHERE project_id = ?{}",
            metrics_table, on_cluster, AWAIT_MUTATION
        );
        client
            .query(&metrics_sql)
            .bind(project_id)
            .execute()
            .await?;
    }

    Ok(count)
}

/// Count spans grouped by project for a set of project IDs.
/// Count every row a project still owns, spans and metrics together.
///
/// `FINAL` on both, because a `ReplacingMergeTree` may still hold superseded parts - and because this is
/// read to decide whether a deleted project's data is really gone, an approximate answer is the wrong
/// kind of answer. The metrics table is asked only if it exists, for the same reason its delete is.
pub async fn count_project_rows(
    client: &Client,
    metrics_table: &str,
    project_id: &str,
) -> Result<u64, ClickhouseError> {
    let spans: u64 = client
        .query("SELECT count() FROM otel_spans FINAL WHERE project_id = ?")
        .bind(project_id)
        .fetch_one()
        .await?;

    let table_exists: u64 = client
        .query("SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?")
        .bind(metrics_table)
        .fetch_one()
        .await?;
    let metrics: u64 = if table_exists > 0 {
        client
            .query("SELECT count() FROM otel_metrics FINAL WHERE project_id = ?")
            .bind(project_id)
            .fetch_one()
            .await?
    } else {
        0
    };
    Ok(spans + metrics)
}

pub async fn count_spans_by_project(
    client: &Client,
    project_ids: &[String],
) -> Result<std::collections::HashMap<String, u64>, ClickhouseError> {
    use std::collections::HashMap;

    if project_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let placeholders: Vec<&str> = project_ids.iter().map(|_| "?").collect();
    let sql = format!(
        "SELECT project_id, count() AS cnt FROM otel_spans FINAL WHERE project_id IN ({}) GROUP BY project_id",
        placeholders.join(", ")
    );

    #[derive(Row, Deserialize)]
    struct CountRow {
        project_id: String,
        cnt: u64,
    }

    let mut query = client.query(&sql);
    for pid in project_ids {
        query = query.bind(pid);
    }

    let rows: Vec<CountRow> = query.fetch_all().await?;

    let mut result = HashMap::new();
    for row in rows {
        result.insert(row.project_id, row.cnt);
    }

    Ok(result)
}

/// ClickHouse row for filter options
#[derive(Row, Deserialize)]
struct ChFilterOptionRow {
    value: Option<String>,
    count: u64,
}

/// Get trace filter options
pub async fn get_trace_filter_options(
    client: &Client,
    project_id: &str,
    columns: &[String],
    from_timestamp: Option<DateTime<Utc>>,
    to_timestamp: Option<DateTime<Utc>>,
) -> Result<
    std::collections::HashMap<String, Vec<crate::data::traits::FilterOptionRow>>,
    ClickhouseError,
> {
    use crate::data::traits::FilterOptionRow;
    use std::collections::HashMap;

    let mut results: HashMap<String, Vec<FilterOptionRow>> = HashMap::new();

    // Build time filter conditions with parameterized timestamps
    let mut time_conditions = String::new();
    let mut time_params: Vec<i64> = Vec::new();
    if let Some(from) = from_timestamp {
        time_conditions.push_str(" AND timestamp_start >= fromUnixTimestamp64Micro(?)");
        time_params.push(from.timestamp_micros());
    }
    if let Some(to) = to_timestamp {
        time_conditions.push_str(" AND timestamp_start <= fromUnixTimestamp64Micro(?)");
        time_params.push(to.timestamp_micros());
    }

    for column in columns {
        // Find the spans table column name
        let span_column = TRACE_FILTER_OPTION_COLUMNS
            .iter()
            .find(|(view_col, _)| *view_col == column.as_str())
            .map(|(_, span_col)| *span_col);

        let span_column = match span_column {
            Some(col) => col,
            None => continue,
        };

        // trace_name is computed per trace with the same fallback the trace list displays - root
        // span, else earliest named span. Listing root span names only offered names that did not
        // match the list and omitted the ones a trace with no root span shows, so filtering by a
        // name the UI had just displayed returned nothing. Mirrors the DuckDB copy.
        let sql = if column == "session_id" {
            // Only values the filter can match; see the DuckDB twin. The trace filter evaluates the
            // canonical session, so offering every session id any span named produced dropdown entries
            // that return nothing.
            format!(
                r#"
                -- `toNullable`: the canonical session comes through `assumeNotNull`, while the row
                -- type is `Option<String>`.
                SELECT toNullable(cts.canonical_session) as value, count(DISTINCT cts.trace_id) as count
                FROM ({CANONICAL}) cts
                WHERE (cts.project_id, cts.trace_id) IN (
                    SELECT project_id, trace_id FROM otel_spans FINAL
                    WHERE project_id = ?{time_cond}
                )
                GROUP BY cts.canonical_session
                ORDER BY count DESC
                LIMIT {limit}
                "#,
                CANONICAL = CANONICAL_TRACE_SESSIONS,
                time_cond = time_conditions,
                limit = QUERY_MAX_FILTER_SUGGESTIONS
            )
        } else if column == "trace_name" {
            format!(
                r#"
                SELECT value, count(DISTINCT trace_id) as count
                FROM (
                    SELECT trace_id,
                        coalesce(
                            argMinIf(span_name, timestamp_start,
                                     parent_span_id IS NULL AND span_name IS NOT NULL),
                            argMinIf(span_name, timestamp_start, span_name IS NOT NULL)
                        ) AS value
                    FROM otel_spans FINAL
                    WHERE project_id = ?{time_cond}
                    GROUP BY trace_id
                )
                WHERE value IS NOT NULL
                GROUP BY value
                ORDER BY count DESC
                LIMIT {limit}
                "#,
                time_cond = time_conditions,
                limit = QUERY_MAX_FILTER_SUGGESTIONS
            )
        } else {
            format!(
                r#"
                SELECT {col} as value, count(DISTINCT trace_id) as count
                FROM otel_spans FINAL
                WHERE project_id = ?{time_cond} AND {col} IS NOT NULL
                GROUP BY {col}
                ORDER BY count DESC
                LIMIT {limit}
                "#,
                col = span_column,
                time_cond = time_conditions,
                limit = QUERY_MAX_FILTER_SUGGESTIONS
            )
        };

        let mut query = client.query(&sql).bind(project_id);
        for ts in &time_params {
            query = query.bind(ts);
        }
        let rows: Vec<ChFilterOptionRow> = query.fetch_all().await?;

        let options: Vec<FilterOptionRow> = rows
            .into_iter()
            .filter_map(|r| {
                r.value.map(|v| FilterOptionRow {
                    value: v,
                    count: r.count,
                })
            })
            .collect();

        results.insert(column.clone(), options);
    }

    Ok(results)
}

/// Get trace tags options
pub async fn get_trace_tags_options(
    client: &Client,
    project_id: &str,
    from_timestamp: Option<DateTime<Utc>>,
    to_timestamp: Option<DateTime<Utc>>,
) -> Result<Vec<crate::data::traits::FilterOptionRow>, ClickhouseError> {
    use crate::data::traits::FilterOptionRow;

    // Build time filter conditions with parameterized timestamps
    let mut time_conditions = String::new();
    let mut time_params: Vec<i64> = Vec::new();
    if let Some(from) = from_timestamp {
        time_conditions.push_str(" AND timestamp_start >= fromUnixTimestamp64Micro(?)");
        time_params.push(from.timestamp_micros());
    }
    if let Some(to) = to_timestamp {
        time_conditions.push_str(" AND timestamp_start <= fromUnixTimestamp64Micro(?)");
        time_params.push(to.timestamp_micros());
    }

    // ClickHouse: extract tags from JSON array and count distinct traces.
    // toNullable because ChFilterOptionRow declares `Option<String>` (every other filter-option
    // query selects a Nullable column) and the clickhouse crate refuses to deserialize a
    // non-Nullable String into Option<T> - it failed at runtime, so the tag filter dropdown was
    // empty on this backend. ifNull rather than assumeNotNull: assuming a NULL is not null yields
    // undefined bytes, and JSONExtractArrayRaw('[]') already produces the empty array.
    let sql = format!(
        r#"
        SELECT
            toNullable(arrayJoin(JSONExtractArrayRaw(ifNull(tags, '[]')))) as value,
            count(DISTINCT trace_id) as count
        FROM otel_spans FINAL
        WHERE project_id = ?{time_cond} AND tags IS NOT NULL AND tags != '[]'
        GROUP BY value
        ORDER BY count DESC
        LIMIT {limit}
        "#,
        time_cond = time_conditions,
        limit = QUERY_MAX_FILTER_SUGGESTIONS
    );

    let mut query = client.query(&sql).bind(project_id);
    for ts in &time_params {
        query = query.bind(ts);
    }
    let rows: Vec<ChFilterOptionRow> = query.fetch_all().await?;

    // Decoded as JSON, not stripped of quotes. `JSONExtractArrayRaw` returns each element as raw
    // JSON, so a tag is `"alpha"` - but also `"say \"hi\""` and `"caf\u00e9"`. Trimming the outer
    // quotes left the escapes in, so such a tag was offered in the dropdown in its encoded form and
    // then matched nothing when selected, because the filter compares against the decoded value.
    let options: Vec<FilterOptionRow> = rows
        .into_iter()
        .filter_map(|r| {
            r.value.map(|v| FilterOptionRow {
                value: serde_json::from_str::<String>(&v)
                    .unwrap_or_else(|_| v.trim_matches('"').to_string()),
                count: r.count,
            })
        })
        .collect();

    Ok(options)
}

/// Get span filter options
pub async fn get_span_filter_options(
    client: &Client,
    project_id: &str,
    columns: &[String],
    from_timestamp: Option<DateTime<Utc>>,
    to_timestamp: Option<DateTime<Utc>>,
    observations_only: bool,
) -> Result<
    std::collections::HashMap<String, Vec<crate::data::traits::FilterOptionRow>>,
    ClickhouseError,
> {
    use crate::data::traits::FilterOptionRow;
    use std::collections::HashMap;

    let mut results: HashMap<String, Vec<FilterOptionRow>> = HashMap::new();

    // Build base conditions with parameterized timestamps
    let mut conditions = String::new();
    let mut time_params: Vec<i64> = Vec::new();
    if let Some(from) = from_timestamp {
        conditions.push_str(" AND timestamp_start >= fromUnixTimestamp64Micro(?)");
        time_params.push(from.timestamp_micros());
    }
    if let Some(to) = to_timestamp {
        conditions.push_str(" AND timestamp_start <= fromUnixTimestamp64Micro(?)");
        time_params.push(to.timestamp_micros());
    }
    if observations_only {
        conditions.push_str(&format!(" AND ({})", genai_span_predicate("")));
    }

    for column in columns {
        // Validate column is allowed
        if !SPAN_FILTER_OPTION_COLUMNS.contains(&column.as_str()) {
            continue;
        }

        let sql = format!(
            r#"
            SELECT {col} as value, count() as count
            FROM otel_spans FINAL
            WHERE project_id = ?{cond} AND {col} IS NOT NULL
            GROUP BY {col}
            ORDER BY count DESC
            LIMIT {limit}
            "#,
            col = column,
            cond = conditions,
            limit = QUERY_MAX_FILTER_SUGGESTIONS
        );

        let mut query = client.query(&sql).bind(project_id);
        for ts in &time_params {
            query = query.bind(ts);
        }
        let rows: Vec<ChFilterOptionRow> = query.fetch_all().await?;

        let options: Vec<FilterOptionRow> = rows
            .into_iter()
            .filter_map(|r| {
                r.value.map(|v| FilterOptionRow {
                    value: v,
                    count: r.count,
                })
            })
            .collect();

        results.insert(column.clone(), options);
    }

    Ok(results)
}

/// Get session filter options
pub async fn get_session_filter_options(
    client: &Client,
    project_id: &str,
    columns: &[String],
    from_timestamp: Option<DateTime<Utc>>,
    to_timestamp: Option<DateTime<Utc>>,
) -> Result<
    std::collections::HashMap<String, Vec<crate::data::traits::FilterOptionRow>>,
    ClickhouseError,
> {
    use crate::data::traits::FilterOptionRow;
    use std::collections::HashMap;

    let mut results: HashMap<String, Vec<FilterOptionRow>> = HashMap::new();

    // Build time conditions with parameterized timestamps
    let mut conditions = String::new();
    let mut time_params: Vec<i64> = Vec::new();
    if let Some(from) = from_timestamp {
        conditions.push_str(" AND timestamp_start >= fromUnixTimestamp64Micro(?)");
        time_params.push(from.timestamp_micros());
    }
    if let Some(to) = to_timestamp {
        conditions.push_str(" AND timestamp_start <= fromUnixTimestamp64Micro(?)");
        time_params.push(to.timestamp_micros());
    }

    for column in columns {
        // Validate column is allowed
        if !SESSION_FILTER_OPTION_COLUMNS.contains(&column.as_str()) {
            continue;
        }

        let sql = format!(
            r#"
            -- Counted over the trace's **canonical** session, so a suggestion cannot claim more sessions
            -- than the session list can show; see the DuckDB twin and `CANONICAL_TRACE_SESSIONS`. The
            -- filter stays in its own subquery so its unqualified columns cannot become ambiguous.
            SELECT s.{col} as value, count(DISTINCT cts.canonical_session) as count
            FROM (SELECT * FROM otel_spans FINAL
                  WHERE project_id = ?{cond} AND session_id IS NOT NULL AND {col} IS NOT NULL) s
            JOIN ({CANONICAL}) cts
              ON cts.project_id = s.project_id AND cts.trace_id = s.trace_id
            GROUP BY s.{col}
            ORDER BY count DESC
            LIMIT {limit}
            "#,
            col = column,
            cond = conditions,
            CANONICAL = CANONICAL_TRACE_SESSIONS,
            limit = QUERY_MAX_FILTER_SUGGESTIONS
        );

        let mut query = client.query(&sql).bind(project_id);
        for ts in &time_params {
            query = query.bind(ts);
        }
        let rows: Vec<ChFilterOptionRow> = query.fetch_all().await?;

        let options: Vec<FilterOptionRow> = rows
            .into_iter()
            .filter_map(|r| {
                r.value.map(|v| FilterOptionRow {
                    value: v,
                    count: r.count,
                })
            })
            .collect();

        results.insert(column.clone(), options);
    }

    Ok(results)
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
}
