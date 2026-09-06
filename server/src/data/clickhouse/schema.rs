//! ClickHouse schema definitions
//!
//! Supports both single-node and distributed cluster deployments:
//! - Single-node: Uses ReplacingMergeTree for deduplication
//! - Distributed: Uses ReplicatedReplacingMergeTree with Distributed routing
//!
//! Optimized for high-throughput SaaS workloads:
//! - Sharding by project_id for tenant isolation
//! - Efficient ORDER BY for time-range queries
//! - Bloom filter indices for ID lookups
//! - TTL for automatic data expiration
//! - Projections for common aggregations

use crate::core::config::ClickhouseConfig;

/// Current schema version
pub const SCHEMA_VERSION: i32 = 2;

/// The oldest schema version this build can migrate *from*.
///
/// The ClickHouse backend was introduced already at v2, so no released database exists below it and
/// there is nothing to migrate from v1. A database older than this has to be recreated.
pub const MIN_UPGRADABLE_FROM: i32 = 2;

/// One schema migration.
///
/// Every statement has `{on_cluster}` replaced with the ON CLUSTER clause - required for DDL in
/// distributed mode, empty otherwise - and `{local}` with the suffix that names the table holding the
/// data (`_local` in distributed mode, nothing in single-node mode).
///
/// A fresh database is created directly at [`SCHEMA_VERSION`] by the initial schema, so entries exist
/// solely for databases written by older builds. `migrations_cover_every_version` fails the build if a
/// version bump arrives without one, which is the guard that was missing: the mechanism compiled, had no
/// entries, and would have refused to start every existing database the moment the version moved.
pub struct Migration {
    pub version: i32,
    pub name: &'static str,
    /// A query returning one row when the migration still has work to do, and no rows when it is
    /// already applied. Checked before `statements` run.
    ///
    /// Not every schema change can be phrased idempotently. `ALTER TABLE ... ADD COLUMN, MODIFY ORDER
    /// BY` is the case in hand: ClickHouse permits a sorting key to be extended only by a column added
    /// in the *same* statement, so re-running it after it has succeeded is an error rather than a
    /// no-op - and a migration whose statements succeed while the version record fails would then leave
    /// a database that can never start again. A precondition makes retrying safe without requiring
    /// every statement to be individually idempotent, which is the property that is actually hard to
    /// keep by hand.
    pub precondition: Option<&'static str>,
    /// Applied in order, against the table that holds the data.
    pub statements: &'static [&'static str],
    /// Applied only in distributed mode, after `statements`.
    ///
    /// A `Distributed` table is created `AS otel_x_local`, which copies the structure once; it does not
    /// track later changes. So a column added to the local table has to be added to the distributed
    /// front end too, and in single-node mode there is no such table and nothing to do.
    pub distributed_statements: &'static [&'static str],
}

/// Empty: the ClickHouse backend was introduced at v2 and nothing above it was ever deployed, so the
/// initial DDL carries everything - the exemplars column and the span instrumentation scope included.
/// The runner and its coverage tests stay, because the first real migration is one entry away.
pub const MIGRATIONS: &[Migration] = &[];

/// Validate and return a cluster name safe for SQL interpolation.
///
/// ClickHouse cluster names may contain alphanumeric characters, underscores,
/// hyphens, and dots. This prevents SQL injection via the config file.
fn safe_cluster_name(config: &ClickhouseConfig) -> &str {
    let name = config.cluster.as_deref().unwrap_or("default");
    assert!(
        !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.'),
        "Invalid ClickHouse cluster name: {name:?}. Only alphanumeric, underscore, hyphen, and dot are allowed."
    );
    name
}

/// Generate schema version table
pub fn schema_version_table(config: &ClickhouseConfig) -> String {
    let engine = if config.distributed {
        format!(
            "ReplicatedReplacingMergeTree('/clickhouse/tables/{{shard}}/{db}/schema_version', '{{replica}}')",
            db = config.database
        )
    } else {
        "ReplacingMergeTree()".to_string()
    };

    let on_cluster = if config.distributed {
        format!(" ON CLUSTER {}", safe_cluster_name(config))
    } else {
        String::new()
    };

    format!(
        r#"
CREATE TABLE IF NOT EXISTS schema_version{on_cluster} (
    id UInt8,
    version Int32,
    applied_at Int64,
    description Nullable(String)
) ENGINE = {engine}
ORDER BY id
"#,
        on_cluster = on_cluster,
        engine = engine
    )
}

/// Generate OTEL spans local table (for distributed mode)
fn otel_spans_local_table(config: &ClickhouseConfig) -> String {
    let cluster = safe_cluster_name(config);
    let ttl_clause = "TTL timestamp_start + INTERVAL 90 DAY DELETE";

    format!(
        r#"
CREATE TABLE IF NOT EXISTS otel_spans_local ON CLUSTER {cluster} (
    -- IDENTITY
    project_id              LowCardinality(String),
    trace_id                String,
    span_id                 String,
    parent_span_id          Nullable(String),
    trace_state             Nullable(String),

    -- CONTEXT
    session_id              Nullable(String),
    user_id                 Nullable(String),
    environment             LowCardinality(Nullable(String)),

    -- SPAN METADATA
    span_name               Nullable(String),
    span_kind               LowCardinality(Nullable(String)),
    status_code             LowCardinality(Nullable(String)),
    status_message          Nullable(String),
    exception_type          Nullable(String),
    exception_message       Nullable(String),
    exception_stacktrace    Nullable(String),

    -- CLASSIFICATION
    span_category           LowCardinality(Nullable(String)),
    observation_type        LowCardinality(Nullable(String)),
    framework               LowCardinality(Nullable(String)),

    -- TIMING
    timestamp_start         DateTime64(6, 'UTC'),
    timestamp_end           Nullable(DateTime64(6, 'UTC')),
    duration_ms             Nullable(Int64),
    ingested_at             DateTime64(6, 'UTC') DEFAULT now64(6),

    -- GEN AI: PROVIDER & MODEL
    gen_ai_system               LowCardinality(Nullable(String)),
    gen_ai_operation_name       LowCardinality(Nullable(String)),
    gen_ai_request_model        LowCardinality(Nullable(String)),
    gen_ai_response_model       LowCardinality(Nullable(String)),
    gen_ai_response_id          Nullable(String),

    -- GEN AI: REQUEST PARAMETERS
    gen_ai_temperature          Nullable(Float64),
    gen_ai_top_p                Nullable(Float64),
    gen_ai_top_k                Nullable(Int64),
    gen_ai_max_tokens           Nullable(Int64),
    gen_ai_frequency_penalty    Nullable(Float64),
    gen_ai_presence_penalty     Nullable(Float64),
    gen_ai_stop_sequences       Nullable(String),

    -- GEN AI: RESPONSE METADATA
    gen_ai_finish_reasons       Nullable(String),

    -- GEN AI: AGENT & TOOL
    gen_ai_agent_id             Nullable(String),
    gen_ai_agent_name           Nullable(String),
    gen_ai_tool_name            Nullable(String),
    gen_ai_tool_call_id         Nullable(String),

    -- GEN AI: PERFORMANCE METRICS
    gen_ai_server_ttft_ms       Nullable(Int64),
    gen_ai_server_request_duration_ms Nullable(Int64),

    -- GEN AI: TOKEN USAGE
    gen_ai_usage_input_tokens       Int64 DEFAULT 0,
    gen_ai_usage_output_tokens      Int64 DEFAULT 0,
    gen_ai_usage_total_tokens       Int64 DEFAULT 0,
    gen_ai_usage_cache_read_tokens  Int64 DEFAULT 0,
    gen_ai_usage_cache_write_tokens Int64 DEFAULT 0,
    gen_ai_usage_reasoning_tokens   Int64 DEFAULT 0,
    gen_ai_usage_details            Nullable(String),

    -- GEN AI: COSTS (Decimal for precision)
    gen_ai_cost_input           Decimal64(6) DEFAULT 0,
    gen_ai_cost_output          Decimal64(6) DEFAULT 0,
    gen_ai_cost_cache_read      Decimal64(6) DEFAULT 0,
    gen_ai_cost_cache_write     Decimal64(6) DEFAULT 0,
    gen_ai_cost_reasoning       Decimal64(6) DEFAULT 0,
    gen_ai_cost_total           Decimal64(6) DEFAULT 0,

    -- HTTP
    http_method                 LowCardinality(Nullable(String)),
    http_url                    Nullable(String),
    http_status_code            Nullable(Int32),

    -- DATABASE
    db_system                   LowCardinality(Nullable(String)),
    db_name                     Nullable(String),
    db_operation                LowCardinality(Nullable(String)),
    db_statement                Nullable(String),

    -- STORAGE
    storage_system              LowCardinality(Nullable(String)),
    storage_bucket              Nullable(String),
    storage_object              Nullable(String),

    -- MESSAGING
    messaging_system            LowCardinality(Nullable(String)),
    messaging_destination       Nullable(String),

    -- USER-DEFINED DATA
    tags                        Nullable(String),
    metadata                    Nullable(String),
    input_preview               Nullable(String),
    output_preview              Nullable(String),

    -- RAW MESSAGES, TOOL DEFINITIONS
    messages                    String DEFAULT '[]',
    tool_definitions            String DEFAULT '[]',
    tool_names                  String DEFAULT '[]',

    -- RAW SPAN (compressed)
    raw_span                    Nullable(String) CODEC(ZSTD(3)),

    -- Instrumentation scope: the library that produced the span, versioned
    scope_name                  Nullable(String) CODEC(ZSTD(1)),
    scope_version               Nullable(String) CODEC(ZSTD(1)),

    -- INDICES for fast lookups
    INDEX idx_trace_id trace_id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_session_id session_id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_span_id span_id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_user_id user_id TYPE bloom_filter GRANULARITY 4,
    INDEX idx_gen_ai_system gen_ai_system TYPE bloom_filter GRANULARITY 4,
    INDEX idx_gen_ai_request_model gen_ai_request_model TYPE bloom_filter GRANULARITY 4,
    INDEX idx_observation_type observation_type TYPE set(0) GRANULARITY 4
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{shard}}/{db}/otel_spans', '{{replica}}', ingested_at)
PARTITION BY toYYYYMM(timestamp_start)
ORDER BY (project_id, toDate(timestamp_start), trace_id, span_id)
{ttl_clause}
SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 3600
"#,
        cluster = cluster,
        db = config.database,
        ttl_clause = ttl_clause
    )
}

/// Generate OTEL spans distributed table
fn otel_spans_distributed_table(config: &ClickhouseConfig) -> String {
    let cluster = safe_cluster_name(config);

    format!(
        r#"
CREATE TABLE IF NOT EXISTS otel_spans ON CLUSTER {cluster} AS otel_spans_local
ENGINE = Distributed('{cluster}', '{db}', 'otel_spans_local', sipHash64(project_id))
"#,
        cluster = cluster,
        db = config.database
    )
}

/// Generate OTEL spans table (single-node mode)
fn otel_spans_single_table() -> String {
    r#"
CREATE TABLE IF NOT EXISTS otel_spans (
    -- IDENTITY
    project_id              LowCardinality(String),
    trace_id                String,
    span_id                 String,
    parent_span_id          Nullable(String),
    trace_state             Nullable(String),

    -- CONTEXT
    session_id              Nullable(String),
    user_id                 Nullable(String),
    environment             LowCardinality(Nullable(String)),

    -- SPAN METADATA
    span_name               Nullable(String),
    span_kind               LowCardinality(Nullable(String)),
    status_code             LowCardinality(Nullable(String)),
    status_message          Nullable(String),
    exception_type          Nullable(String),
    exception_message       Nullable(String),
    exception_stacktrace    Nullable(String),

    -- CLASSIFICATION
    span_category           LowCardinality(Nullable(String)),
    observation_type        LowCardinality(Nullable(String)),
    framework               LowCardinality(Nullable(String)),

    -- TIMING
    timestamp_start         DateTime64(6, 'UTC'),
    timestamp_end           Nullable(DateTime64(6, 'UTC')),
    duration_ms             Nullable(Int64),
    ingested_at             DateTime64(6, 'UTC') DEFAULT now64(6),

    -- GEN AI: PROVIDER & MODEL
    gen_ai_system               LowCardinality(Nullable(String)),
    gen_ai_operation_name       LowCardinality(Nullable(String)),
    gen_ai_request_model        LowCardinality(Nullable(String)),
    gen_ai_response_model       LowCardinality(Nullable(String)),
    gen_ai_response_id          Nullable(String),

    -- GEN AI: REQUEST PARAMETERS
    gen_ai_temperature          Nullable(Float64),
    gen_ai_top_p                Nullable(Float64),
    gen_ai_top_k                Nullable(Int64),
    gen_ai_max_tokens           Nullable(Int64),
    gen_ai_frequency_penalty    Nullable(Float64),
    gen_ai_presence_penalty     Nullable(Float64),
    gen_ai_stop_sequences       Nullable(String),

    -- GEN AI: RESPONSE METADATA
    gen_ai_finish_reasons       Nullable(String),

    -- GEN AI: AGENT & TOOL
    gen_ai_agent_id             Nullable(String),
    gen_ai_agent_name           Nullable(String),
    gen_ai_tool_name            Nullable(String),
    gen_ai_tool_call_id         Nullable(String),

    -- GEN AI: PERFORMANCE METRICS
    gen_ai_server_ttft_ms       Nullable(Int64),
    gen_ai_server_request_duration_ms Nullable(Int64),

    -- GEN AI: TOKEN USAGE
    gen_ai_usage_input_tokens       Int64 DEFAULT 0,
    gen_ai_usage_output_tokens      Int64 DEFAULT 0,
    gen_ai_usage_total_tokens       Int64 DEFAULT 0,
    gen_ai_usage_cache_read_tokens  Int64 DEFAULT 0,
    gen_ai_usage_cache_write_tokens Int64 DEFAULT 0,
    gen_ai_usage_reasoning_tokens   Int64 DEFAULT 0,
    gen_ai_usage_details            Nullable(String),

    -- GEN AI: COSTS (Decimal for precision)
    gen_ai_cost_input           Decimal64(6) DEFAULT 0,
    gen_ai_cost_output          Decimal64(6) DEFAULT 0,
    gen_ai_cost_cache_read      Decimal64(6) DEFAULT 0,
    gen_ai_cost_cache_write     Decimal64(6) DEFAULT 0,
    gen_ai_cost_reasoning       Decimal64(6) DEFAULT 0,
    gen_ai_cost_total           Decimal64(6) DEFAULT 0,

    -- HTTP
    http_method                 LowCardinality(Nullable(String)),
    http_url                    Nullable(String),
    http_status_code            Nullable(Int32),

    -- DATABASE
    db_system                   LowCardinality(Nullable(String)),
    db_name                     Nullable(String),
    db_operation                LowCardinality(Nullable(String)),
    db_statement                Nullable(String),

    -- STORAGE
    storage_system              LowCardinality(Nullable(String)),
    storage_bucket              Nullable(String),
    storage_object              Nullable(String),

    -- MESSAGING
    messaging_system            LowCardinality(Nullable(String)),
    messaging_destination       Nullable(String),

    -- USER-DEFINED DATA
    tags                        Nullable(String),
    metadata                    Nullable(String),
    input_preview               Nullable(String),
    output_preview              Nullable(String),

    -- RAW MESSAGES, TOOL DEFINITIONS
    messages                    String DEFAULT '[]',
    tool_definitions            String DEFAULT '[]',
    tool_names                  String DEFAULT '[]',

    -- RAW SPAN (compressed)
    raw_span                    Nullable(String) CODEC(ZSTD(3)),

    -- Instrumentation scope: the library that produced the span, versioned
    scope_name                  Nullable(String) CODEC(ZSTD(1)),
    scope_version               Nullable(String) CODEC(ZSTD(1)),

    -- INDICES for fast lookups
    INDEX idx_trace_id trace_id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_session_id session_id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_span_id span_id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_user_id user_id TYPE bloom_filter GRANULARITY 4,
    INDEX idx_gen_ai_system gen_ai_system TYPE bloom_filter GRANULARITY 4,
    INDEX idx_gen_ai_request_model gen_ai_request_model TYPE bloom_filter GRANULARITY 4,
    INDEX idx_observation_type observation_type TYPE set(0) GRANULARITY 4
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(timestamp_start)
ORDER BY (project_id, toDate(timestamp_start), trace_id, span_id)
TTL timestamp_start + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 3600
"#
    .to_string()
}

/// Generate OTEL metrics local table (for distributed mode)
fn otel_metrics_local_table(config: &ClickhouseConfig) -> String {
    let cluster = safe_cluster_name(config);

    format!(
        r#"
CREATE TABLE IF NOT EXISTS otel_metrics_local ON CLUSTER {cluster} (
    -- IDENTITY
    project_id              LowCardinality(String),
    datapoint_id            String,
    metric_name             LowCardinality(String),
    metric_description      Nullable(String),
    metric_unit             LowCardinality(Nullable(String)),

    -- METRIC TYPE
    metric_type             LowCardinality(String),
    aggregation_temporality LowCardinality(Nullable(String)),
    is_monotonic            Nullable(UInt8),

    -- TIMING
    timestamp               DateTime64(6, 'UTC'),
    start_timestamp         Nullable(DateTime64(6, 'UTC')),

    -- VALUE
    value_int               Nullable(Int64),
    value_double            Nullable(Float64),

    -- HISTOGRAM
    histogram_count         Nullable(UInt64),
    histogram_sum           Nullable(Float64),
    histogram_min           Nullable(Float64),
    histogram_max           Nullable(Float64),
    histogram_bucket_counts Nullable(String),
    histogram_explicit_bounds Nullable(String),

    -- EXPONENTIAL HISTOGRAM
    exp_histogram_scale     Nullable(Int32),
    exp_histogram_zero_count Nullable(UInt64),
    exp_histogram_zero_threshold Nullable(Float64),
    exp_histogram_positive  Nullable(String),
    exp_histogram_negative  Nullable(String),

    -- SUMMARY
    summary_count           Nullable(UInt64),
    summary_sum             Nullable(Float64),
    summary_quantiles       Nullable(String),

    -- EXEMPLAR (the first one; the full set is in `exemplars` below)
    exemplar_trace_id       Nullable(String),
    exemplar_span_id        Nullable(String),
    exemplar_value_int      Nullable(Int64),
    exemplar_value_double   Nullable(Float64),
    exemplar_timestamp      Nullable(DateTime64(6, 'UTC')),
    exemplar_attributes     Nullable(String),

    -- CONTEXT
    session_id              Nullable(String),
    user_id                 Nullable(String),
    environment             LowCardinality(Nullable(String)),

    -- RESOURCE
    service_name            LowCardinality(Nullable(String)),
    service_version         Nullable(String),
    service_namespace       Nullable(String),
    service_instance_id     Nullable(String),

    -- INSTRUMENTATION SCOPE
    scope_name              Nullable(String),
    scope_version           Nullable(String),

    -- ATTRIBUTES
    attributes              Nullable(String),
    resource_attributes     Nullable(String),

    -- FLAGS & RAW
    flags                   Nullable(Int32),
    raw_metric              Nullable(String) CODEC(ZSTD(3)),

    -- SCOPE AND SCHEMA
    scope_attributes        Nullable(String),
    scope_schema_url        Nullable(String),
    resource_schema_url     Nullable(String),

    -- Every exemplar, not only the first. A histogram carries one per bucket, so the flat exemplar_*
    -- columns above hold one trace link out of however many the exporter sent.
    exemplars               Nullable(String) CODEC(ZSTD(3)),

    -- INDEXES
    INDEX idx_metric_name metric_name TYPE bloom_filter GRANULARITY 1,
    INDEX idx_session_id session_id TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{shard}}/{db}/otel_metrics', '{{replica}}')
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, metric_name, toDate(timestamp), timestamp, datapoint_id)
TTL timestamp + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192
"#,
        cluster = cluster,
        db = config.database
    )
}

/// Generate OTEL metrics distributed table
fn otel_metrics_distributed_table(config: &ClickhouseConfig) -> String {
    let cluster = safe_cluster_name(config);

    format!(
        r#"
CREATE TABLE IF NOT EXISTS otel_metrics ON CLUSTER {cluster} AS otel_metrics_local
ENGINE = Distributed('{cluster}', '{db}', 'otel_metrics_local', sipHash64(project_id))
"#,
        cluster = cluster,
        db = config.database
    )
}

/// Generate OTEL metrics table (single-node mode)
fn otel_metrics_single_table() -> String {
    r#"
CREATE TABLE IF NOT EXISTS otel_metrics (
    -- IDENTITY
    project_id              LowCardinality(String),
    datapoint_id            String,
    metric_name             LowCardinality(String),
    metric_description      Nullable(String),
    metric_unit             LowCardinality(Nullable(String)),

    -- METRIC TYPE
    metric_type             LowCardinality(String),
    aggregation_temporality LowCardinality(Nullable(String)),
    is_monotonic            Nullable(UInt8),

    -- TIMING
    timestamp               DateTime64(6, 'UTC'),
    start_timestamp         Nullable(DateTime64(6, 'UTC')),

    -- VALUE
    value_int               Nullable(Int64),
    value_double            Nullable(Float64),

    -- HISTOGRAM
    histogram_count         Nullable(UInt64),
    histogram_sum           Nullable(Float64),
    histogram_min           Nullable(Float64),
    histogram_max           Nullable(Float64),
    histogram_bucket_counts Nullable(String),
    histogram_explicit_bounds Nullable(String),

    -- EXPONENTIAL HISTOGRAM
    exp_histogram_scale     Nullable(Int32),
    exp_histogram_zero_count Nullable(UInt64),
    exp_histogram_zero_threshold Nullable(Float64),
    exp_histogram_positive  Nullable(String),
    exp_histogram_negative  Nullable(String),

    -- SUMMARY
    summary_count           Nullable(UInt64),
    summary_sum             Nullable(Float64),
    summary_quantiles       Nullable(String),

    -- EXEMPLAR (the first one; the full set is in `exemplars` below)
    exemplar_trace_id       Nullable(String),
    exemplar_span_id        Nullable(String),
    exemplar_value_int      Nullable(Int64),
    exemplar_value_double   Nullable(Float64),
    exemplar_timestamp      Nullable(DateTime64(6, 'UTC')),
    exemplar_attributes     Nullable(String),

    -- CONTEXT
    session_id              Nullable(String),
    user_id                 Nullable(String),
    environment             LowCardinality(Nullable(String)),

    -- RESOURCE
    service_name            LowCardinality(Nullable(String)),
    service_version         Nullable(String),
    service_namespace       Nullable(String),
    service_instance_id     Nullable(String),

    -- INSTRUMENTATION SCOPE
    scope_name              Nullable(String),
    scope_version           Nullable(String),

    -- ATTRIBUTES
    attributes              Nullable(String),
    resource_attributes     Nullable(String),

    -- FLAGS & RAW
    flags                   Nullable(Int32),
    raw_metric              Nullable(String) CODEC(ZSTD(3)),

    -- SCOPE AND SCHEMA
    scope_attributes        Nullable(String),
    scope_schema_url        Nullable(String),
    resource_schema_url     Nullable(String),

    -- Every exemplar, not only the first. A histogram carries one per bucket, so the flat exemplar_*
    -- columns above hold one trace link out of however many the exporter sent.
    exemplars               Nullable(String) CODEC(ZSTD(3)),

    -- INDEXES
    INDEX idx_metric_name metric_name TYPE bloom_filter GRANULARITY 1,
    INDEX idx_session_id session_id TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, metric_name, toDate(timestamp), timestamp, datapoint_id)
TTL timestamp + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192
"#
    .to_string()
}

/// Generate all schema statements for given config
pub fn generate_schema(config: &ClickhouseConfig) -> Vec<String> {
    let mut statements = Vec::new();

    // Schema version table
    statements.push(schema_version_table(config));

    if config.distributed {
        // Distributed mode: create local tables first, then distributed tables
        statements.push(otel_spans_local_table(config));
        statements.push(otel_spans_distributed_table(config));
        statements.push(otel_metrics_local_table(config));
        statements.push(otel_metrics_distributed_table(config));
    } else {
        // Single-node mode
        statements.push(otel_spans_single_table());
        statements.push(otel_metrics_single_table());
    }

    statements
}

/// The table to insert into: the `Distributed` front end, which is the only thing that shards.
///
/// This used to append `_local` in distributed mode, for throughput. It made sharding a fiction. The
/// distributed tables are declared `Distributed(cluster, db, table_local, sipHash64(project_id))`, so
/// which shard a row belongs on is a function of its project - but a write aimed at `_local` lands on
/// whichever node the connection happened to reach, and nothing corrects it afterwards.
///
/// Two failures follow, and both are silent. Behind a load balancer, one span delivered twice can land on
/// two different shards; `FINAL` deduplicates *within* a shard, so the read returns it twice and every
/// count is wrong. Behind a fixed endpoint the whole cluster's data goes to one node, which reads as a
/// mysteriously slow cluster rather than as a misconfiguration.
///
/// Reads already go through the distributed table and deletes are `_local` with `ON CLUSTER`, which is
/// correct - a mutation has to run where the parts are. Only the insert was wrong.
pub fn get_insert_table(_config: &ClickhouseConfig, base_name: &str) -> String {
    base_name.to_string()
}

/// Get the table name to query from (always the main table name)
pub fn get_query_table<'a>(_config: &ClickhouseConfig, base_name: &'a str) -> &'a str {
    base_name
}

/// Get the table name for DELETE operations (local table for distributed mode)
///
/// In distributed mode, DELETE mutations must be executed on local tables
/// because ALTER TABLE DELETE doesn't propagate through Distributed tables.
pub fn get_delete_table(config: &ClickhouseConfig, base_name: &str) -> String {
    if config.distributed {
        format!("{}_local", base_name)
    } else {
        base_name.to_string()
    }
}

/// Get the ON CLUSTER clause for DDL/mutation operations
///
/// In distributed mode, mutations need ON CLUSTER to execute on all nodes.
/// The suffix naming the table that holds the data: `_local` in distributed mode, nothing otherwise.
pub fn local_table_suffix(config: &ClickhouseConfig) -> &'static str {
    if config.distributed { "_local" } else { "" }
}

pub fn get_on_cluster_clause(config: &ClickhouseConfig) -> String {
    if config.distributed {
        format!(" ON CLUSTER {}", safe_cluster_name(config))
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> ClickhouseConfig {
        ClickhouseConfig {
            url: "http://localhost:8123".to_string(),
            database: "sideseat".to_string(),
            user: None,
            password: None,
            timeout_secs: 30,
            compression: true,
            async_insert: true,
            wait_for_async_insert: false,
            cluster: None,
            distributed: false,
            insert_quorum: 0,
        }
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_schema_version_is_positive() {
        assert!(SCHEMA_VERSION > 0);
    }

    #[test]
    fn test_generate_schema_single_node() {
        let config = default_config();
        let statements = generate_schema(&config);

        // Should have 3 tables: schema_version, otel_spans, otel_metrics
        assert_eq!(statements.len(), 3);

        // Should use ReplacingMergeTree (not Replicated)
        assert!(statements[1].contains("ReplacingMergeTree"));
        assert!(!statements[1].contains("ReplicatedReplacingMergeTree"));
        assert!(!statements[1].contains("ON CLUSTER"));
    }

    #[test]
    fn test_generate_schema_distributed() {
        let config = ClickhouseConfig {
            cluster: Some("test_cluster".to_string()),
            distributed: true,
            ..default_config()
        };
        let statements = generate_schema(&config);

        // Should have 5 tables: schema_version, otel_spans_local, otel_spans, otel_metrics_local, otel_metrics
        assert_eq!(statements.len(), 5);

        // Local tables should use ReplicatedReplacingMergeTree
        assert!(statements[1].contains("ReplicatedReplacingMergeTree"));
        assert!(statements[1].contains("ON CLUSTER"));

        // Distributed tables should use Distributed engine
        assert!(statements[2].contains("ENGINE = Distributed"));
    }

    #[test]
    fn test_get_insert_table_single_node() {
        let config = default_config();
        assert_eq!(get_insert_table(&config, "otel_spans"), "otel_spans");
    }

    #[test]
    fn test_get_insert_table_distributed() {
        let config = ClickhouseConfig {
            cluster: Some("test_cluster".to_string()),
            distributed: true,
            ..default_config()
        };
        // The distributed front end, not `_local`: it is what applies the sharding key, and a write
        // aimed past it lands on whichever node the connection reached.
        assert_eq!(get_insert_table(&config, "otel_spans"), "otel_spans");
    }

    #[test]
    fn test_schema_has_low_cardinality() {
        let config = default_config();
        let statements = generate_schema(&config);

        // Check that frequently queried columns use LowCardinality
        let spans_schema = &statements[1];
        assert!(spans_schema.contains("LowCardinality(String)"));
        assert!(spans_schema.contains("LowCardinality(Nullable(String))"));
    }

    #[test]
    fn test_schema_has_ttl() {
        let config = default_config();
        let statements = generate_schema(&config);

        let spans_schema = &statements[1];
        assert!(spans_schema.contains("TTL timestamp_start + INTERVAL"));
    }

    #[test]
    fn test_schema_has_indices() {
        let config = default_config();
        let statements = generate_schema(&config);

        let spans_schema = &statements[1];
        assert!(spans_schema.contains("INDEX idx_trace_id"));
        assert!(spans_schema.contains("INDEX idx_session_id"));
        assert!(spans_schema.contains("INDEX idx_span_id"));
        assert!(spans_schema.contains("bloom_filter"));
    }

    #[test]
    fn test_get_delete_table_single_node() {
        let config = default_config();
        assert_eq!(get_delete_table(&config, "otel_spans"), "otel_spans");
        assert_eq!(get_delete_table(&config, "otel_metrics"), "otel_metrics");
    }

    #[test]
    fn test_get_delete_table_distributed() {
        let config = ClickhouseConfig {
            cluster: Some("test_cluster".to_string()),
            distributed: true,
            ..default_config()
        };
        assert_eq!(get_delete_table(&config, "otel_spans"), "otel_spans_local");
        assert_eq!(
            get_delete_table(&config, "otel_metrics"),
            "otel_metrics_local"
        );
    }

    /// A version bump without a migration is a database that cannot start.
    ///
    /// `apply_versioned_migration` walks `(current+1)..=SCHEMA_VERSION` and fails on any version it
    /// does not know, so bumping the constant without adding an entry turns every existing database
    /// into a startup error. That failure belongs here, not at a user's first restart after upgrading.
    #[test]
    fn migrations_cover_every_version() {
        let first_upgradable = MIN_UPGRADABLE_FROM + 1;
        let current = SCHEMA_VERSION;
        for version in first_upgradable..=current {
            assert!(
                MIGRATIONS.iter().any(|m| m.version == version),
                "schema v{version} has no entry in MIGRATIONS: a database written by an older build \
                 would fail to start. Add the migration, or raise MIN_UPGRADABLE_FROM if v{version} \
                 was never released."
            );
        }
    }

    /// Entries must be ordered and unique, because they are applied in sequence.
    #[test]
    fn migrations_are_ordered_and_unique() {
        let versions: Vec<i32> = MIGRATIONS.iter().map(|m| m.version).collect();
        let mut sorted = versions.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            versions, sorted,
            "MIGRATIONS must be strictly ascending with no repeats"
        );
        for m in MIGRATIONS {
            let (version, name) = (m.version, m.name);
            assert!(
                version > MIN_UPGRADABLE_FROM,
                "migration v{version} ({name}) is at or below MIN_UPGRADABLE_FROM, so it can never run"
            );
            assert!(
                !m.statements.is_empty(),
                "migration v{version} ({name}) has no statements"
            );
            // A `Distributed` front end has no rows, so a statement aimed at it can only be DDL that
            // the local table also received - never the sorting-key change, which would be rejected.
            for statement in m.distributed_statements {
                assert!(
                    !statement.contains("{local}"),
                    "migration v{version} ({name}) aims a {{local}} statement at the distributed table"
                );
            }
        }
    }

    /// Every placeholder a migration uses is one the runner substitutes.
    ///
    /// A statement carrying an unknown `{...}` reaches ClickHouse verbatim and fails at a user's
    /// upgrade, which is the least useful moment to learn about a typo.
    #[test]
    fn migrations_use_only_known_placeholders() {
        const KNOWN: [&str; 2] = ["{on_cluster}", "{local}"];
        for m in MIGRATIONS {
            let all = m
                .statements
                .iter()
                .chain(m.distributed_statements.iter())
                .chain(m.precondition.iter());
            for statement in all {
                let mut rest = *statement;
                while let Some(start) = rest.find('{') {
                    let end = rest[start..]
                        .find('}')
                        .unwrap_or_else(|| panic!("migration v{} has an unclosed '{{'", m.version))
                        + start;
                    let placeholder = &rest[start..=end];
                    assert!(
                        KNOWN.contains(&placeholder),
                        "migration v{} uses unknown placeholder {placeholder}",
                        m.version
                    );
                    rest = &rest[end + 1..];
                }
            }
        }
    }

    #[test]
    fn test_get_on_cluster_clause_single_node() {
        let config = default_config();
        assert_eq!(get_on_cluster_clause(&config), "");
    }

    #[test]
    fn test_get_on_cluster_clause_distributed() {
        let config = ClickhouseConfig {
            cluster: Some("test_cluster".to_string()),
            distributed: true,
            ..default_config()
        };
        assert_eq!(get_on_cluster_clause(&config), " ON CLUSTER test_cluster");
    }

    #[test]
    fn test_get_on_cluster_clause_distributed_default_cluster() {
        let config = ClickhouseConfig {
            cluster: None,
            distributed: true,
            ..default_config()
        };
        assert_eq!(get_on_cluster_clause(&config), " ON CLUSTER default");
    }
}
