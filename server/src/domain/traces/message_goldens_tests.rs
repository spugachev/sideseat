//! Golden tests for message parsing, per framework and per sample.
//!
//! Message count, content, ordering and absence of duplicates are the properties users
//! actually see, and they are easy to break invisibly: a dedup identity tweak or a new
//! extractor can silently drop a tool result or duplicate a turn, and every existing unit
//! test still passes because they each cover one stage in isolation.
//!
//! This harness closes that gap end to end. Each fixture is the exact OTLP payload a real
//! sample sent (captured by `misc/record-otlp.py`, see `misc/capture-message-fixtures.sh`).
//! It is replayed through the real ingestion path — `extract_attributes_batch`,
//! `extract_messages_batch`, SideML conversion, enrichment — and then through each of the
//! four views the API exposes:
//!
//! | View    | Feed entry point                | API endpoint                     |
//! |---------|---------------------------------|----------------------------------|
//! | span    | `process_spans` (1 span)        | `/spans/{trace}/{span}/messages` |
//! | trace   | `process_spans` (1 trace)       | `/traces/{id}/messages`          |
//! | session | `process_spans` (whole session) | `/sessions/{id}/messages`        |
//! | feed    | `process_feed` (every row)      | `/feed/messages`                 |
//!
//! The first three use `process_spans` and differ only in their row set, so each must be built with
//! its own row set - using `process_feed` for a session tested an ordering no session request can
//! return. The feed is the fourth because it is the one view with its own pipeline entry point and
//! its own ordering, and while it was left out it was the only place a duplicate could still
//! surface unchecked. Pagination is not modelled: it is a property of the endpoint rather than of
//! parsing, so the feed view is the whole fixture as one page.
//!
//! The result is compared against a committed expectation file. Regenerate with:
//!
//! ```bash
//! UPDATE_GOLDENS=1 cargo test -p sideseat-server message_goldens
//! ```
//!
//! Regenerating is deliberately a separate, explicit step: a golden written straight from
//! current output enshrines whatever bugs exist today, so a regenerated file has to be read
//! before it is committed. The invariant checks below exist precisely because they hold
//! regardless of what the golden says — they catch bugs a blind snapshot would bless.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message as _;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::api::routes::otel::messages::scope_feed_to_trace;
use crate::data::types::MessageSpanRow;
use crate::domain::pricing::PricingService;
use crate::domain::sideml::feed::{
    FeedOptions, extract_tools_from_rows, legacy_and_neutral_order, presented_and_unconstrained,
    process_feed, process_spans, shadow_resolved_order,
};
use crate::domain::traces::extract::ExtractionMode;

// ============================================================================
// Fixture discovery
// ============================================================================

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/messages")
}

/// `(label, request paths)` for every captured sample, sorted for stable test output.
fn discover_fixtures() -> Vec<(String, Vec<PathBuf>)> {
    let root = fixture_root();
    let mut out: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    collect(&root, &root, &mut out);
    out.into_iter()
        .map(|(k, mut v)| {
            v.sort();
            (k, v)
        })
        .collect()
}

fn collect(root: &Path, dir: &Path, out: &mut BTreeMap<String, Vec<PathBuf>>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(root, &path, out);
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(is_captured_request)
        {
            let label = path
                .parent()
                .and_then(|p| p.strip_prefix(root).ok())
                .map(|p| p.to_string_lossy().replace('\\', "/"))
                .unwrap_or_default();
            out.entry(label).or_default().push(path);
        }
    }
}

// ============================================================================
// Replay: OTLP bytes -> MessageSpanRow
// ============================================================================

fn decode_request(path: &Path) -> ExportTraceServiceRequest {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    match path.extension().and_then(|e| e.to_str()) {
        Some("json") => serde_json::from_slice(&bytes)
            .unwrap_or_else(|e| panic!("decode JSON {}: {e}", path.display())),
        _ => ExportTraceServiceRequest::decode(bytes.as_slice())
            .unwrap_or_else(|e| panic!("decode protobuf {}: {e}", path.display())),
    }
}

/// Run the real ingestion path over every captured request, in capture order.
/// Returns `(span_name, row)`: `MessageSpanRow` carries no span name, but the golden keys
/// span views by name so a diff points at a recognisable span rather than a raw id.
/// Whether a file in a sample directory is a captured request the golden runner will replay.
///
/// One predicate, shared with `the_corpus_matches_the_support_matrix`. They had two: discovery required a
/// `req-` prefix while the matrix counted every `.pb`/`.json` that was not `expected.json`. So renaming
/// `req-001.pb` to `capture.pb` left the documented count unchanged while silently removing that request
/// from every golden check - the corpus would still claim to cover it.
fn is_captured_request(name: &str) -> bool {
    name.starts_with("req-") && (name.ends_with(".pb") || name.ends_with(".json"))
}

fn rows_for(paths: &[PathBuf]) -> Vec<(String, MessageSpanRow)> {
    let pricing = PricingService::init_for_test().expect("offline pricing service");
    let mut rows = Vec::new();
    for path in paths {
        let request = decode_request(path);
        rows.extend(super::normalize_for_test(&request, &pricing));
    }
    rows
}

// ============================================================================
// Comparable projection
// ============================================================================

/// One message as the golden records it.
///
/// Volatile fields (ids, timestamps, tokens, cost) are deliberately excluded: they change on
/// every capture and would make every golden a false failure. What is kept is exactly what the
/// user reads — order, role, kind and content — plus the fields the pipeline is expected to
/// derive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct GoldenMessage {
    /// Position in the returned feed. Recorded explicitly so a reordering fails loudly
    /// rather than silently shifting every line of the diff.
    index: usize,
    role: String,
    entry_type: String,
    /// Content, normalised: long text is truncated and whitespace collapsed so the golden
    /// stays reviewable, and model output wording differences do not dominate the diff.
    content: String,
    /// Digest of the FULL content block, so a change past the preview's cutoff — or one that
    /// survives whitespace collapse — still fails. The preview above is for reading; this is
    /// what actually guards the content.
    content_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    finish_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    observation_type: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct GoldenView {
    message_count: usize,
    /// Role sequence on its own line: the single most reviewable signal for ordering bugs.
    role_sequence: Vec<String>,
    tool_names: Vec<String>,
    tool_definition_count: usize,
    messages: Vec<GoldenMessage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Golden {
    label: String,
    /// Number of OTLP requests and spans the fixture carries, so a re-capture that lost
    /// data is obvious at the top of the diff.
    request_count: usize,
    span_count: usize,
    trace_count: usize,
    /// One entry per span, keyed `<trace_id_prefix>/<span_name>` (ids are unstable, so only
    /// a short prefix is kept for grouping).
    session_count: usize,
    span_views: BTreeMap<String, GoldenView>,
    trace_views: BTreeMap<String, GoldenView>,
    /// One per session, keyed by session id (or `trace:<id>` for a sessionless trace), because
    /// the endpoint serves one session at a time. Merging them all into a single view tested a
    /// request no client can make.
    session_views: BTreeMap<String, GoldenView>,
    /// The project feed over every row of the fixture, newest first.
    #[serde(default)]
    feed_view: GoldenView,
}

const MAX_CONTENT: usize = 240;

/// Stable digest of a full content block. Canonicalised through serde_json so key order does
/// not matter, then hashed - short enough to keep the golden readable, wide enough that a
/// real change cannot collide.
fn content_digest(value: &serde_json::Value) -> String {
    use std::hash::{Hash, Hasher};
    // Keys sorted before hashing. The workspace enables serde_json/preserve_order, so
    // to_string() keeps insertion order and two blocks that differ only in key order hashed
    // differently - a re-capture could churn every golden, and duplicate detection could miss a
    // genuine repeat that arrived with its keys in another order.
    let canonical = canonical_json(value);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    canonical.len().hash(&mut hasher);
    canonical.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Serialize with object keys in sorted order, recursively.
fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let inner: Vec<String> = keys
                .into_iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap_or_default(),
                        canonical_json(&map[k])
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        serde_json::Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", inner.join(","))
        }
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn normalise_content(value: &serde_json::Value) -> String {
    // Prefer the human-visible text; fall back to compact JSON for structural blocks.
    let raw = value
        .get("text")
        .and_then(|t| t.as_str())
        .map(str::to_owned)
        .or_else(|| {
            value
                .get("content")
                .and_then(|c| c.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| serde_json::to_string(value).unwrap_or_default());

    let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() > MAX_CONTENT {
        let head: String = collapsed.chars().take(MAX_CONTENT).collect();
        format!("{head}…[{} chars]", collapsed.chars().count())
    } else {
        collapsed
    }
}

/// Identity of a message for invariant purposes, including the trace it belongs to.
///
/// Not part of the golden: trace ids change on every capture. But invariants have to respect
/// conversation boundaries, so they need it. The same prompt in two different traces is two
/// legitimate messages, not a duplicate - samples that run a conversation twice (as
/// `strands/tool_use` does, once with a session id and once without) produce exactly that.
#[derive(Debug, Clone)]
struct InvariantRow {
    trace_id: String,
    span_id: String,
    index: usize,
    role: String,
    entry_type: String,
    content: String,
    /// Digest of the FULL content. Duplicate identity must not use the truncated, whitespace
    /// collapsed preview: two genuinely different long messages share a preview and would be
    /// reported as duplicates, while a whitespace-only difference would hide a real one.
    content_digest: String,
    /// Kept for diagnostics in assertion messages rather than for matching, which goes by id.
    #[allow(dead_code)]
    tool_name: Option<String>,
    /// Correlation id, so a result can be matched to the call it answers rather than merely
    /// counted against it.
    tool_use_id: Option<String>,
    /// The event or attribute this block was read from - what an extractor claims.
    carrier: String,
    /// Where the block sat in that carrier's payload, as a sortable string.
    position: String,
}

/// Which API endpoint a view reproduces. Every one of them calls `process_spans`; what
/// differs is the row set and the post-scoping, and getting that wrong means testing
/// something the API never does.
enum View<'a> {
    /// `/spans/{trace}/{span}/messages` - rows for one span.
    Span,
    /// `/sessions/{id}/messages` - rows for one session.
    Session,
    /// `/traces/{id}/messages` - when the trace belongs to a session the endpoint loads the
    /// WHOLE session so cross-trace prefix stripping can run, then scopes the result back to
    /// the requested trace. Processing the trace in isolation skips that stripping entirely.
    Trace {
        trace_id: &'a str,
        session_scoped: bool,
    },
    /// `/feed/messages` - the project feed, newest first, over every row the fixture holds.
    ///
    /// The one view that used `process_feed`, and the one the harness did not check. It is where a
    /// duplicate can still surface: it has its own ordering and, before the trace-complete
    /// reconstruction, its own answer to what collapses. A page is not modelled - pagination is a
    /// property of the endpoint, not of parsing - so this is the whole fixture as one page, which is
    /// what a page of a small project is.
    Feed,
}

fn build_view(rows: Vec<MessageSpanRow>, view: View<'_>) -> (GoldenView, Vec<InvariantRow>) {
    let options = FeedOptions::new();

    // All three endpoints call process_spans; `process_feed` belongs to the project feed
    // endpoint (routes/otel/feed.rs) and has different ordering semantics, so using it for
    // the session view tested behaviour no session request can produce.
    let result = match &view {
        View::Trace {
            trace_id,
            session_scoped: true,
        } => {
            let scoped_tools =
                extract_tools_from_rows(rows.iter().filter(|r| r.trace_id == **trace_id));
            let mut processed = process_spans(rows, &options);
            scope_feed_to_trace(&mut processed, scoped_tools, trace_id);
            processed
        }
        View::Feed => process_feed(rows, &options),
        _ => process_spans(rows, &options),
    };

    let mut messages = Vec::new();
    for (index, block) in result.messages.iter().enumerate() {
        let content_json = serde_json::to_value(&block.content).unwrap_or(json!(null));
        messages.push(GoldenMessage {
            index,
            role: block.role.as_str().to_string(),
            entry_type: block.entry_type.clone(),
            content: normalise_content(&content_json),
            content_digest: content_digest(&content_json),
            tool_name: content_json
                .get("name")
                .and_then(|n| n.as_str())
                .map(str::to_owned),
            finish_reason: block.finish_reason.as_ref().map(|f| format!("{f:?}")),
            observation_type: block.observation_type.clone(),
        });
    }

    let invariant_rows = result
        .messages
        .iter()
        .zip(messages.iter())
        .map(|(block, m)| InvariantRow {
            trace_id: block.trace_id.clone(),
            span_id: block.span_id.clone(),
            index: m.index,
            role: m.role.clone(),
            entry_type: m.entry_type.clone(),
            content: m.content.clone(),
            content_digest: m.content_digest.clone(),
            tool_name: m.tool_name.clone(),
            tool_use_id: block.tool_use_id.clone(),
            carrier: match (&block.event_name, &block.source_attribute) {
                (Some(event), _) => format!("event:{event}"),
                (None, Some(attribute)) => format!("attr:{attribute}"),
                (None, None) => "synthesised".to_string(),
            },
            position: block.position.to_string(),
        })
        .collect();

    (
        GoldenView {
            message_count: messages.len(),
            role_sequence: messages.iter().map(|m| m.role.clone()).collect(),
            tool_names: result.tool_names.clone(),
            tool_definition_count: result.tool_definitions.len(),
            messages,
        },
        invariant_rows,
    )
}

/// What a view is scoped to, carried explicitly.
///
/// Previously inferred by string-matching the display key, which broke the moment the key
/// became a canonical label instead of an id prefix - and a scope check that silently stops
/// checking is worse than none.
#[derive(Debug, Clone)]
enum Scope {
    Span {
        trace_id: String,
        span_id: String,
    },
    Trace {
        trace_id: String,
    },
    Session,
    /// The project feed: every trace in the project belongs, so scope constrains nothing - and the
    /// order is newest-first, which the answer check has to account for.
    Feed,
}

/// Golden plus the per-view invariant rows, which are checked but never serialized.
struct Built {
    golden: Golden,
    invariants: Vec<(String, Scope, Vec<InvariantRow>)>,
    /// session id -> its traces, and trace id -> canonical label, for the cross-view invariant.
    ///
    /// Session membership is a set, not a single value: a trace can belong to more than one
    /// session. Google ADK emits its own session id on some spans and the sample's `session.id` on
    /// others - which used to put one ADK trace under two sessions. A trace now belongs to the session
    /// on its earliest span, so each appears exactly once.
    traces_of_session: BTreeMap<String, BTreeSet<String>>,
    trace_labels: BTreeMap<String, String>,
}

/// The content filter every trace/session message query applies
/// (`MESSAGE_CONTENT_FILTER` in data/duckdb/repositories/messages.rs). Rows with no messages,
/// no tools and no error are never returned, so feeding them to the pipeline tests an input
/// the pipeline never sees. Including them made whole sessions come back empty.
fn passes_content_filter(row: &MessageSpanRow) -> bool {
    row.messages_json != "[]"
        || row.tool_definitions_json != "[]"
        || row.tool_names_json != "[]"
        || row.status_code.as_deref() == Some("ERROR")
}

/// `ORDER BY timestamp_start ASC`, as the query does. Capture order is not query order.
fn sorted_by_timestamp(mut rows: Vec<MessageSpanRow>) -> Vec<MessageSpanRow> {
    rows.sort_by(|a, b| {
        a.span_timestamp
            .cmp(&b.span_timestamp)
            .then_with(|| a.span_id.cmp(&b.span_id))
    });
    rows
}

/// Stable label for each trace: `trace-1`, `trace-2`, ... ordered by earliest span timestamp
/// then id.
///
/// The previous key was the first eight characters of the trace id, which silently collided:
/// these ids are time-ordered (UUIDv7-style), so traces created moments apart share a long
/// prefix. Seven fixtures lost trace views to `BTreeMap` overwrites - `openai/session` reported
/// trace_count 3 while comparing one. An index is also stable across re-captures, where a raw
/// id changes every time.
fn trace_labels(rows: &[(String, MessageSpanRow)]) -> BTreeMap<String, String> {
    let mut first_seen: BTreeMap<String, (chrono::DateTime<chrono::Utc>, String)> = BTreeMap::new();
    for (_, r) in rows {
        let e = first_seen
            .entry(r.trace_id.clone())
            .or_insert((r.span_timestamp, r.trace_id.clone()));
        if r.span_timestamp < e.0 {
            e.0 = r.span_timestamp;
        }
    }
    let mut ordered: Vec<(chrono::DateTime<chrono::Utc>, String)> =
        first_seen.into_values().collect();
    ordered.sort();
    ordered
        .into_iter()
        .enumerate()
        .map(|(i, (_, id))| (id, format!("trace-{}", i + 1)))
        .collect()
}

fn build_golden(label: &str, paths: &[PathBuf], rows: &[(String, MessageSpanRow)]) -> Built {
    let mut span_views = BTreeMap::new();
    let mut trace_views = BTreeMap::new();
    let mut session_views = BTreeMap::new();
    let mut invariants: Vec<(String, Scope, Vec<InvariantRow>)> = Vec::new();

    let labels = trace_labels(rows);
    let mut by_span: BTreeMap<(String, String), (String, Vec<MessageSpanRow>)> = BTreeMap::new();
    let mut by_trace: BTreeMap<String, Vec<MessageSpanRow>> = BTreeMap::new();
    // session id -> the traces that belong to it. The query is
    // `trace_id IN (SELECT trace_id WHERE session_id = ?)`, so membership is decided per
    // TRACE and then every row of those traces is returned - not only the rows that
    // themselves carry the session id. Filtering by each row's own session_id dropped the
    // rows holding the messages and made sessions look empty.
    let mut traces_of_session: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    // trace id -> (earliest timestamp carrying a session id, that session id)
    // Keyed by `(timestamp_start, span_id)`, so the earliest span is unambiguous - the same total order
    // production uses.
    #[allow(clippy::type_complexity)]
    let mut session_of_trace: BTreeMap<
        String,
        (
            (chrono::DateTime<chrono::Utc>, String),
            chrono::DateTime<chrono::Utc>,
            String,
        ),
    > = BTreeMap::new();

    for (span_name, row) in rows {
        by_span
            .entry((row.trace_id.clone(), row.span_id.clone()))
            .or_insert_with(|| (span_name.clone(), Vec::new()))
            .1
            .push(row.clone());
        by_trace
            .entry(row.trace_id.clone())
            .or_default()
            .push(row.clone());

        // Which session a trace belongs to, chosen the way production does: the session on its **earliest**
        // span, ordered by `(timestamp_start, span_id)` so the answer is total. `arg_min` over that key is
        // what every query uses - the display, the membership subquery, the lists and both deletions.
        if let Some(sid) = row.session_id.clone().filter(|s| !s.is_empty()) {
            let key = (row.span_timestamp, row.span_id.clone());
            let ingested = row.ingested_at;
            match session_of_trace.entry(row.trace_id.clone()) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert((key, ingested, sid));
                }
                std::collections::btree_map::Entry::Occupied(mut slot) => {
                    // Earliest span wins, and an exact tie is broken by the session id itself.
                    //
                    // That is deliberately **not** production's rule, which is "the latest delivery wins"
                    // (`DEDUP_SPANS` orders by `ingested_at DESC, rowid DESC`). Production cannot face this
                    // tie: it reads one row per span, so two different sessions never compete. This harness
                    // is handed *pre-deduplication* rows - the same span appears once per request that
                    // carried it - and it has no delivery order to consult, because `normalize_for_test`
                    // stamps `ingested_at` from the span's own start time. Three rules were tried: `<` keeps
                    // whichever row came first and `<=` whichever came last, both order-dependent, and
                    // `ingested_at` cannot separate them for the reason just given. Only a function of the
                    // *values* is order-independent here, which is what
                    // `the_order_spans_arrive_in_does_not_change_the_answer` requires.
                    if (&key, &sid) < (&slot.get().0, &slot.get().2) {
                        slot.insert((key, ingested, sid));
                    }
                }
            }
        }
    }

    // Membership follows from that, rather than being collected per row. Collecting each row's own session
    // gave a trace whose spans name two sessions a session view under **both** - which production does not
    // do, so the goldens described an answer no endpoint returns.
    for (trace_id, (_, _, session)) in &session_of_trace {
        traces_of_session
            .entry(session.clone())
            .or_default()
            .insert(trace_id.clone());
    }

    // Rows a session query would return: every row of every trace in the session, content
    // filtered, timestamp ordered.
    let session_rows = |sid: &str| -> Vec<MessageSpanRow> {
        let traces = traces_of_session.get(sid).cloned().unwrap_or_default();
        sorted_by_timestamp(
            rows.iter()
                .map(|(_, r)| r)
                .filter(|r| traces.contains(&r.trace_id) && passes_content_filter(r))
                .cloned()
                .collect(),
        )
    };

    for ((trace_id, span_id), (name, span_rows)) in &by_span {
        // `<trace-N>/<span name>/<span-M>`: the span index disambiguates repeats of the same
        // name and, like the trace label, survives a re-capture.
        //
        // Numbered by earliest timestamp, not by span-id order: ids change on every capture, so
        // id order made span-N shuffle between captures and produced diff noise unrelated to any
        // behaviour change.
        // Ties broken by parent then id: six same-name span groups in the current fixtures share
        // an identical start. Neither tiebreaker is capture-stable - both ids are regenerated
        // every capture - so a tied group can still renumber; what this buys is a *total* order,
        // which keeps numbering deterministic within a run so the same fixture always produces
        // the same labels. Renumbering a tied group is diff noise in a re-capture, not a test
        // failure, because the assertions compare the label set as a whole.
        let mut siblings: Vec<(chrono::DateTime<chrono::Utc>, String, &String)> = by_span
            .iter()
            .filter(|((t, _), _)| t == trace_id)
            .map(|((_, sp), (_, rows))| {
                let first = rows
                    .iter()
                    .map(|r| r.span_timestamp)
                    .min()
                    .unwrap_or_default();
                let parent = rows
                    .first()
                    .and_then(|r| r.parent_span_id.clone())
                    .unwrap_or_default();
                (first, parent, sp)
            })
            .collect();
        siblings.sort();
        let span_no = siblings
            .iter()
            .position(|(_, _, s)| *s == span_id)
            .unwrap_or(0)
            + 1;
        let key = format!(
            "{}/{name}/span-{span_no}",
            labels
                .get(trace_id)
                .map(String::as_str)
                .unwrap_or("trace-?")
        );
        // The span query filters by span_id alone and applies no content filter.
        let (view, inv) = build_view(sorted_by_timestamp(span_rows.clone()), View::Span);
        invariants.push((
            format!("span {key}"),
            Scope::Span {
                trace_id: trace_id.clone(),
                span_id: span_id.clone(),
            },
            inv,
        ));
        span_views.insert(key, view);
    }

    for (trace_id, trace_rows) in &by_trace {
        let key = labels
            .get(trace_id)
            .cloned()
            .unwrap_or_else(|| "trace-?".to_string());
        let session = session_of_trace
            .get(trace_id)
            .map(|(_, _, sid)| sid.clone());
        let (rows_for_view, session_scoped) = match &session {
            Some(sid) => (session_rows(sid), true),
            None => (
                sorted_by_timestamp(
                    trace_rows
                        .iter()
                        .filter(|r| passes_content_filter(r))
                        .cloned()
                        .collect(),
                ),
                false,
            ),
        };
        let (view, inv) = build_view(
            rows_for_view,
            View::Trace {
                trace_id,
                session_scoped,
            },
        );
        invariants.push((
            format!("trace {key}"),
            Scope::Trace {
                trace_id: trace_id.clone(),
            },
            inv,
        ));
        trace_views.insert(key, view);
    }

    for sid in traces_of_session.keys() {
        let (view, inv) = build_view(session_rows(sid), View::Session);
        invariants.push((format!("session {sid}"), Scope::Session, inv));
        session_views.insert(sid.clone(), view);
    }

    // The project feed over every row the fixture holds. The endpoint applies the same content
    // filter the trace and session queries do, so the row set is built the same way.
    let feed_rows = sorted_by_timestamp(
        by_span
            .values()
            .flat_map(|rows| rows.1.iter())
            .filter(|r| passes_content_filter(r))
            .cloned()
            .collect(),
    );
    let (feed_view, feed_inv) = build_view(feed_rows, View::Feed);
    invariants.push(("feed".to_string(), Scope::Feed, feed_inv));

    Built {
        golden: Golden {
            label: label.to_string(),
            request_count: paths.len(),
            span_count: by_span.len(),
            trace_count: by_trace.len(),
            session_count: traces_of_session.len(),
            span_views,
            trace_views,
            session_views,
            feed_view,
        },
        invariants,
        traces_of_session,
        trace_labels: labels,
    }
}

/// Duplicate detection, the property most at risk from dedup changes.
///
/// Partitioned by trace: identity is (role, entry_type, content) **within one trace**. Two
/// identical prompts in two different traces are two legitimate messages, not a duplicate -
/// several samples run their whole conversation twice, once with a session id and once
/// without, so a session view contains each prompt twice by design.
///
/// What it does *not* forbid, and this is deliberate: a genuine repeat that the telemetry
/// distinguishes. A provider's call id is part of a tool block's content, so two identical calls in
/// one response - `crewai/mcp_tools` retries one - have different digests and both belong here. What
/// remains forbidden is identical content with nothing to tell the copies apart, which is the shape a
/// history re-send takes. If the pipeline ever learns to keep id-less repeats, this check has to
/// learn it at the same time, or it will report the improvement as a defect.
fn assert_no_duplicates(label: &str, view_name: &str, rows: &[InvariantRow]) {
    let mut seen: HashMap<(&str, &str, &str, &str, &str), usize> = HashMap::new();
    for r in rows {
        // An exception is a fact about a *span*, and it is composed from that span's own
        // `exception_*` fields rather than read from a payload - so two spans reporting the same
        // failure are two failures, not a re-send. `openai-agents/image_gen` is the case: three
        // `generate_image` executions each failed with the same message, and collapsing them showed
        // three tool results beside one explanation.
        //
        // This is the amendment the check's own doc comment anticipated: the pipeline learned to keep a
        // repeat that has no id, so the invariant had to learn it in the same change, or it reports the
        // repair as the defect.
        let scope = if r.carrier == "attr:exception" {
            r.span_id.as_str()
        } else {
            ""
        };
        *seen
            .entry((
                r.trace_id.as_str(),
                scope,
                r.role.as_str(),
                r.entry_type.as_str(),
                r.content_digest.as_str(),
            ))
            .or_insert(0) += 1;
    }
    let mut dupes: Vec<String> = seen
        .iter()
        .filter(|(_, n)| **n > 1)
        .map(|((trace, _scope, role, kind, content), n)| {
            let head: String = content.chars().take(70).collect();
            format!(
                "{n}x in trace {} [{role}/{kind}] {head}",
                &trace[..trace.len().min(8)]
            )
        })
        .collect();
    dupes.sort();
    assert!(
        dupes.is_empty(),
        "{label} / {view_name}: duplicate messages within one trace:\n  {}",
        dupes.join("\n  ")
    );
}

/// A tool_use block's own call id: from the block field when set, otherwise from the id
/// embedded in the serialized content.
fn extract_tool_use_id(row: &InvariantRow) -> Option<&str> {
    row.tool_use_id.as_deref().or_else(|| {
        let start = row.content.find("\"id\":\"")? + 6;
        let rest = &row.content[start..];
        let end = rest.find('"')?;
        Some(&rest[..end])
    })
}

/// Tool calls and results must balance within a trace.
///
/// Counted per (trace, tool_use_id) rather than with a stack: a stack assumes results arrive
/// in call order, which is false for parallel tool calls — every framework here issues two at
/// once — and it also cannot tell whether a result belongs to the call it was popped against.
/// The earlier stack version additionally never checked that the stack drained, so
/// "every call is answered" was never actually verified.
///
/// Two things are asserted. A result whose id matches no call is always a defect. And a call
/// cannot be answered twice: two results carrying the same id mean the same invocation was
/// rendered twice.
///
/// Unanswered calls are NOT asserted - a cancelled or failed turn legitimately leaves one open,
/// and a time-filtered view can cut between the two halves.
/// Fixtures whose SOURCE telemetry cannot satisfy tool pairing, with the reason.
///
/// A capability limit of the framework, not a parsing defect, so it is recorded per fixture
/// rather than weakening the check for everyone. Verified by reading the raw payload: the id in
/// question appears only on `claude_code.tool.execution` / `claude_code.tool` spans as
/// `tool_use_id`, and the Claude Code CLI never emits a matching `tool_use` block for a
/// SUBAGENT's tool call — the subagent's assistant message is not part of the exported
/// conversation. The result is therefore genuinely callless upstream.
const PAIRING_EXEMPT: &[(&str, &str)] = &[
    (
        "claude-agent-sdk/subagents",
        "Claude Code CLI emits subagent tool executions without the matching tool_use block",
    ),
    (
        "claude-agent-sdk-js/subagents",
        "Claude Code CLI emits subagent tool executions without the matching tool_use block",
    ),
];

/// Fixtures whose source has no answer to show, with the reason.
///
/// Everything else is required to have one: a run that was asked something and completed must show
/// what it replied. CrewAI's answers were dropped for months because no invariant said so - its
/// reasoning fixture recorded `system -> user -> user -> user`, three questions and no answers, and
/// the goldens blessed it as correct.
const NO_ANSWER_EXPECTED: &[(&str, &str)] = &[(
    "strands/error",
    "the sample exists to fail, so the run never produced an answer",
)];

/// A conversation that asked something must show an answer.
///
/// The other invariants are all about *not* returning the wrong thing - scope, duplicates, pairing.
/// None of them notices content that never arrives, which is the failure mode of a broken extractor:
/// the feed looks orderly and is missing the reply.
///
/// `ordered` asks the stronger question - that the *last* turn was answered - and is false only for
/// the project feed, whose order is descending across responses and ascending within one, so no
/// position in it is the last turn.
fn assert_has_an_answer(label: &str, view_name: &str, rows: &[InvariantRow], ordered: bool) {
    if let Some((_, reason)) = NO_ANSWER_EXPECTED.iter().find(|(l, _)| *l == label) {
        eprintln!("message_goldens: {label}: answer check skipped - {reason}");
        return;
    }

    if !ordered {
        if !rows.iter().any(|r| r.role == "user") {
            return;
        }
        assert!(
            rows.iter()
                .any(|r| r.role == "assistant" || r.role == "tool"),
            "{label} / {view_name}: {} messages, a question among them, and nothing from the \
             assistant or a tool anywhere",
            rows.len()
        );
        return;
    }

    // Keyed on the *last* question, not on whether the view holds an answer anywhere. "Some
    // assistant message exists" is satisfied by an earlier turn's reply, so a view that answered
    // turn 1 and lost the answer to turn 2 passed - which is most of the CrewAI defect, since
    // every one of its runs kept the history and only ever dropped the current reply.
    let Some(last_question) = rows.iter().rposition(|r| r.role == "user") else {
        return;
    };
    let answered = rows[last_question + 1..]
        .iter()
        .any(|r| r.role == "assistant" || r.role == "tool");
    assert!(
        answered,
        "{label} / {view_name}: {} messages, the last of them a user message at index \
         {last_question} with nothing from the assistant or a tool after it - the reply to the \
         final turn is missing rather than merely out of order",
        rows.len()
    );
}

/// Invariant 5: a result follows the call it answers.
///
/// Causality, not adjacency - Vercel emits `call, call, result, result`, so requiring a result to come
/// *immediately* after its call would falsely accuse it. Nor does it apply to the project feed, which
/// is newest-first: there a call and the result of an earlier response are legitimately reversed, and
/// asserting otherwise accused `_synthetic/tool_use` the moment this check was added. The pair is matched by id within one trace,
/// exactly as `assert_tool_pairing` matches them.
///
/// A cross-span tie used to break this: an index restarts at zero in every span, so the tool span's
/// result could sort before the generation span's call. That is what `adopt_call_positions` settles,
/// and this is the property that says so.
fn assert_tool_causality(label: &str, view_name: &str, rows: &[InvariantRow]) {
    let mut call_at: HashMap<(&str, &str), usize> = HashMap::new();
    for (position, row) in rows.iter().enumerate() {
        if row.entry_type != "tool_use" {
            continue;
        }
        if let Some(id) = row.tool_use_id.as_deref().filter(|s| !s.is_empty()) {
            call_at
                .entry((row.trace_id.as_str(), id))
                .or_insert(position);
        }
    }

    for (position, row) in rows.iter().enumerate() {
        if row.entry_type != "tool_result" {
            continue;
        }
        let Some(id) = row.tool_use_id.as_deref().filter(|s| !s.is_empty()) else {
            continue;
        };
        // Only a *matched* pair constrains order. An unmatched result is the orphan case, which
        // `assert_tool_pairing` judges.
        let Some(&call_position) = call_at.get(&(row.trace_id.as_str(), id)) else {
            continue;
        };
        assert!(
            call_position < position,
            "{label} / {view_name}: the result of {id} is at index {position}, before its call at \
             {call_position} - an answer cannot precede its question"
        );
    }
}

/// Invariant 3: survivors from one carrier keep the order that carrier stated.
///
/// A carrier states order only where it *has* one: inside an array. Two entries of `messages` are
/// ordered by their indices, and a block from `system` versus one from `messages` is not - object
/// members have no order, and Anthropic's payload puts its system prompt in a sibling member of the
/// array, which the pipeline deliberately renders first. So the comparison applies to paths that
/// diverge at an index, and to nothing else.
///
/// Scoped per (trace, span, carrier): across carriers or spans, order comes from other evidence, and
/// demanding a global position order would falsely accuse every re-sent history.
fn assert_carrier_subsequence(label: &str, view_name: &str, rows: &[InvariantRow]) {
    /// One carrier of one span: (trace, span, carrier).
    type CarrierKey<'a> = (&'a str, &'a str, &'a str);
    /// A block of that carrier: (position path, index in the returned feed).
    type Placed<'a> = (&'a str, usize);

    let mut seen: HashMap<CarrierKey<'_>, Vec<Placed<'_>>> = HashMap::new();
    for (position, row) in rows.iter().enumerate() {
        // A synthesised block has no place in any payload, so there is no order to keep.
        if row.carrier == "synthesised" || row.position.is_empty() {
            continue;
        }
        seen.entry((
            row.trace_id.as_str(),
            row.span_id.as_str(),
            row.carrier.as_str(),
        ))
        .or_default()
        .push((row.position.as_str(), position));
    }

    for ((_, span_id, carrier), blocks) in seen {
        for (i, (earlier_path, earlier_index)) in blocks.iter().enumerate() {
            for (later_path, later_index) in blocks.iter().skip(i + 1) {
                let Some((earlier_sibling, later_sibling)) =
                    diverging_array_indices(earlier_path, later_path)
                else {
                    continue;
                };
                assert!(
                    earlier_sibling < later_sibling,
                    "{label} / {view_name}: carrier {carrier} of span {span_id} returned {later_path} \
                     at index {later_index} before {earlier_path} at index {earlier_index}, but the \
                     payload lists them the other way round"
                );
            }
        }
    }
}

/// The pair of array indices at which two position paths diverge, if they diverge at one.
///
/// `None` when they diverge at an object member - where the payload states no order - or when one path
/// is a prefix of the other, which is a parent and its child rather than two siblings.
fn diverging_array_indices(left: &str, right: &str) -> Option<(usize, usize)> {
    for (left_segment, right_segment) in left.split('.').zip(right.split('.')) {
        if left_segment == right_segment {
            continue;
        }
        let left_index = left_segment.parse::<usize>().ok()?;
        let right_index = right_segment.parse::<usize>().ok()?;
        return Some((left_index, right_index));
    }
    None
}

fn assert_tool_pairing(label: &str, view_name: &str, rows: &[InvariantRow]) {
    // Exempt fixtures skip only the "result must match a call" assertion, which their source
    // cannot satisfy. The duplicate-answer check below still applies: nothing about the Claude
    // CLI's subagent reporting makes it legitimate to render one invocation twice.
    let exempt = PAIRING_EXEMPT.iter().find(|(l, _)| *l == label);
    if let Some((_, reason)) = exempt {
        eprintln!("message_goldens: {label}: unmatched-result check skipped - {reason}");
    }
    let mut calls: BTreeMap<(&str, String), usize> = BTreeMap::new();
    for r in rows {
        if r.entry_type == "tool_use"
            && let Some(id) = extract_tool_use_id(r)
        {
            *calls
                .entry((r.trace_id.as_str(), id.to_string()))
                .or_insert(0) += 1;
        }
    }
    let mut answered: BTreeMap<(&str, String), usize> = BTreeMap::new();
    for r in rows {
        if r.entry_type != "tool_result" {
            continue;
        }
        let Some(id) = r.tool_use_id.as_deref() else {
            continue; // id-less results are matched by content by the pipeline
        };
        let key = (r.trace_id.as_str(), id.to_string());
        assert!(
            exempt.is_some() || calls.contains_key(&key),
            "{label} / {view_name}: tool_result at index {} has id {id:?} with no matching tool_use in trace {}",
            r.index,
            &r.trace_id[..r.trace_id.len().min(8)]
        );
        *answered.entry(key).or_insert(0) += 1;
    }
    for ((trace, id), n) in &answered {
        let Some(call_count) = calls.get(&(*trace, id.clone())) else {
            continue; // unmatched result: already handled (or exempted) above
        };
        assert!(
            n <= call_count,
            "{label} / {view_name}: tool_use id {id:?} in trace {} has {n} results but only {} call(s) - the same invocation is rendered more than once",
            &trace[..trace.len().min(8)],
            call_count
        );
    }
}

/// The projection itself must be self-consistent.
///
/// Deliberately narrow. Two earlier checks here were theatre: dense ascending indices cannot
/// fail because the projection assigns them with `enumerate()`, and "roles are one of four
/// known values" cannot fail because `ChatRole` has exactly four variants — an unmapped source
/// role has already become `User` by this point, silently, which is the defect those checks
/// looked like they were guarding. What remains are the fields that could genuinely disagree.
fn assert_projection_consistent(label: &str, view_name: &str, view: &GoldenView) {
    assert_eq!(
        view.role_sequence.len(),
        view.message_count,
        "{label} / {view_name}: role_sequence and message_count disagree"
    );
    assert_eq!(
        view.messages.len(),
        view.message_count,
        "{label} / {view_name}: message list and message_count disagree"
    );
    for m in &view.messages {
        assert!(
            !m.content_digest.is_empty(),
            "{label} / {view_name}: message {} has no content digest",
            m.index
        );
    }
}

/// Every returned block must belong to the scope that was requested.
///
/// This is the property the endpoints promise and the one a scoping bug breaks: a span view
/// leaking a sibling span's messages, or a trace view leaking another trace's, is invisible to
/// count and ordering checks because the totals still look plausible. Compared against exact
/// ids rather than a key prefix.
fn assert_scope(label: &str, view_name: &str, scope: &Scope, rows: &[InvariantRow]) {
    match scope {
        Scope::Span { trace_id, span_id } => {
            for r in rows {
                assert_eq!(
                    &r.trace_id, trace_id,
                    "{label} / {view_name}: block from another trace leaked into a span view"
                );
                assert_eq!(
                    &r.span_id, span_id,
                    "{label} / {view_name}: block from another span leaked into a span view"
                );
            }
        }
        Scope::Trace { trace_id } => {
            for r in rows {
                assert_eq!(
                    &r.trace_id, trace_id,
                    "{label} / {view_name}: block from another trace survived scope_feed_to_trace"
                );
            }
        }
        // A session legitimately spans traces, and the project feed spans everything, so there is
        // nothing to constrain in either.
        Scope::Session | Scope::Feed => {}
    }
}

/// A session's trace views must partition its session view exactly.
///
/// The trace endpoint loads the whole session, runs the same pipeline, then retains only the
/// requested trace's blocks; the session endpoint returns all of them. So summing the trace
/// views of one session must equal that session's view. This catches `scope_feed_to_trace`
/// dropping or duplicating blocks, and it is strictly stronger than what it replaced.
///
/// The previous check here - "a trace whose spans carry messages is not itself empty" - is
/// false. A trace can legitimately scope to nothing: `langgraph/rag_local` has 18 traces in one
/// session, 15 of which are pure cross-trace replays whose content is stripped as history and
/// only shown on the trace that first sent it.
fn assert_session_partitions_into_traces(
    label: &str,
    golden: &Golden,
    traces_of_session: &BTreeMap<String, BTreeSet<String>>,
    trace_labels: &BTreeMap<String, String>,
) {
    for (session_id, session_view) in &golden.session_views {
        let Some(traces) = traces_of_session.get(session_id) else {
            continue;
        };
        // A trace can carry two session ids (Google ADK emits its own plus the sample's).
        // Production builds that trace's view from whichever session it resolves to, so the
        // partition only holds for that session; skip the others rather than assert something
        // the API would not produce.
        // The partition is asserted for **every** session now. It used to `continue` for a session sharing
        // a trace with another - ADK emits its own session id alongside the sample's, so a trace appeared
        // under two - which skipped the check for exactly the fixtures where it mattered. A trace belongs to
        // one session, so that no longer happens.
        //
        // There is deliberately no assertion here that a session claims no foreign trace: this harness
        // *derives* `traces_of_session` from `session_of_trace`, so such a check compares a map with itself
        // and cannot fail. The production rule is proved where it is implemented - the DuckDB query tests and
        // the ClickHouse parity suite - not by a tautology here. What this loop does prove is that the
        // recorded trace views and the recorded session view agree, which are two independent numbers.
        let expected: usize = traces
            .iter()
            .filter_map(|trace_id| trace_labels.get(trace_id))
            .filter_map(|lbl| golden.trace_views.get(lbl))
            .map(|v| v.message_count)
            .sum();
        assert_eq!(
            expected, session_view.message_count,
            "{label} / session {session_id}: trace views sum to {expected} but the session view \
             has {} - scoping lost or duplicated blocks",
            session_view.message_count
        );
    }
}

/// Content must not be empty for text-bearing entries: an empty bubble in the UI is
/// indistinguishable from a parsing failure.
fn assert_no_empty_text(label: &str, view_name: &str, view: &GoldenView) {
    for m in &view.messages {
        if matches!(m.entry_type.as_str(), "text" | "thinking") {
            assert!(
                !m.content.trim().is_empty(),
                "{label} / {view_name}: empty {} block at index {}",
                m.entry_type,
                m.index
            );
        }
    }
}

fn check_invariants(label: &str, built: &Built) {
    let golden = &built.golden;
    let views: Vec<(String, &GoldenView)> = golden
        .span_views
        .iter()
        .map(|(k, v)| (format!("span {k}"), v))
        .chain(
            golden
                .trace_views
                .iter()
                .map(|(k, v)| (format!("trace {k}"), v)),
        )
        .chain(
            golden
                .session_views
                .iter()
                .map(|(k, v)| (format!("session {k}"), v)),
        )
        .chain(std::iter::once(("feed".to_string(), &golden.feed_view)))
        .collect();

    for (name, view) in &views {
        assert_projection_consistent(label, name, view);
        assert_no_empty_text(label, name, view);
    }

    for (name, scope, rows) in &built.invariants {
        assert_scope(label, name, scope, rows);
        assert_no_duplicates(label, name, rows);
        assert_carrier_subsequence(label, name, rows);
        // Causality applies to every chronological view, span views included. It reads only *matched*
        // pairs, so it is vacuous for the usual span holding one half - while a span that holds both,
        // as an ADK `call_llm` does, is exactly where an inversion hides: one such view listed both
        // tool results ahead of both calls and no invariant objected, because the blanket exemption
        // covered it. The project feed keeps its exemption: it descends across responses by contract,
        // so a call and an earlier response's result do appear reversed there.
        if !matches!(scope, Scope::Feed) {
            assert_tool_causality(label, name, rows);
        }
        // Pairing stays span-exempt: a single span usually holds only one half, so the *other* half's
        // id is legitimately absent and demanding it would accuse every ordinary span view.
        if !name.starts_with("span ") {
            assert_tool_pairing(label, name, rows);
            // Span views are excluded here too: one span legitimately holds only the request or
            // only the reply.
            //
            // The feed gets the weaker form. Its order is descending across responses and ascending
            // within one, so no single position is "the last turn" - reversing it does not give
            // chronological order either. What still holds, and would still have caught a whole
            // framework's answers going missing, is that a feed showing a question shows something
            // that answered one.
            let ordered = !matches!(scope, Scope::Feed);
            assert_has_an_answer(label, name, rows, ordered);
        }
    }

    assert_session_partitions_into_traces(
        label,
        golden,
        &built.traces_of_session,
        &built.trace_labels,
    );

    // Not every sample uses sessions, so assert on traces: those always exist.
    let total: usize = golden.trace_views.values().map(|v| v.message_count).sum();
    assert!(
        total > 0,
        "{label}: every trace view is empty - the sample produced no messages at all"
    );
}

fn golden_path(label: &str) -> PathBuf {
    fixture_root().join(label).join("expected.json")
}

#[test]
fn message_goldens() {
    let fixtures = discover_fixtures();
    if fixtures.is_empty() {
        // Not a silent pass: capturing fixtures needs credentials and a live model, so a
        // clean checkout legitimately has none. Say so loudly instead of reporting success.
        eprintln!(
            "message_goldens: no fixtures under {} - run misc/capture-message-fixtures.sh",
            fixture_root().display()
        );
        return;
    }

    let update = std::env::var("UPDATE_GOLDENS").is_ok();
    let mut failures: Vec<String> = Vec::new();
    let mut violations: Vec<String> = Vec::new();
    let mut checked = 0usize;

    for (label, paths) in &fixtures {
        let rows = rows_for(paths);
        let built = build_golden(label, paths, &rows);
        let golden = &built.golden;

        if update {
            // Report violations rather than aborting: the point of a record run is to see
            // the whole picture, including which fixtures are currently wrong. Aborting on
            // the first one hides the rest.
            if let Err(e) = std::panic::catch_unwind(|| check_invariants(label, &built)) {
                let msg = e
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| e.downcast_ref::<&str>().map(|s| (*s).to_string()))
                    .unwrap_or_else(|| "unknown".to_string());
                eprintln!("message_goldens: INVARIANT VIOLATION while recording {label}:\n  {msg}");
                violations.push(format!("{label}: {msg}"));
            }
        } else {
            check_invariants(label, &built);
        }

        let path = golden_path(label);
        if update {
            std::fs::create_dir_all(path.parent().unwrap()).ok();
            let json = serde_json::to_string_pretty(golden).unwrap();
            std::fs::write(&path, json + "\n").unwrap();
            eprintln!("message_goldens: wrote {}", path.display());
            continue;
        }

        let Ok(existing) = std::fs::read_to_string(&path) else {
            failures.push(format!(
                "{label}: no expectation file at {} (UPDATE_GOLDENS=1 to record)",
                path.display()
            ));
            continue;
        };
        let expected: Golden = match serde_json::from_str(&existing) {
            Ok(v) => v,
            Err(e) => {
                failures.push(format!("{label}: expectation file is not valid: {e}"));
                continue;
            }
        };
        checked += 1;

        if expected != *golden {
            failures.push(describe_diff(label, &expected, golden));
        }
    }

    if update {
        eprintln!("message_goldens: recorded {} fixture(s)", fixtures.len());
        // Recording still fails when an invariant was violated. Writing the files first is
        // deliberate - you want to see the whole picture - but exiting 0 would let a
        // known-bad expectation be committed as if it had been reviewed and accepted.
        assert!(
            violations.is_empty(),
            "recorded {} fixture(s) but {} violated an invariant; the written expectations \
             capture current (wrong) behaviour and must not be committed as-is:\n\n{}",
            fixtures.len(),
            violations.len(),
            violations.join("\n\n")
        );
        return;
    }

    assert!(
        failures.is_empty(),
        "message parsing changed for {} of {} fixture(s):\n\n{}",
        failures.len(),
        fixtures.len(),
        failures.join("\n\n")
    );
    eprintln!("message_goldens: {checked} fixture(s) matched");
}

/// Human-readable differences between one expected and one actual view.
fn compare_view(name: &str, e: &GoldenView, a: &GoldenView) -> Vec<String> {
    let mut out = Vec::new();
    if e.message_count != a.message_count {
        out.push(format!(
            "  {name}: message_count expected {}, got {}",
            e.message_count, a.message_count
        ));
    }
    if e.role_sequence != a.role_sequence {
        out.push(format!("  {name}: role sequence changed"));
        out.push(format!("    expected: {}", e.role_sequence.join(" -> ")));
        out.push(format!("    actual:   {}", a.role_sequence.join(" -> ")));
    }
    if e.tool_names != a.tool_names {
        out.push(format!(
            "  {name}: tool_names expected {:?}, got {:?}",
            e.tool_names, a.tool_names
        ));
    }
    for (i, (em, am)) in e.messages.iter().zip(a.messages.iter()).enumerate() {
        if em != am {
            out.push(format!("  {name}: message {i} changed"));
            if em.role != am.role || em.entry_type != am.entry_type {
                out.push(format!(
                    "    kind: expected {}/{}, got {}/{}",
                    em.role, em.entry_type, am.role, am.entry_type
                ));
            }
            if em.content != am.content {
                out.push(format!(
                    "    content expected: {}",
                    em.content.chars().take(100).collect::<String>()
                ));
                out.push(format!(
                    "    content actual:   {}",
                    am.content.chars().take(100).collect::<String>()
                ));
            }
            break; // one example per view is enough to diagnose
        }
    }
    out
}

/// Views that appeared or vanished between expectation and actual.
fn key_set_diff(
    kind: &str,
    expected: &BTreeMap<String, GoldenView>,
    actual: &BTreeMap<String, GoldenView>,
) -> Vec<String> {
    let ek: HashSet<&String> = expected.keys().collect();
    let ak: HashSet<&String> = actual.keys().collect();
    let mut gone: Vec<&String> = ek.difference(&ak).copied().collect();
    let mut added: Vec<&String> = ak.difference(&ek).copied().collect();
    gone.sort();
    added.sort();
    let mut out = Vec::new();
    if !gone.is_empty() {
        out.push(format!("  {kind} views missing: {gone:?}"));
    }
    if !added.is_empty() {
        out.push(format!("  {kind} views added: {added:?}"));
    }
    out
}

/// A readable summary rather than two pretty-printed blobs: the useful signal is almost
/// always a count or a role sequence, so lead with those.
fn describe_diff(label: &str, expected: &Golden, actual: &Golden) -> String {
    let mut out = vec![format!("{label}:")];

    if expected.span_count != actual.span_count {
        out.push(format!(
            "  span_count: expected {}, got {}",
            expected.span_count, actual.span_count
        ));
    }
    if expected.trace_count != actual.trace_count {
        out.push(format!(
            "  trace_count: expected {}, got {}",
            expected.trace_count, actual.trace_count
        ));
    }
    if expected.session_count != actual.session_count {
        out.push(format!(
            "  session_count: expected {}, got {}",
            expected.session_count, actual.session_count
        ));
    }

    if expected.request_count != actual.request_count {
        out.push(format!(
            "  request_count: expected {}, got {} (fixture re-captured?)",
            expected.request_count, actual.request_count
        ));
    }

    // Key-set changes were previously invisible, leaving only "differs in a field not
    // summarised above" - useless when a view appeared or vanished.
    out.extend(key_set_diff(
        "trace",
        &expected.trace_views,
        &actual.trace_views,
    ));
    out.extend(key_set_diff(
        "span",
        &expected.span_views,
        &actual.span_views,
    ));
    out.extend(key_set_diff(
        "session",
        &expected.session_views,
        &actual.session_views,
    ));

    for (key, e) in &expected.session_views {
        if let Some(a) = actual.session_views.get(key) {
            out.extend(compare_view(&format!("session {key}"), e, a));
        }
    }

    for (key, e) in &expected.trace_views {
        if let Some(a) = actual.trace_views.get(key) {
            out.extend(compare_view(&format!("trace {key}"), e, a));
        }
    }
    for (key, e) in &expected.span_views {
        if let Some(a) = actual.span_views.get(key) {
            out.extend(compare_view(&format!("span {key}"), e, a));
        }
    }

    // The feed view, which is not keyed - a change in it would otherwise print only "differs in a
    // field not summarised above", which is what this whole function exists to avoid.
    out.extend(compare_view("feed", &expected.feed_view, &actual.feed_view));

    if out.len() == 1 {
        out.push("  differs in a field not summarised above".to_string());
    }
    out.join("\n")
}

/// The harness is only meaningful if the invariant checks can actually fail. Every assertion
/// here also documents a case that a previous version of these checks got wrong.
#[test]
fn invariant_checks_are_not_vacuous() {
    fn row(trace: &str, index: usize, role: &str, kind: &str, content: &str) -> InvariantRow {
        InvariantRow {
            carrier: "attr:test".to_string(),
            position: index.to_string(),
            trace_id: trace.to_string(),
            span_id: "span-1".to_string(),
            index,
            role: role.to_string(),
            entry_type: kind.to_string(),
            content: content.to_string(),
            content_digest: format!("d:{content}"),
            tool_name: None,
            tool_use_id: None,
        }
    }

    fn tool_row(trace: &str, index: usize, kind: &str, id: &str) -> InvariantRow {
        InvariantRow {
            carrier: "attr:test".to_string(),
            position: index.to_string(),
            trace_id: trace.to_string(),
            span_id: "span-1".to_string(),
            index,
            role: if kind == "tool_use" {
                "assistant"
            } else {
                "tool"
            }
            .to_string(),
            entry_type: kind.to_string(),
            content: format!("{{\"id\":\"{id}\"}}"),
            content_digest: format!("d:{kind}:{id}"),
            tool_name: Some("calc".to_string()),
            tool_use_id: Some(id.to_string()),
        }
    }

    let fires = |f: &dyn Fn()| std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).is_err();

    // Same content twice in the SAME trace is a duplicate.
    let same_trace = vec![
        row("trace-a", 0, "user", "text", "Hello"),
        row("trace-a", 1, "user", "text", "Hello"),
    ];
    assert!(
        fires(&|| assert_no_duplicates("test", "synthetic", &same_trace)),
        "duplicate detection failed to fire for a within-trace duplicate"
    );

    // The same content in DIFFERENT traces is legitimate: a sample that runs the same
    // conversation twice produces exactly this, and flagging it was the original defect.
    let cross_trace = vec![
        row("trace-a", 0, "user", "text", "Hello"),
        row("trace-b", 1, "user", "text", "Hello"),
    ];
    assert!(
        !fires(&|| assert_no_duplicates("test", "synthetic", &cross_trace)),
        "the same content in two different traces must not be reported"
    );

    // A result whose id matches a call is fine, even when the call comes after it in the
    // list: matching is by id, not by position, because the session view interleaves.
    let matched = vec![
        tool_row("trace-a", 0, "tool_result", "id-1"),
        tool_row("trace-a", 1, "tool_use", "id-1"),
    ];
    assert!(
        !fires(&|| assert_tool_pairing("test", "synthetic", &matched)),
        "id-matched pairs must not be reported regardless of order"
    );

    // Parallel tool calls: two calls, two results, interleaved. A stack-based check reported
    // this as an error; matching by id must accept it.
    let parallel = vec![
        tool_row("trace-a", 0, "tool_use", "id-1"),
        tool_row("trace-a", 1, "tool_use", "id-2"),
        tool_row("trace-a", 2, "tool_result", "id-2"),
        tool_row("trace-a", 3, "tool_result", "id-1"),
    ];
    assert!(
        !fires(&|| assert_tool_pairing("test", "synthetic", &parallel)),
        "parallel tool calls must not be reported as mispaired"
    );

    // A result referencing an id no call emitted is a defect.
    let orphan = vec![
        tool_row("trace-a", 0, "tool_use", "id-1"),
        tool_row("trace-a", 1, "tool_result", "id-WRONG"),
    ];
    assert!(
        fires(&|| assert_tool_pairing("test", "synthetic", &orphan)),
        "a tool_result referencing an unknown call id must be reported"
    );

    // Pairing is per trace: a call in one trace must not answer a result in another.
    let cross_trace_pair = vec![
        tool_row("trace-a", 0, "tool_use", "id-1"),
        tool_row("trace-b", 1, "tool_result", "id-1"),
    ];
    assert!(
        fires(&|| assert_tool_pairing("test", "synthetic", &cross_trace_pair)),
        "pairing must be scoped per trace"
    );

    // Scope: a span view must not contain another span's or another trace's block.
    let leaked = vec![InvariantRow {
        carrier: "attr:test".to_string(),
        position: "0".to_string(),
        trace_id: "aaaaaaaa1111".to_string(),
        span_id: "bbbb2222".to_string(),
        index: 0,
        role: "user".to_string(),
        entry_type: "text".to_string(),
        content: "x".to_string(),
        content_digest: "d:x".to_string(),
        tool_name: None,
        tool_use_id: None,
    }];
    let in_scope = Scope::Span {
        trace_id: "aaaaaaaa1111".into(),
        span_id: "bbbb2222".into(),
    };
    let wrong_span = Scope::Span {
        trace_id: "aaaaaaaa1111".into(),
        span_id: "cccc3333".into(),
    };
    let wrong_trace = Scope::Trace {
        trace_id: "ffffffff9999".into(),
    };
    assert!(
        !fires(&|| assert_scope("test", "span x", &in_scope, &leaked)),
        "scope check must accept a block that is in scope"
    );
    assert!(
        fires(&|| assert_scope("test", "span x", &wrong_span, &leaked)),
        "scope check failed to catch a block from another span"
    );
    assert!(
        fires(&|| assert_scope("test", "trace x", &wrong_trace, &leaked)),
        "scope check failed to catch a block from another trace"
    );
    assert!(
        !fires(&|| assert_scope("test", "session x", &Scope::Session, &leaked)),
        "a session view spans traces, so nothing is constrained"
    );

    // Projection consistency: a count that disagrees with the list is a defect.
    let inconsistent = GoldenView {
        message_count: 5,
        role_sequence: vec!["user".into()],
        tool_names: vec![],
        tool_definition_count: 0,
        messages: vec![GoldenMessage {
            index: 0,
            role: "user".into(),
            entry_type: "text".into(),
            content: "a".into(),
            content_digest: "deadbeef".into(),
            tool_name: None,
            finish_reason: None,
            observation_type: None,
        }],
    };
    assert!(
        fires(&|| assert_projection_consistent("test", "synthetic", &inconsistent)),
        "projection consistency check failed to fire"
    );

    // A missing answer. The whole point of this check is a view that looks orderly, so the cases
    // it must fire on are all well-formed.
    let unanswered = vec![row("trace-a", 0, "user", "text", "the question")];
    assert!(
        fires(&|| assert_has_an_answer("test", "synthetic", &unanswered, true)),
        "a question with no reply at all must be reported"
    );

    // The case that matters, and that the first version of this check missed: an earlier turn was
    // answered, the last one was not. "Some assistant message exists" is true here.
    let last_turn_dropped = vec![
        row("trace-a", 0, "user", "text", "first question"),
        row("trace-a", 1, "assistant", "text", "first answer"),
        row("trace-a", 2, "user", "text", "second question"),
    ];
    assert!(
        fires(&|| assert_has_an_answer("test", "synthetic", &last_turn_dropped, true)),
        "an unanswered final turn must be reported even when an earlier turn was answered"
    );

    let answered = vec![
        row("trace-a", 0, "user", "text", "first question"),
        row("trace-a", 1, "assistant", "text", "first answer"),
        row("trace-a", 2, "user", "text", "second question"),
        row("trace-a", 3, "assistant", "text", "second answer"),
    ];
    assert!(
        !fires(&|| assert_has_an_answer("test", "synthetic", &answered, true)),
        "a complete conversation must pass"
    );

    // A tool result counts as an answer: the turn was acted on, and the reply may be in a span
    // this view does not contain.
    let answered_by_tool = vec![
        row("trace-a", 0, "user", "text", "the question"),
        row("trace-a", 1, "tool", "tool_result", "the result"),
    ];
    assert!(
        !fires(&|| assert_has_an_answer("test", "synthetic", &answered_by_tool, true)),
        "a tool result is an answer"
    );

    // Nothing was asked, so nothing is owed - a tool span's view holds no user message.
    let no_question = vec![row("trace-a", 0, "assistant", "tool_use", "call")];
    assert!(
        !fires(&|| assert_has_an_answer("test", "synthetic", &no_question, true)),
        "a view with no question must not be required to hold an answer"
    );

    // The exemption is by label, and must actually exempt - otherwise strands/error fails.
    assert!(
        !fires(&|| assert_has_an_answer("strands/error", "synthetic", &unanswered, true)),
        "an exempt fixture must skip the check"
    );

    // The feed's weaker branch: it cannot ask about the last turn, but it must still fire when a
    // question has no answer anywhere - the shape of a whole framework's replies going missing.
    assert!(
        fires(&|| assert_has_an_answer("test", "synthetic", &unanswered, false)),
        "the unordered form must still report a question with no answer at all"
    );
    // And it must accept what it cannot judge: an answer before its question is ordinary in a feed,
    // which descends across responses.
    let newest_first = vec![
        row("trace-a", 0, "assistant", "text", "second answer"),
        row("trace-a", 1, "user", "text", "second question"),
    ];
    assert!(
        !fires(&|| assert_has_an_answer("test", "synthetic", &newest_first, false)),
        "the unordered form must not require the answer to follow the question"
    );
    assert!(
        fires(&|| assert_has_an_answer("test", "synthetic", &newest_first, true)),
        "and the ordered form must still reject that same list, or the two forms are the same check"
    );
}

/// `passes_content_filter` reimplements the SQL predicate in Rust, so the two can drift: adding
/// a condition to the query would silently leave the harness feeding rows the API never
/// returns. This pins the coupling by checking the constant still mentions exactly the columns
/// the Rust version tests.
#[test]
fn content_filter_matches_the_sql_predicate() {
    use crate::data::types::MESSAGE_CONTENT_FILTER;

    // The exact predicate, not a substring or clause count: checking only that the column names
    // appear left an inverted operator (`=` for `!=`) or a changed literal ('ERROR' -> 'error')
    // passing while `passes_content_filter` kept the old meaning.
    const EXPECTED: &str = "(messages != '[]' OR tool_definitions != '[]' OR tool_names != '[]' OR status_code = 'ERROR')";
    assert_eq!(
        MESSAGE_CONTENT_FILTER, EXPECTED,
        "the SQL predicate changed; re-derive passes_content_filter from it, then update this \
         expectation in the same commit"
    );
}

/// Processing the same fixture twice must give the same answer.
///
/// Not hypothetical: the output sort's tie-break omitted content, so two blocks sharing span,
/// message index, entry index, entry type and role but differing in content were left in
/// HashMap order. Repeated runs over one real fixture disagreed on 3 then 4 views — two
/// identical API requests could return the same messages in a different order.
#[test]
fn processing_is_deterministic() {
    let fixtures = discover_fixtures();
    if fixtures.is_empty() {
        eprintln!("processing_is_deterministic: no fixtures - skipping");
        return;
    }
    // One fixture per suite rather than the first eight alphabetically, which covered only
    // _synthetic and early ADK samples and left every other framework's ordering untested.
    let mut per_suite: BTreeMap<&str, &(String, Vec<PathBuf>)> = BTreeMap::new();
    for f in &fixtures {
        let suite = f.0.split('/').next().unwrap_or("");
        per_suite.entry(suite).or_insert(f);
    }
    for (label, paths) in per_suite.into_values() {
        let first = build_golden(label, paths, &rows_for(paths)).golden;
        let second = build_golden(label, paths, &rows_for(paths)).golden;
        assert!(
            first == second,
            "{label}: two identical runs produced different output:\n{}",
            describe_diff(label, &first, &second)
        );
    }
}

/// Invariant 1: redundant evidence changes nothing.
///
/// A retried OTLP delivery is the same span twice. Reconstruction must reach the same answer from the
/// duplicate as from the original - not merely the same *count*, which the existing property test
/// checks, but the same messages in the same order.
///
/// This is one of the two properties my carrier-claiming and output-timing experiments violated
/// without any test noticing: both changed order globally, and the only detector was a 107-file diff.
#[test]
fn redundant_evidence_does_not_change_the_answer() {
    let fixtures = discover_fixtures();
    if fixtures.is_empty() {
        eprintln!("redundant_evidence: no fixtures - skipping");
        return;
    }
    let mut per_suite: BTreeMap<&str, &(String, Vec<PathBuf>)> = BTreeMap::new();
    for f in &fixtures {
        let suite = f.0.split('/').next().unwrap_or("");
        per_suite.entry(suite).or_insert(f);
    }

    for (label, paths) in per_suite.into_values() {
        let rows = rows_for(paths);
        let baseline = build_golden(label, paths, &rows).golden;

        // Re-deliver every span: the same rows again, as a retried export would arrive.
        let doubled: Vec<(String, MessageSpanRow)> =
            rows.iter().chain(rows.iter()).cloned().collect();
        let with_duplicates = build_golden(label, paths, &doubled).golden;

        assert!(
            baseline == with_duplicates,
            "{label}: re-delivering every span changed the answer:\n{}",
            describe_diff(label, &baseline, &with_duplicates)
        );
    }
}

/// Invariant 7: the order spans arrive in does not decide the answer.
///
/// Spans reach the pipeline in whatever order a query returned them, and a page of the feed can hold
/// them in a different order again. Anything the answer depends on has to come from the payloads, not
/// from that arrival order - otherwise two identical requests can disagree, and an extraction change
/// that merely shifts arrival order looks like a content change.
#[test]
fn the_order_spans_arrive_in_does_not_change_the_answer() {
    let fixtures = discover_fixtures();
    if fixtures.is_empty() {
        eprintln!("arrival_order: no fixtures - skipping");
        return;
    }
    let mut per_suite: BTreeMap<&str, &(String, Vec<PathBuf>)> = BTreeMap::new();
    for f in &fixtures {
        let suite = f.0.split('/').next().unwrap_or("");
        per_suite.entry(suite).or_insert(f);
    }

    for (label, paths) in per_suite.into_values() {
        let rows = rows_for(paths);
        let baseline = build_golden(label, paths, &rows).golden;

        // Reversed rather than randomly shuffled: deterministic, and the permutation most likely to
        // expose a rule that depends on first-seen order.
        let reversed: Vec<(String, MessageSpanRow)> = rows.iter().rev().cloned().collect();
        let from_reversed = build_golden(label, paths, &reversed).golden;

        assert!(
            baseline == from_reversed,
            "{label}: reversing the order spans arrived in changed the answer:\n{}",
            describe_diff(label, &baseline, &from_reversed)
        );
    }
}

/// Invariant 2, as a metamorphic test: reading a carrier nobody read only *adds*.
///
/// `ExtractionMode::PerCarrier` shares a span's attributes out per carrier instead of giving the whole
/// span to the first extractor that recognises anything. That is a strict increase in what is read, so
/// the answer must be a strict extension of today's: every message that was there before is still
/// there, in the same relative order, with the newly readable carriers' messages interleaved.
///
/// This is the property my first attempt at carrier claiming violated - it repaired the langgraph span
/// views and reordered the trace views - and the only detector at the time was a 107-file snapshot
/// diff. Here the failure names the fixture, the view, and the message that moved.
///
/// The fixtures listed in `REORDERS_UNDER_PER_CARRIER` are the known-bad set: they gain their missing
/// answer *and* reorder. They are named so the defect is in the suite rather than in my head, and each
/// one leaves the list when the ordering work lands.
#[test]
fn reading_more_carriers_only_adds_messages() {
    let fixtures = discover_fixtures();
    if fixtures.is_empty() {
        eprintln!("metamorphic: no fixtures - skipping");
        return;
    }

    let pricing = PricingService::init_for_test().expect("offline pricing service");
    let mut reordered: Vec<String> = Vec::new();
    let mut extended: Vec<String> = Vec::new();

    for (label, paths) in &fixtures {
        let baseline = rows_for_mode(&pricing, paths, ExtractionMode::FirstMatch);
        let per_carrier = rows_for_mode(&pricing, paths, ExtractionMode::PerCarrier);

        let before = build_golden(label, paths, &baseline).golden;
        let after = build_golden(label, paths, &per_carrier).golden;

        // Both views matter, and they answer different questions. A span view is where a carrier
        // nobody read shows up as a *gain* - the langgraph RunnableSequence spans that showed a
        // question and no answer. A trace view is where the same content already existed on another
        // span, so it collapses and only the *order* can change.
        let views = before
            .trace_views
            .iter()
            .map(|(k, v)| (format!("trace {k}"), v, after.trace_views.get(k)))
            .chain(
                before
                    .span_views
                    .iter()
                    .map(|(k, v)| (format!("span {k}"), v, after.span_views.get(k))),
            );

        for (name, before_view, after_view) in views {
            let Some(after_view) = after_view else {
                panic!("{label} / {name}: the view disappeared when more carriers were read");
            };
            if before_view.messages.len() < after_view.messages.len() {
                extended.push(format!("{label} / {name}"));
            }
            // Compared by message *identity*, not by role. A role sequence cannot tell two assistant
            // messages apart, so swapping them read as order-preserving - which is exactly the failure
            // this invariant exists to catch.
            let identities = |view: &GoldenView| -> Vec<String> {
                view.messages
                    .iter()
                    .map(|m| format!("{}/{}/{}", m.role, m.entry_type, m.content_digest))
                    .collect()
            };
            let before_ids = identities(before_view);
            let after_ids = identities(after_view);
            if !is_subsequence(&before_ids, &after_ids) {
                reordered.push(format!(
                    "{label} / {name}: {:?} is not preserved in {:?}",
                    before_view.role_sequence, after_view.role_sequence
                ));
            }
        }
    }

    let unexpected: Vec<&String> = reordered
        .iter()
        .filter(|r| {
            !REORDERS_UNDER_PER_CARRIER
                .iter()
                .any(|(fixture, _)| r.starts_with(fixture))
        })
        .collect();
    assert!(
        unexpected.is_empty(),
        "reading more carriers reordered messages that were already visible:\n  {}",
        unexpected
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );

    // The known-bad list must stay honest in both directions: an entry that no longer reorders has
    // been fixed and should be removed, or the test stops meaning anything.
    for (fixture, reason) in REORDERS_UNDER_PER_CARRIER {
        assert!(
            reordered.iter().any(|r| r.starts_with(fixture)),
            "{fixture} no longer reorders under PerCarrier ({reason}) - remove it from \
             REORDERS_UNDER_PER_CARRIER"
        );
    }

    assert!(
        !extended.is_empty(),
        "reading every carrier added nothing anywhere, so this test compares two identical runs"
    );
    eprintln!(
        "metamorphic: {} view(s) gained messages under PerCarrier, {} reorder (all known)",
        extended.len(),
        reordered.len()
    );
}

/// Fixtures whose already-visible messages *move* when more carriers are read.
///
/// Not an exemption for convenience: each is a defect with a diagnosis, kept in the suite so the
/// ordering work has an acceptance test.
/// Empty, and it must stay empty: reading more carriers may only *add* messages.
///
/// It held `langgraph/` while a `RunnableSequence` span's question and answer could not both be read
/// without shifting the response order. The generation-dataflow constraint removed every one of those
/// ten reorders, so the exemption is gone - and an entry appearing here again means an extraction
/// change is silently moving messages that were already visible, which is the failure mode this whole
/// redesign exists to prevent.
const REORDERS_UNDER_PER_CARRIER: &[(&str, &str)] = &[];

/// A system instruction is the frame a request was made in, so it precedes the request's first user
/// turn.
///
/// Nine of the eleven suites carrying a `tool_use` sample satisfy this, four of them with a
/// byte-identical role/kind shape. The two that do not are the two where the instruction and the
/// question arrive on *different* spans: a framework reports the instruction on the span that sent it -
/// the generation span - while the question came in on the orchestration span that started earlier, so
/// ordering by when the evidence arrived puts the frame after the question.
///
/// This is a **known gap**, not an exemption: the invariant's antecedent is true and the output is
/// wrong. Listing it bidirectionally is what keeps that honest - a new fixture cannot join the list
/// silently, and a fixture that starts passing must be removed. Fixing it needs an ordering *edge*
/// (`system` → the request's other inputs) rather than a key term; two scalar attempts are recorded in
/// `server/docs/ingestion-architecture.md`, one of which repaired no trace view at all.
///
/// Scoped to the *first* system and the *first* user message of a trace, which is deliberately weaker
/// than the real relation. The real one is per **request**, one generation invocation's input envelope,
/// and a trace legitimately holds several: `adk/image_gen` carries a second instruction at index 9 after
/// the first turn completed, and `adk/reasoning` repeats one instruction at indices 0, 4 and 8. Comparing
/// firsts is immune to all of that.
///
/// What it cannot see: a trace whose *first* request carries no instruction while a later one does
/// would be accused wrongly. No fixture has that shape - every one of the 27 violations is the
/// one-position displacement above - so the weaker form is sound against this corpus and would need
/// replacing by the per-request relation before it could be trusted against an arbitrary producer.
///
/// Also deliberately not keyed on `role == System`: `developer` normalises to `System`, and an in-band
/// system message inside an ordered array is a turn in that array rather than a frame. Comparing firsts
/// sidesteps that too, where a general rule would have to distinguish them.
/// 27 trace views across 22 fixtures, and every one of them is `system@1, user@0` - the frame is
/// displaced by exactly one position, never more. One cause, one fix.
const SYSTEM_FRAME_GAP: &[(&str, &str)] = &[
    (
        "langgraph/",
        "instruction on the generation span, question on the chain span that started earlier",
    ),
    (
        "claude-agent-sdk/",
        "user_system_prompt is reported on the generation span; the turn arrives on an earlier span",
    ),
    (
        "claude-agent-sdk-js/",
        "same as the Python SDK - the two agree exactly, including on this",
    ),
    (
        "strands/swarm",
        "the swarm's request arrives on its orchestrator span, the planner's prompt on the agent span",
    ),
];

/// Every trace view whose system instruction sorts after the first user turn.
fn system_frame_violations(built: &Built) -> Vec<String> {
    let mut out = Vec::new();
    for (name, scope, rows) in &built.invariants {
        if !matches!(scope, Scope::Trace { .. }) {
            continue;
        }
        let first_user = rows.iter().position(|r| r.role == "user");
        let first_system = rows.iter().position(|r| r.role == "system");
        if let (Some(user), Some(system)) = (first_user, first_system)
            && system > user
        {
            out.push(format!("{name}: system at {system}, first user at {user}"));
        }
    }
    out
}

/// The frame precedes the first turn, and the exceptions are named in both directions.
#[test]
fn a_system_instruction_precedes_the_first_user_turn() {
    let fixtures = discover_fixtures();
    if fixtures.is_empty() {
        eprintln!("system frame: no fixtures - skipping");
        return;
    }

    let pricing = PricingService::init_for_test().expect("offline pricing service");
    let mut violations: Vec<String> = Vec::new();
    for (label, paths) in &fixtures {
        let rows = rows_for_mode(&pricing, paths, ExtractionMode::PerCarrier);
        let built = build_golden(label, paths, &rows);
        for v in system_frame_violations(&built) {
            violations.push(format!("{label} / {v}"));
        }
    }

    let unexpected: Vec<&String> = violations
        .iter()
        .filter(|v| !SYSTEM_FRAME_GAP.iter().any(|(f, _)| v.starts_with(f)))
        .collect();
    assert!(
        unexpected.is_empty(),
        "a system instruction sorted after the first user turn in a fixture that is not a known gap \
         - a frame is not a turn:\n  {}",
        unexpected
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );

    // Both directions, or the list rots into a formality: an entry that now passes has been fixed by
    // the ordering work and must be removed.
    for (fixture, reason) in SYSTEM_FRAME_GAP {
        assert!(
            violations.iter().any(|v| v.starts_with(fixture)),
            "{fixture} now orders its system instruction first ({reason}) - remove it from \
             SYSTEM_FRAME_GAP"
        );
    }
}

/// Replay a fixture through the real ingestion path in a chosen extraction mode.
fn rows_for_mode(
    pricing: &PricingService,
    paths: &[PathBuf],
    mode: ExtractionMode,
) -> Vec<(String, MessageSpanRow)> {
    let mut rows = Vec::new();
    for path in paths {
        let request = decode_request(path);
        rows.extend(super::normalize_for_test_with_mode(&request, pricing, mode));
    }
    rows
}

/// Whether `needle` appears in `haystack` in order, with gaps allowed.
fn is_subsequence(needle: &[String], haystack: &[String]) -> bool {
    let mut haystack = haystack.iter();
    needle
        .iter()
        .all(|wanted| haystack.any(|candidate| candidate == wanted))
}

/// The content digest must actually distinguish content, including changes past the preview
/// cutoff and ones that survive whitespace collapse. Otherwise truncating the preview would
/// quietly become the real comparison.
#[test]
fn content_digest_detects_changes_beyond_the_preview() {
    // Same LENGTH, differing only past the preview cutoff. A length change is already
    // caught by the "[N chars]" suffix the preview carries; this is the case only a digest
    // can see.
    let long_a = "x".repeat(MAX_CONTENT + 50);
    let mut long_b = long_a.clone();
    long_b.replace_range(MAX_CONTENT + 10..MAX_CONTENT + 11, "y");

    let a = json!({"type": "text", "text": long_a});
    let b = json!({"type": "text", "text": long_b});
    assert_ne!(
        content_digest(&a),
        content_digest(&b),
        "digest must differ when content changes past the preview cutoff"
    );

    let preview_a = normalise_content(&a);
    let preview_b = normalise_content(&b);
    assert_eq!(
        preview_a, preview_b,
        "the preview is expected to be identical here - that is why the digest is needed"
    );

    // Key order must not matter, or every golden would churn and duplicate detection would miss
    // a repeat whose keys arrived in another order. These two objects are equal but written in
    // different orders, which serde_json/preserve_order keeps distinct in to_string().
    let o1 = json!({"type": "tool_use", "name": "calc", "id": "1"});
    let o2 = json!({"id": "1", "name": "calc", "type": "tool_use"});
    assert_ne!(
        serde_json::to_string(&o1).unwrap(),
        serde_json::to_string(&o2).unwrap(),
        "precondition: preserve_order keeps these textually different"
    );
    assert_eq!(
        content_digest(&o1),
        content_digest(&o2),
        "digest must be canonical, not insertion-ordered"
    );

    // Nested objects too.
    let n1 = json!({"a": {"x": 1, "y": 2}, "b": [{"p": 1, "q": 2}]});
    let n2 = json!({"b": [{"q": 2, "p": 1}], "a": {"y": 2, "x": 1}});
    assert_eq!(content_digest(&n1), content_digest(&n2));
}

// ============================================================================
// Shadow order resolver (increment 1 of the reconstruction redesign)
// ============================================================================

/// Roles/kinds of one trace's blocks, in shadow-resolver order.
fn shadow_order_of(label: &str) -> Vec<(String, String)> {
    let (_, paths) = discover_fixtures()
        .into_iter()
        .find(|(l, _)| l == label)
        .unwrap_or_else(|| panic!("fixture {label} not found"));
    let all: Vec<MessageSpanRow> = rows_for(&paths).into_iter().map(|(_, r)| r).collect();
    let first = all[0].trace_id.clone();
    let rows = sorted_by_timestamp(
        all.into_iter()
            .filter(|r| r.trace_id == first && passes_content_filter(r))
            .collect(),
    );
    shadow_resolved_order(rows)
        .into_iter()
        .map(|b| (b.role.as_str().to_string(), b.entry_type))
        .collect()
}

/// The case that motivated the redesign. On `strands-js/swarm` the intro text and the tool call it
/// introduces are one `gen_ai.choice` emission on the chat span, but dedup keeps the Planner span's
/// re-listed copy of the text, so the scalar sort key detaches the text and the trace view returns
/// `assistant/tool_use, tool/tool_result, assistant/text` — the intro trailing the result it
/// introduces.
///
/// The shadow resolver contracts the emission (so the text stays with its call) and adds the
/// call → result edge, yielding intro, then call, then result. This is the property increment 1
/// exists to prove before any view consumes the new order.
#[test]
fn shadow_resolver_keeps_intro_with_its_call_before_the_result() {
    let order = shadow_order_of("strands-js/swarm");

    let text = order
        .iter()
        .position(|(r, k)| r == "assistant" && k == "text")
        .expect("an assistant text block");
    let call = order
        .iter()
        .position(|(_, k)| k == "tool_use")
        .expect("a tool_use block");
    let result = order
        .iter()
        .position(|(_, k)| k == "tool_result")
        .expect("a tool_result block");

    assert!(
        text < call,
        "intro text (at {text}) must precede the call it introduces (at {call}); order was {order:?}"
    );
    assert!(
        call < result,
        "the call (at {call}) must precede its result (at {result}); order was {order:?}"
    );
}

/// The resolver is a permutation: it reorders survivors, it does not add, drop or alter them.
#[test]
fn shadow_resolver_is_a_permutation_of_the_survivors() {
    for label in ["strands-js/swarm", "strands/tool_use", "strands/mcp_tools"] {
        let (_, paths) = discover_fixtures()
            .into_iter()
            .find(|(l, _)| l == label)
            .unwrap_or_else(|| panic!("fixture {label} not found"));
        let all: Vec<MessageSpanRow> = rows_for(&paths).into_iter().map(|(_, r)| r).collect();
        let first = all[0].trace_id.clone();
        let rows = sorted_by_timestamp(
            all.into_iter()
                .filter(|r| r.trace_id == first && passes_content_filter(r))
                .collect(),
        );

        let baseline = process_spans(rows.clone(), &FeedOptions::new());
        let shadow = shadow_resolved_order(rows);

        assert_eq!(
            shadow.len(),
            baseline.messages.len(),
            "{label}: shadow order dropped or added blocks"
        );
        let mut a: Vec<String> = shadow.iter().map(|b| b.content_hash.clone()).collect();
        let mut b: Vec<String> = baseline
            .messages
            .iter()
            .map(|b| b.content_hash.clone())
            .collect();
        a.sort();
        b.sort();
        assert_eq!(
            a, b,
            "{label}: shadow order is not a permutation of the survivors"
        );
    }
}

/// The resolver returns every survivor exactly once, under the constraints production enforces.
///
/// Permutation was only ever checked for `NEUTRAL`, which is the configuration that provably cannot
/// move anything - so it could not have caught a resolver that drops or duplicates a block once a
/// class is promoted. Checked here against the real pipeline: the message *set* of a view must not
/// depend on the ordering constraints at all, because ordering is downstream of deduplication.
///
/// Companion to `ordering_constraints_do_not_change_a_session_s_messages`, which covers the session
/// path this one cannot.
///
/// Scope, deliberately stated: this covers the single-trace path only. Ordering and deduplication are
/// *not* independent across traces - `process_multi_trace_spans` strips a session's repeated prefix by
/// matching the previous trace's reconstructed sequence, so changing the order changes what is
/// stripped. That coupling is why promoting the dataflow class moved `adk/tool_use`'s session count
/// from 24 to 29, and it is an architectural problem rather than a test gap: a session's
/// deduplication should not be a function of a sibling trace's presentation order.
#[test]
fn promoted_constraints_do_not_change_which_messages_appear() {
    let mut checked = 0usize;
    for (label, paths) in discover_fixtures() {
        let all = rows_for(&paths);
        let mut by_trace: BTreeMap<String, Vec<MessageSpanRow>> = BTreeMap::new();
        for (_, row) in all {
            by_trace.entry(row.trace_id.clone()).or_default().push(row);
        }
        for (trace_id, trace_rows) in by_trace {
            let rows = sorted_by_timestamp(
                trace_rows
                    .into_iter()
                    .filter(passes_content_filter)
                    .collect(),
            );
            if rows.is_empty() {
                continue;
            }
            let (legacy, _) = legacy_and_neutral_order(rows.clone());
            let produced = process_spans(rows, &FeedOptions::new()).messages;
            let digest = |blocks: &[crate::domain::sideml::feed::BlockEntry]| -> Vec<String> {
                let mut out: Vec<String> = blocks
                    .iter()
                    .map(|b| format!("{}/{}/{}", b.role.as_str(), b.entry_type, b.content_hash))
                    .collect();
                out.sort();
                out
            };
            assert_eq!(
                digest(&produced),
                digest(&legacy),
                "{label} / trace {trace_id}: promoting an ordering constraint changed which \
                 messages appear, not just their order"
            );
            checked += 1;
        }
    }
    assert!(checked > 50, "only checked {checked} traces");
}

/// The resolver cannot move a block with every constraint class off.
///
/// With `Constraints::NEUTRAL` the resolver enforces only what the previous sort already satisfies -
/// every edge already forward, every contracted emission already contiguous, the legacy index as the
/// pop seed - so its output must be the previous order exactly, on every trace of every fixture. That
/// is the proof the machinery has no opinion of its own: whatever production's promoted classes then
/// change is attributable to those classes, not to the graph, the Kahn resolve or the cycle fallback.
///
/// Checked as a property here rather than left to the goldens, which are regenerable: a golden diff
/// would show a scaffold reorder as "expected output changed" and could be blessed by accident.
#[test]
fn the_neutral_resolver_reproduces_the_legacy_order() {
    let mut checked = 0usize;
    for (label, paths) in discover_fixtures() {
        let all = rows_for(&paths);
        let mut by_trace: BTreeMap<String, Vec<MessageSpanRow>> = BTreeMap::new();
        for (_, row) in all {
            by_trace.entry(row.trace_id.clone()).or_default().push(row);
        }
        for (trace_id, trace_rows) in by_trace {
            let rows = sorted_by_timestamp(
                trace_rows
                    .into_iter()
                    .filter(passes_content_filter)
                    .collect(),
            );
            if rows.is_empty() {
                continue;
            }
            let (legacy, neutral) = legacy_and_neutral_order(rows);
            // The whole block, serialised. A role/type/span/hash fingerprint is too weak: two
            // identical tool calls with distinct ids share it, so swapping them would have passed -
            // and those two calls are exactly what the resolver's contraction reasons about.
            let seq = |blocks: &[crate::domain::sideml::feed::BlockEntry]| -> Vec<String> {
                blocks
                    .iter()
                    .map(|b| serde_json::to_string(b).expect("a block serialises"))
                    .collect()
            };
            assert_eq!(
                seq(&neutral),
                seq(&legacy),
                "{label} / trace {trace_id}: the resolver moved a block with every class off, so the \
                 machinery is not neutral"
            );
            checked += 1;
        }
    }
    assert!(
        checked > 50,
        "expected the corpus to contribute many traces, only checked {checked}"
    );
}

/// Lineage must actually project the evidence, not merely avoid projecting it wrongly.
///
/// Two identical tool calls in one response share a `MessageIdentity` and both survive, keyed by their
/// rank. Recomputing identities after dedup could not tell them apart, so their evidence was left
/// unprojected - safe, but a gap, and `crewai/mcp_tools` is the corpus trace that has the pair. With a
/// lineage the pair projects, so the resolver sees both calls' emissions.
///
/// Asserted as "the resolver's answer still holds every survivor, and the two calls are both there":
/// projection is internal, so what is checked is the observable consequence.
#[test]
fn repeated_identical_calls_keep_both_and_stay_resolvable() {
    let label = "crewai/mcp_tools";
    let (_, paths) = discover_fixtures()
        .into_iter()
        .find(|(l, _)| l == label)
        .unwrap_or_else(|| panic!("fixture {label} not found"));
    let all: Vec<MessageSpanRow> = rows_for(&paths).into_iter().map(|(_, r)| r).collect();
    let first = all[0].trace_id.clone();
    let rows = sorted_by_timestamp(
        all.into_iter()
            .filter(|r| r.trace_id == first && passes_content_filter(r))
            .collect(),
    );

    let (legacy, scaffold) = legacy_and_neutral_order(rows.clone());
    let calls = |blocks: &[crate::domain::sideml::feed::BlockEntry]| -> usize {
        blocks.iter().filter(|b| b.entry_type == "tool_use").count()
    };
    assert!(
        calls(&legacy) >= 2,
        "this fixture is here because it has repeated identical calls, found {}",
        calls(&legacy)
    );
    assert_eq!(
        calls(&scaffold),
        calls(&legacy),
        "the resolver dropped or duplicated a call"
    );

    // And under FULL the answer is still every survivor, exactly once.
    let full = shadow_resolved_order(rows);
    assert_eq!(
        full.len(),
        legacy.len(),
        "FULL resolve is not a permutation for a trace with repeated identical calls"
    );
}

/// The feed is newest-first by *response*, and forward inside one.
///
/// The whole-view causality invariant is exempt for the feed, and correctly so: a call in one response
/// and the result in the next legitimately appear reversed there. But *within* a response the feed is
/// forward, and that is the property that lets the order resolver reach this view - the feed takes each
/// response's internal order from the reconstruction instead of re-deriving it from positions, so any
/// ordering the resolver improves shows up here too rather than only in the chronological views.
///
/// Checked as: a call and the result answering it, when they share a response, appear in that order.
#[test]
fn the_feed_keeps_each_response_forward() {
    let mut checked = 0usize;
    for (label, paths) in discover_fixtures() {
        let rows: Vec<MessageSpanRow> = rows_for(&paths).into_iter().map(|(_, r)| r).collect();
        if rows.is_empty() {
            continue;
        }
        let feed = process_feed(rows, &FeedOptions::new()).messages;

        // Where each call sits, per response.
        type Response = (String, chrono::DateTime<chrono::Utc>);
        let mut calls: HashMap<(Response, String), usize> = HashMap::new();
        for (index, block) in feed.iter().enumerate() {
            if block.entry_type != "tool_use" {
                continue;
            }
            if let Some(id) = block.tool_use_id.as_deref().filter(|s| !s.is_empty()) {
                let response = (block.trace_id.clone(), block.order_time);
                calls.entry((response, id.to_string())).or_insert(index);
            }
        }
        for (index, block) in feed.iter().enumerate() {
            if block.entry_type != "tool_result" {
                continue;
            }
            let Some(id) = block.tool_use_id.as_deref().filter(|s| !s.is_empty()) else {
                continue;
            };
            let response = (block.trace_id.clone(), block.order_time);
            let Some(&call_index) = calls.get(&(response, id.to_string())) else {
                continue; // the call is in another response - the feed may show that reversed
            };
            assert!(
                call_index < index,
                "{label}: within one response the feed put result {id} at {index} before its call \
                 at {call_index} - the response is not forward"
            );
            checked += 1;
        }
    }
    assert!(
        checked > 10,
        "expected the corpus to contain same-response call/result pairs, found {checked}"
    );
}

#[test]
#[ignore]
fn bench_pipeline() {
    let want = std::env::var("BENCH").unwrap_or_else(|_| "langgraph/swarm".to_string());
    let (_, paths) = discover_fixtures()
        .into_iter()
        .find(|(l, _)| *l == want)
        .unwrap_or_else(|| panic!("fixture {want} not found"));
    let rows: Vec<MessageSpanRow> = rows_for(&paths).into_iter().map(|(_, r)| r).collect();
    let iterations: u32 = std::env::var("ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);

    // The harness has to hand the pipeline owned rows, and cloning a fixture whose payloads are
    // base64 images costs more than the pipeline does - so it is measured and subtracted rather than
    // reported as pipeline time. Production reads its rows from the database and never pays it.
    let _ = process_spans(rows.clone(), &FeedOptions::new());
    let clone_start = std::time::Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(rows.clone());
    }
    let clone_cost = clone_start.elapsed() / iterations;

    let start = std::time::Instant::now();
    let mut blocks = 0usize;
    for _ in 0..iterations {
        blocks = process_spans(rows.clone(), &FeedOptions::new())
            .messages
            .len();
    }
    let elapsed = start.elapsed() / iterations;
    eprintln!(
        "BENCH {want}: {} rows -> {blocks} blocks, {:?} pipeline ({:?} measured - {:?} row clone),          {iterations} runs",
        rows.len(),
        elapsed.saturating_sub(clone_cost),
        elapsed,
        clone_cost
    );
    for (stage, took) in crate::domain::sideml::feed::stage_timings(rows) {
        eprintln!("  STAGE {stage}: {took:?}");
    }

    // The CPU half of ingestion: OTLP bytes through extraction, normalisation and enrichment. NOT
    // production ingestion - file extraction and every persistence step are outside this, so the
    // number is a floor rather than a measurement of what a request costs.
    let pricing = PricingService::init_for_test().expect("offline pricing service");
    let requests: Vec<_> = paths.iter().map(|p| decode_request(p)).collect();
    let bytes: usize = paths
        .iter()
        .map(|p| std::fs::metadata(p).map(|m| m.len() as usize).unwrap_or(0))
        .sum();
    let start = std::time::Instant::now();
    let mut produced = 0usize;
    for _ in 0..iterations {
        produced = requests
            .iter()
            .map(|r| super::normalize_for_test(r, &pricing).len())
            .sum();
    }
    let per_run = start.elapsed() / iterations;
    eprintln!(
        "  INGEST(cpu only) {want}: {produced} rows from {} KiB of OTLP, {per_run:?} per run",
        bytes / 1024
    );
}
/// No captured fixture exhausts the replay matcher's search budget.
///
/// The budget is a resource guard, and `FeedMetadata::replay_matching_complete` reports when it bites - but
/// a corpus that reaches it would mean real telemetry getting duplicated history, which is a different
/// matter from an adversarial shape doing so. Asserted over every fixture so that an extraction change
/// which widens a span's replayable set is caught here rather than by a user seeing a turn twice.
#[test]
fn no_fixture_exhausts_the_replay_matching_budget() {
    let mut checked = 0usize;
    for (label, paths) in discover_fixtures() {
        let rows: Vec<MessageSpanRow> = rows_for(&paths)
            .into_iter()
            .map(|(_, r)| r)
            .filter(passes_content_filter)
            .collect();
        if rows.is_empty() {
            continue;
        }
        let result = crate::domain::sideml::feed::process_spans(
            sorted_by_timestamp(rows),
            &FeedOptions::new(),
        );
        assert!(
            result.metadata.replay_matching_complete,
            "{label}: replay matching hit its budget, so this fixture's history may be shown twice"
        );
        checked += 1;
    }
    assert!(checked > 80, "only checked {checked} fixtures");
}

/// The corpus matches the support matrix, parsed from the document that carries it.
///
/// "Correct for all frameworks" is an open-world claim unless the set is written down, so it is written
/// down - and a table in a document drifts the moment someone adds a suite. A second hard-coded table in
/// this file would have drifted with it, which is what this test used to be: it compared the corpus against
/// its own copy and left the document free to be wrong. It reads the document now, so the claim "the table
/// and the corpus agree" is the thing actually checked.
///
/// The document is `server/tests/fixtures/messages/README.md`, **not** `CLAUDE.md`, and that is the whole
/// point of moving it. `CLAUDE.md` is tracked, but project convention keeps it out of routine commits, so its
/// committed content lags the working copy by however much has been written since: a test reading it compares
/// the corpus against whatever state a checkout happens to carry, and so passes or fails on how recently
/// someone committed a document rather than on whether the corpus matches it. This README is maintained
/// beside the fixtures, which is what makes it answerable.
#[test]
fn the_corpus_matches_the_support_matrix() {
    let doc = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/messages/README.md");
    let text = std::fs::read_to_string(&doc).expect("fixtures README");

    // Rows look like: | `suite` | version | samples | requests |
    let mut documented: Vec<(String, usize, usize, String)> = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if !line.starts_with("| `") {
            continue;
        }
        let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
        if cells.len() != 4 {
            continue;
        }
        let suite = cells[0].trim_matches('`');
        let (Ok(samples), Ok(requests)) = (cells[2].parse::<usize>(), cells[3].parse::<usize>())
        else {
            continue;
        };
        documented.push((suite.to_string(), samples, requests, cells[1].to_string()));
    }
    documented.sort();
    assert!(
        documented.len() > 10,
        "the support matrix was not found in the fixtures README; this test is the thing that keeps it \
         true, so a \
         format change here must be matched there"
    );

    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/messages");
    let mut found: Vec<(String, usize, usize)> = Vec::new();
    // Kept honest by `local_only_samples_are_actually_gitignored`, so this cannot drift into excusing a
    // sample that someone simply forgot to commit.
    const LOCAL_ONLY_SAMPLES: [(&str, &str); 2] =
        [("strands-js", "image-gen"), ("vercel-ai-js", "image-gen")];

    for entry in std::fs::read_dir(&root).expect("fixture root") {
        let path = entry.expect("dir entry").path();
        if !path.is_dir() {
            continue;
        }
        let suite = path.file_name().unwrap().to_string_lossy().to_string();
        let mut samples = 0usize;
        let mut requests = 0usize;
        for sample in std::fs::read_dir(&path).expect("suite dir") {
            let sample = sample.expect("sample entry").path();
            if !sample.is_dir() {
                continue;
            }
            // Samples captured locally only are not part of the documented corpus. They are gitignored
            // because their payloads are 15 MB and 7 MB of inlined base64 image data, so a clean checkout
            // does not have them - and comparing the *filesystem* against a table that counted them made
            // this test fail for everyone but whoever captured them, including CI. The table now describes
            // what the repository actually contains.
            let sample_name = sample.file_name().unwrap().to_string_lossy().to_string();
            if LOCAL_ONLY_SAMPLES.contains(&(suite.as_str(), sample_name.as_str())) {
                continue;
            }
            samples += 1;
            requests += std::fs::read_dir(&sample)
                .expect("sample dir")
                .filter_map(Result::ok)
                // The same predicate discovery uses, so the documented count is exactly what runs.
                .filter(|f| is_captured_request(&f.file_name().to_string_lossy()))
                .count();
        }
        found.push((suite, samples, requests));
    }
    found.sort();

    let documented_counts: Vec<(String, usize, usize)> = documented
        .iter()
        .map(|(s, a, b, _)| (s.clone(), *a, *b))
        .collect();
    assert_eq!(
        found, documented_counts,
        "the fixture corpus and the fixtures README's support matrix disagree; update the table, because \
         the \
         boundary of \"correct for all frameworks\" is exactly that list"
    );

    // Every row names the SDK it was captured against: a count without a version does not bound anything,
    // because the same framework's next release can emit a different shape.
    for (suite, _, _, version) in &documented {
        assert!(
            !version.is_empty() && version != "—",
            "the support matrix row for `{suite}` has no version, so it does not say what was verified"
        );
    }
}

/// A cached reconstruction is what recomputation would have produced, byte for byte.
///
/// The memo is only sound if reconstruction is a pure function of the rows, and that is a claim about
/// every stage rather than an intention - a single read of the clock, or an output order that follows a
/// `HashMap`'s iteration, and a cached answer stops matching a fresh one. Checked over the whole corpus
/// rather than argued: reconstruct each fixture cold, then warm, and require the serialised blocks to be
/// identical.
///
/// It also settles the horizontally-scaled question. Instances do not need to share this cache or agree
/// on when to drop entries, because the memo is over a pure function: the same rows give the same answer
/// on any instance, an instance that starts cold simply recomputes it, and an instance that dies loses
/// nothing but the saving. What N instances change is the hit rate, not the answer.
#[test]
fn a_cached_reconstruction_equals_a_fresh_one() {
    use crate::domain::sideml::feed::cache::ReconstructionCache;
    use crate::domain::sideml::feed::{process_spans, process_spans_cached};

    let mut checked = 0usize;
    for (label, paths) in discover_fixtures() {
        let rows: Vec<MessageSpanRow> = rows_for(&paths)
            .into_iter()
            .map(|(_, r)| r)
            .filter(passes_content_filter)
            .collect();
        if rows.is_empty() {
            continue;
        }
        let rows = sorted_by_timestamp(rows);

        let fresh = process_spans(rows.clone(), &FeedOptions::new());
        let cache = ReconstructionCache::new();
        let cold = process_spans_cached(&cache, rows.clone(), &FeedOptions::new());
        let warm = process_spans_cached(&cache, rows, &FeedOptions::new());
        assert_eq!(
            cache.len(),
            1,
            "{label}: the second read must have been a hit"
        );

        let serialise = |result: &crate::domain::sideml::feed::FeedResult| -> String {
            result
                .messages
                .iter()
                .map(|b| {
                    format!(
                        "{}|{}|{}|{:?}|{}|{}",
                        b.trace_id,
                        b.span_id,
                        b.role.as_str(),
                        b.content,
                        b.timestamp.to_rfc3339(),
                        b.is_history
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        };
        assert_eq!(
            serialise(&cold),
            serialise(&fresh),
            "{label}: the cached path's cold read differs from the uncached one"
        );
        assert_eq!(
            serialise(&warm),
            serialise(&fresh),
            "{label}: a warm read differs from recomputing, so the memo is not transparent"
        );
        checked += 1;
    }
    assert!(checked > 80, "only checked {checked} fixtures");
}

/// Changing only the presentation constraints must not change which messages a session returns.
///
/// The acceptance property of the ordering redesign, and it was false until the causal transcript was
/// separated from the presented order: `process_multi_trace_spans` accumulated its cross-trace replay
/// prefix from each trace's *finished, ordered* messages, and the next trace consumed it as a sequence.
/// So promoting an ordering constraint changed what a later trace stripped - `adk/tool_use`'s session
/// view gained five replayed messages, the previous turns' tool results re-appearing, purely because
/// the presented sequence no longer lined up with what ADK replays.
///
/// Run over the whole row set of every fixture, so the multi-trace path is the one under test.
#[test]
fn ordering_constraints_do_not_change_a_session_s_messages() {
    let mut checked = 0usize;
    for (label, paths) in discover_fixtures() {
        let rows: Vec<MessageSpanRow> = rows_for(&paths)
            .into_iter()
            .map(|(_, r)| r)
            .filter(passes_content_filter)
            .collect();
        if rows.is_empty() {
            continue;
        }
        let (presented, unconstrained) = presented_and_unconstrained(sorted_by_timestamp(rows));
        let multiset = |blocks: &[crate::domain::sideml::feed::BlockEntry]| -> Vec<String> {
            let mut out: Vec<String> = blocks
                .iter()
                .map(|b| {
                    format!(
                        "{}/{}/{}/{}",
                        b.trace_id,
                        b.role.as_str(),
                        b.entry_type,
                        b.content_hash
                    )
                })
                .collect();
            out.sort();
            out
        };
        assert_eq!(
            multiset(&presented),
            multiset(&unconstrained),
            "{label}: the ordering constraints changed which messages the session returns, so \
             deduplication still depends on presentation"
        );
        checked += 1;
    }
    assert!(checked > 80, "only checked {checked} fixtures");
}

#[test]
#[ignore]
fn probe_pre_dedup() {
    let want = std::env::var("PROBE").unwrap_or_else(|_| "_synthetic/tool_use".to_string());
    let (_, paths) = discover_fixtures()
        .into_iter()
        .find(|(l, _)| *l == want)
        .unwrap_or_else(|| panic!("fixture {want} not found"));
    let rows: Vec<MessageSpanRow> = rows_for(&paths)
        .into_iter()
        .map(|(_, r)| r)
        .filter(passes_content_filter)
        .collect();
    for (i, b) in crate::domain::sideml::feed::classified_blocks_for_test(sorted_by_timestamp(rows))
        .iter()
        .enumerate()
    {
        let c: String = format!("{:?}", b.content).chars().take(60).collect();
        eprintln!(
            "{i:2} span={} {:9} {:11} out={} hist={} obs={:?} carrier={:?}/{:?} {c}",
            &b.span_id[..8],
            b.role.as_str(),
            b.entry_type,
            b.is_output_source(),
            b.is_history,
            b.observation_type,
            b.event_name,
            b.source_attribute
        );
    }
}

/// Every carrier the corpus produces is classified deliberately in `sideml::carrier`.
///
/// `carrier.rs` claims this test exists; until now it did not. It cannot prove a classification is
/// semantically right - identical JSON can be accumulated state or one emission - but it can require
/// that no carrier reaches the resolver on the catch-all default without someone having looked at it.
/// That is the difference between a declaration and a guess.
#[test]
fn carrier_semantics_are_declared() {
    use crate::domain::sideml::carrier::declared_semantics;

    // Carriers the corpus contains that are knowingly left on the cautious default. Each one is a
    // decision, not an oversight: the reading is "a conversation as this span saw it", which can
    // under-report but never invent, and the answer invariant would catch under-reporting.
    const KNOWN_DEFAULTED: &[&str] = &[
        "attr:gcp.vertex.agent.llm_request",
        "attr:gcp.vertex.agent.llm_response",
        "attr:mlflow.spanInputs",
        "attr:mlflow.spanOutputs",
        "attr:traceloop.entity.input",
        "attr:traceloop.entity.output",
        "attr:response_data",
        "attr:system_prompt",
        "attr:crewai.crew.tasks_output",
    ];

    let mut seen: BTreeSet<String> = BTreeSet::new();
    for (_, paths) in discover_fixtures() {
        let rows: Vec<MessageSpanRow> = rows_for(&paths).into_iter().map(|(_, r)| r).collect();
        for block in crate::domain::sideml::feed::classified_blocks_for_test(rows) {
            match (&block.event_name, &block.source_attribute) {
                (Some(event), _) => seen.insert(format!("event:{event}")),
                (None, Some(attribute)) => seen.insert(format!("attr:{attribute}")),
                (None, None) => false,
            };
        }
    }
    assert!(
        seen.len() > 20,
        "expected the corpus to exercise many carriers, found {}",
        seen.len()
    );

    // Asked of the table directly, not by comparing values: a declared snapshot carrier and an
    // unclassified one have the same semantics and are completely different facts.
    let mut undeclared: Vec<String> = Vec::new();
    for carrier in &seen {
        let declared = match carrier.strip_prefix("event:") {
            Some(event) => declared_semantics(Some(event), None),
            None => declared_semantics(None, carrier.strip_prefix("attr:")),
        };
        if declared.is_none() && !KNOWN_DEFAULTED.contains(&carrier.as_str()) {
            undeclared.push(carrier.clone());
        }
    }
    assert!(
        undeclared.is_empty(),
        "these carriers reach the resolver on the catch-all default and nobody has classified them:\n  \
         {}\nAdd each to `sideml::carrier::semantics_for`, or to KNOWN_DEFAULTED with the reason the \
         cautious reading is right for it.",
        undeclared.join("\n  ")
    );
}

/// Which copy of a message survives deduplication must not change the *order* of the answer.
///
/// The central claim of the ordering redesign, and it had no test. Reversing the row order does not
/// reach it - rows are re-sorted by timestamp before dedup - so nothing varied the one thing that used
/// to decide the order: the surviving block's index in the previous sort.
///
/// `PREFER_LATER_ON_TIE` flips the tie-break, which changes the surviving copy in 17 of the 111
/// fixtures, so the perturbation is real rather than theoretical. Content may legitimately differ when
/// it flips - a copy carrying thinking blocks or model info is a better one, which is what quality
/// scoring exists to pick - so what is compared is the sequence of roles and kinds, not the text.
#[test]
fn which_copy_survives_does_not_change_the_order() {
    use crate::domain::sideml::feed::PREFER_LATER_ON_TIE;

    let set = |value: bool| PREFER_LATER_ON_TIE.with(|flag| flag.set(value));

    let mut differing: Vec<String> = Vec::new();
    let mut perturbed = 0usize;
    let content_of = |result: &crate::domain::sideml::feed::FeedResult| -> Vec<String> {
        result
            .messages
            .iter()
            .map(|b| b.content_hash.clone())
            .collect()
    };
    for (label, paths) in discover_fixtures() {
        let rows: Vec<MessageSpanRow> = rows_for(&paths)
            .into_iter()
            .map(|(_, r)| r)
            .filter(passes_content_filter)
            .collect();
        if rows.is_empty() {
            continue;
        }
        let rows = sorted_by_timestamp(rows);
        let rows_again = rows.clone();

        let shape = |blocks: &[crate::domain::sideml::feed::BlockEntry]| -> Vec<String> {
            blocks
                .iter()
                .map(|b| format!("{}/{}", b.role.as_str(), b.entry_type))
                .collect()
        };

        set(false);
        let early = shape(&process_spans(rows.clone(), &FeedOptions::new()).messages);
        set(true);
        let late = shape(&process_spans(rows, &FeedOptions::new()).messages);
        set(false);

        if early != late {
            differing.push(format!(
                "{label}:\n    early: {early:?}\n    late:  {late:?}"
            ));
        }

        // The perturbation has to be *reaching* dedup, or this test proves nothing. A flipped
        // tie-break selects a different copy, and copies differ in content, so somewhere in the corpus
        // the content must move even while the shape does not.
        set(false);
        let early_content = content_of(&process_spans(rows_again.clone(), &FeedOptions::new()));
        set(true);
        let late_content = content_of(&process_spans(rows_again, &FeedOptions::new()));
        set(false);
        if early_content != late_content {
            perturbed += 1;
        }
    }

    assert!(
        perturbed > 0,
        "flipping the dedup tie-break changed nothing anywhere, so this test is vacuous - the hook is \
         not reaching dedup"
    );
    assert!(
        differing.is_empty(),
        "the order of the answer depends on which copy survived dedup, in {} view(s):\n  {}",
        differing.len(),
        differing.join("\n  ")
    );
}

/// A barrier node orders exactly as the pairwise edges it replaces.
///
/// "Everything a generation received precedes everything it produced" is the *product* of the two sets
/// as edges, and a span re-sending a long history has hundreds of inputs - so the graph grew
/// quadratically in a span's message count. One barrier expresses the same relation in `inputs + outputs`
/// edges, keyed to pop exactly when the earliest output would have.
///
/// That is only sound if the two agree, and they do not agree unconditionally: where a span both
/// received and produced the same message the sets overlap, and a barrier would assert
/// `u -> barrier -> u` while the product contains a real `u -> b`. Those spans keep the pairwise form,
/// which is why this test compares the whole corpus rather than a constructed case.
#[test]
fn a_barrier_orders_exactly_as_pairwise_edges_do() {
    use crate::domain::sideml::feed::barrier_and_pairwise_order;

    let mut checked = 0usize;
    for (label, paths) in discover_fixtures() {
        let rows: Vec<MessageSpanRow> = rows_for(&paths)
            .into_iter()
            .map(|(_, r)| r)
            .filter(passes_content_filter)
            .collect();
        if rows.is_empty() {
            continue;
        }
        let (barrier, pairwise) = barrier_and_pairwise_order(sorted_by_timestamp(rows));
        let shape = |blocks: &[crate::domain::sideml::feed::BlockEntry]| -> Vec<String> {
            blocks
                .iter()
                .map(|b| {
                    format!(
                        "{}/{}/{}/{}",
                        b.trace_id,
                        b.role.as_str(),
                        b.entry_type,
                        b.content_hash
                    )
                })
                .collect()
        };
        assert_eq!(
            shape(&barrier),
            shape(&pairwise),
            "{label}: the barrier node changed the order the pairwise edges produce"
        );
        checked += 1;
    }
    assert!(checked > 80, "only checked {checked} fixtures");
}

/// End-to-end ingestion: OTLP bytes through extraction, enrichment, file storage and both writes.
///
/// `bench_pipeline`'s ingestion number is explicitly the CPU half - it stops before file extraction and
/// every persistence step - so what a request actually costs was unmeasured, which is not a claim anyone
/// should accept about a write path. This runs the real `run_batch`: a temp DuckDB, a temp SQLite, real
/// filesystem file storage, and the fence lookups included.
///
/// ```bash
/// cargo test --release -p sideseat-server bench_ingestion -- --ignored --nocapture
/// ```
// The multi-threaded runtime, because `run_batch` uses `block_in_place` for its parallel extraction -
// so the current-thread runtime a plain `#[tokio::test]` gives would panic rather than measure.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_ingestion_end_to_end() {
    use crate::core::config::{FilesConfig, StorageBackend};
    use crate::core::storage::AppStorage;
    use crate::data::files::FileService;
    use crate::data::{AnalyticsService, TransactionalService};
    use crate::domain::traces::TracePipeline;
    use std::sync::Arc;

    let want = std::env::var("BENCH").unwrap_or_else(|_| "langgraph/swarm".to_string());
    let (label, paths) = discover_fixtures()
        .into_iter()
        .find(|(l, _)| *l == want)
        .unwrap_or_else(|| panic!("fixture {want} not found; set BENCH=<suite>/<sample>"));
    let requests: Vec<ExportTraceServiceRequest> =
        paths.iter().map(|p| decode_request(p)).collect();
    let bytes: usize = paths
        .iter()
        .map(|p| std::fs::metadata(p).map(|m| m.len() as usize).unwrap_or(0))
        .sum();

    let iterations = std::env::var("ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10u32);

    // Whatever refuses a batch says so through `tracing`, and a refused batch measures nothing - so the
    // reason has to be visible rather than inferred.
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();

    let mut totals: Vec<std::time::Duration> = Vec::new();
    let mut spans_written = 0u64;
    for _ in 0..iterations {
        // A fresh instance per iteration: ingestion is idempotent by span id, so re-delivering the same
        // requests into one database would measure an update path rather than a first write.
        let temp = tempfile::TempDir::new().expect("temp dir");
        let storage = AppStorage::init_for_test(temp.path().to_path_buf());
        tokio::fs::create_dir_all(temp.path().join("duckdb"))
            .await
            .expect("duckdb dir");
        // Both, and `files_temp` is the one that is easy to miss: `AppStorage::subdir` does not create
        // directories, and file writes stage into `files_temp` before being renamed into place - so
        // without it every write fails and the batch is refused rather than measured.
        tokio::fs::create_dir_all(temp.path().join("files"))
            .await
            .expect("files dir");
        tokio::fs::create_dir_all(temp.path().join("files_temp"))
            .await
            .expect("files temp dir");
        let analytics = Arc::new(AnalyticsService::Duckdb(Arc::new(
            crate::data::duckdb::DuckdbService::init(&storage)
                .await
                .expect("duckdb"),
        )));
        let sqlite_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("sqlite");
        sqlx::raw_sql(crate::data::sqlite::schema::SCHEMA)
            .execute(&sqlite_pool)
            .await
            .expect("sqlite schema");
        let database = Arc::new(TransactionalService::Sqlite(Arc::new(
            crate::data::sqlite::SqliteService::from_pool(sqlite_pool),
        )));
        let files = Arc::new(
            FileService::new(
                FilesConfig {
                    enabled: true,
                    storage: StorageBackend::Filesystem,
                    quota_bytes: u64::MAX,
                    filesystem_path: Some(temp.path().join("files").display().to_string()),
                    s3: None,
                },
                &storage,
                Arc::clone(&database),
                Arc::new(
                    crate::data::cache::CacheService::new(&crate::core::config::CacheConfig {
                        backend: crate::core::config::CacheBackendType::Memory,
                        max_entries: 1000,
                        eviction_policy: crate::core::config::EvictionPolicy::TinyLfu,
                        redis_url: None,
                        redis_min_replica_acks: 0,
                    })
                    .await
                    .expect("memory cache"),
                ),
            )
            .await
            .expect("file service"),
        );
        let pipeline = TracePipeline::new(
            Arc::clone(&analytics),
            Arc::new(PricingService::init_for_test().expect("offline pricing service")),
            Arc::new(crate::core::TopicService::default()),
            files,
        );

        // `PER_REQUEST=1` measures the same work with no batching, which is what acknowledging only
        // what is stored would cost: one write per request instead of one per batch.
        let per_request = std::env::var("PER_REQUEST").is_ok();
        let start = std::time::Instant::now();
        let ok = if per_request {
            let mut all = true;
            for request in &requests {
                all &= pipeline
                    .run_batch_for_test(std::slice::from_ref(request))
                    .await;
            }
            all
        } else {
            pipeline.run_batch_for_test(&requests).await
        };
        totals.push(start.elapsed());
        assert!(
            ok,
            "{label}: the batch was refused, so this measures nothing"
        );

        spans_written = analytics
            .repository()
            .count_spans_by_project(&["default".to_string()])
            .await
            .map(|c| c.values().sum())
            .unwrap_or(0);
        assert!(
            spans_written > 0,
            "{label}: nothing was written - the batch was accepted but its spans went nowhere"
        );
    }

    totals.sort();
    let sum: std::time::Duration = totals.iter().sum();
    eprintln!(
        "INGEST(end to end) {label}: {} requests / {} KB -> {spans_written} spans, \
         mean {:?}, p50 {:?}, max {:?} over {iterations} runs",
        requests.len(),
        bytes / 1024,
        sum / iterations,
        totals[totals.len() / 2],
        totals[totals.len() - 1],
    );
    assert!(
        spans_written > 0,
        "nothing was written, so nothing was measured"
    );
}

/// How reconstruction scales with the history a read has to reprocess.
///
/// Normalisation happens at query time, so a session read reparses and rehashes the whole session on
/// every request - there is no cache. Whether that matters is a question about the curve, not about the
/// design, and the curve was unmeasured. Two shapes, because frameworks differ in the one way that
/// decides the answer:
///
/// - **replaying**: every generation span re-sends the whole conversation so far, which is what ADK,
///   LangGraph and Vercel do. The *input* is then quadratic in the turn count however fast the pipeline
///   is, so this is the shape that can hurt.
/// - **incremental**: every span carries only its own turn, as Strands' per-message events do. Linear
///   input.
///
/// ```bash
/// cargo test --release -p sideseat-server bench_session_scaling -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn bench_session_scaling() {
    fn row(
        trace: usize,
        span: usize,
        messages: String,
        t: chrono::DateTime<chrono::Utc>,
    ) -> MessageSpanRow {
        MessageSpanRow {
            trace_id: format!("trace-{trace}"),
            span_id: format!("span-{trace}-{span}"),
            parent_span_id: None,
            span_timestamp: t,
            span_end_timestamp: Some(t),
            messages_json: messages,
            tool_definitions_json: "[]".to_string(),
            tool_names_json: "[]".to_string(),
            model: Some("claude".to_string()),
            provider: Some("bedrock".to_string()),
            status_code: None,
            exception_type: None,
            exception_message: None,
            exception_stacktrace: None,
            input_tokens: 10,
            output_tokens: 5,
            total_tokens: 15,
            cost_total: 0.001,
            observation_type: Some("generation".to_string()),
            session_id: Some("session-1".to_string()),
            ingested_at: t,
        }
    }

    use chrono::TimeZone;
    let t0 = chrono::Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .expect("valid time");

    for turns in [100usize, 1_000, 10_000] {
        for shape in ["incremental", "replaying"] {
            // The replaying shape is quadratic in its own input, so the largest size would be 10^8
            // message entries - which measures the fixture generator, not the pipeline.
            if shape == "replaying" && turns > 1_000 {
                continue;
            }

            let build = std::time::Instant::now();
            let mut rows = Vec::with_capacity(turns);
            for turn in 0..turns {
                let t = t0 + chrono::Duration::seconds(turn as i64);
                let mut entries: Vec<String> = Vec::new();
                let history = if shape == "replaying" { 0 } else { turn };
                for past in history..=turn {
                    entries.push(format!(
                        r#"{{"source":{{"attribute":{{"key":"llm.input_messages","time":"{}"}}}},"content":{{"role":"user","content":"question {past}"}}}}"#,
                        (t0 + chrono::Duration::seconds(past as i64)).to_rfc3339()
                    ));
                }
                entries.push(format!(
                    r#"{{"source":{{"event":{{"name":"gen_ai.choice","time":"{}"}}}},"content":{{"role":"assistant","content":"answer {turn}"}}}}"#,
                    t.to_rfc3339()
                ));
                rows.push(row(turn, 0, format!("[{}]", entries.join(",")), t));
            }
            let built = build.elapsed();
            let payload: usize = rows.iter().map(|r| r.messages_json.len()).sum();

            // Cold, then warm: the second read is what a user's second look at a session costs, and
            // whether the memo earns its place is exactly that difference.
            let cache = crate::domain::sideml::feed::cache::ReconstructionCache::new();
            let start = std::time::Instant::now();
            let result = crate::domain::sideml::feed::process_spans_cached(
                &cache,
                rows.clone(),
                &FeedOptions::new(),
            );
            let cold = start.elapsed();
            let start = std::time::Instant::now();
            let warm_result = crate::domain::sideml::feed::process_spans_cached(
                &cache,
                rows,
                &FeedOptions::new(),
            );
            let warm = start.elapsed();
            assert_eq!(
                warm_result.messages.len(),
                result.messages.len(),
                "a warm read must answer with the same blocks"
            );
            eprintln!(
                "SESSION {shape} {turns} turns: {} KB input -> {} blocks, cold {:?}, warm {:?} \
                 (fixture built in {:?})",
                payload / 1024,
                result.messages.len(),
                cold,
                warm,
                built
            );
        }
    }
}

/// A session known only to the store reconstructs exactly as one named on every row.
///
/// The feed groups traces into conversations so a replay crossing traces can be recognised, and it derived
/// that grouping from the `session_id` on the rows it was handed. Those rows have been through
/// `MESSAGE_CONTENT_FILTER`, and a framework records the session on the span that knows it - usually a root
/// carrying no messages, which the filter removes. When every row naming the session was filtered out, each
/// trace became its own conversation, the cross-trace stripping never ran, and the second trace's re-sent
/// history came back as duplicates while the response still reported `session_scoped`.
///
/// Checked against the fixtures where cross-trace stripping actually fires - the ones whose session view
/// holds fewer messages than its traces' views sum to - because a fixture with nothing to strip would pass
/// this test without exercising anything.
#[test]
fn a_session_known_only_to_the_store_reconstructs_identically() {
    let fixtures = discover_fixtures();
    if fixtures.is_empty() {
        eprintln!("session grouping: no fixtures - run misc/capture-message-fixtures.sh");
        return;
    }

    let mut exercised = 0usize;
    let mut failures: Vec<String> = Vec::new();

    for (label, paths) in &fixtures {
        let rows = rows_for(paths);

        // The trace -> session mapping the store would report.
        let mut session_of_trace: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for (_, row) in &rows {
            if let Some(session) = row.session_id.as_deref().filter(|s| !s.is_empty()) {
                session_of_trace
                    .entry(row.trace_id.clone())
                    .or_insert_with(|| session.to_string());
            }
        }
        // Only fixtures whose session spans more than one trace can strip anything across traces.
        let traces: std::collections::HashSet<&String> = session_of_trace.keys().collect();
        if session_of_trace.is_empty() || traces.len() < 2 {
            continue;
        }

        let with_session_on_rows: Vec<MessageSpanRow> =
            rows.iter().map(|(_, r)| r.clone()).collect();

        // The same rows with every session id removed - the state the content filter leaves when the only
        // spans naming the session carried no messages.
        let stripped: Vec<MessageSpanRow> = with_session_on_rows
            .iter()
            .cloned()
            .map(|mut r| {
                r.session_id = None;
                r
            })
            .collect();

        let named = process_feed(with_session_on_rows, &FeedOptions::new());
        let from_store = process_feed(
            stripped.clone(),
            &FeedOptions::new().with_session_of_trace(session_of_trace.clone()),
        );
        let ungrouped = process_feed(stripped, &FeedOptions::new());

        let describe = |result: &crate::domain::sideml::FeedResult| -> Vec<String> {
            result
                .messages
                .iter()
                .map(|b| format!("{}:{}", b.role.as_str(), b.content_hash))
                .collect()
        };

        let named_blocks = describe(&named);
        let store_blocks = describe(&from_store);

        if named_blocks != store_blocks {
            failures.push(format!(
                "{label}: a store-supplied session differs from one on the rows\n  on rows: {named_blocks:?}\n  from store: {store_blocks:?}"
            ));
            continue;
        }

        // And the grouping is what does the work: without it, this fixture returns more.
        if describe(&ungrouped).len() > named_blocks.len() {
            exercised += 1;
        }
    }

    assert!(
        failures.is_empty(),
        "session grouping must not depend on which rows survived the content filter:\n{}",
        failures.join("\n")
    );
    assert!(
        exercised > 0,
        "no fixture exercised cross-trace stripping, so this test proved nothing"
    );
    eprintln!("session grouping: {exercised} fixture(s) exercised cross-trace stripping");
}

/// Every sample the support matrix excuses is one `.gitignore` actually excludes.
///
/// The exclusion list exists so a clean checkout passes `the_corpus_matches_the_support_matrix`, and that is
/// only legitimate for samples the repository deliberately does not carry. Without this check the list would
/// equally excuse a sample somebody forgot to commit, which is the opposite of what the matrix is for.
#[test]
fn local_only_samples_are_actually_gitignored() {
    let gitignore = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("repo root")
            .join(".gitignore"),
    )
    .expect("read .gitignore");

    for (suite, sample) in [("strands-js", "image-gen"), ("vercel-ai-js", "image-gen")] {
        let path = format!("server/tests/fixtures/messages/{suite}/{sample}/");
        assert!(
            gitignore.lines().any(|line| line.trim() == path),
            "{path} is excused from the support matrix but is not gitignored, so a missing capture there \
             would be silently accepted"
        );
    }
}
