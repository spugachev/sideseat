//! Trace persistence (Stage 4)
//!
//! Handles SSE publishing and DuckDB writes with retry logic.
//! Stores raw messages (not normalized) for data preservation.
//! SideML conversion happens at query time in feed pipeline (process_spans).
//! Builds raw span JSON from original OTLP request (deferred for performance).
//!
//! ## File Extraction
//!
//! Before persisting to DuckDB, base64 data >= 1KB is extracted from messages
//! and replaced with `#!B64!#[mime]::hash` URIs. Files are stored separately
//! with reference counting for cleanup.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use base64::prelude::*;
use futures::stream::StreamExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::Span;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use super::enrich::SpanEnrichment;
use super::extract::files::{
    ExtractedFile, FileExtractionCache, collect_file_references_in_str, extract_and_replace_files,
    extract_and_replace_files_cached,
};
use super::extract::{RawMessage, RawToolDefinition, RawToolNames, SpanData};
use crate::core::constants::{
    DEFAULT_PROJECT_ID, FILE_HASH_ALGORITHM, FILES_MAX_CONCURRENT_FINALIZATION,
};
use crate::core::{TopicMessage, TopicService};
use crate::data::AnalyticsService;
use crate::data::files::FileService;
use crate::data::types::{NormalizedSpan, json_to_pre_serialized};
use crate::utils::otlp::{build_attributes_json, extract_attributes};
use crate::utils::retry::{DEFAULT_BASE_DELAY_MS, DEFAULT_MAX_ATTEMPTS, retry_with_backoff_async};
use crate::utils::time::nanos_to_iso;

// ============================================================================
// SSE EVENT MODEL
// ============================================================================

/// SSE event for notifying clients of new spans.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SseSpanEvent {
    pub project_id: Option<String>,
    pub trace_id: String,
    pub span_id: String,
    pub session_id: Option<String>,
    pub user_id: Option<String>,
}

impl From<&NormalizedSpan> for SseSpanEvent {
    fn from(span: &NormalizedSpan) -> Self {
        Self {
            project_id: span.project_id.clone(),
            trace_id: span.trace_id.clone(),
            span_id: span.span_id.clone(),
            session_id: span.session_id.clone(),
            user_id: span.user_id.clone(),
        }
    }
}

impl TopicMessage for SseSpanEvent {
    fn size_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.project_id.as_ref().map_or(0, |s| s.len())
            + self.trace_id.len()
            + self.span_id.len()
            + self.session_id.as_ref().map_or(0, |s| s.len())
            + self.user_id.as_ref().map_or(0, |s| s.len())
    }
}

// ============================================================================
// BATCH PERSISTENCE
// ============================================================================

/// File data extracted during CPU phase, pending I/O write.
///
/// Contains the file bytes and metadata needed to persist to storage.
/// Created during the CPU-only extraction phase, consumed by
/// `persist_extracted_files` in parallel with DuckDB write.
pub(super) struct PendingFileWrite {
    pub project_id: String,
    pub trace_id: String,
    pub hash: String,
    pub media_type: Option<String>,
    pub size: usize,
    /// Raw base64 bytes (not decoded) for cache misses, empty for cache hits
    pub data: Vec<u8>,
    pub hash_algo: String,
}

/// Extracted data from a batch of spans, ready for flattening and persistence.
pub(super) struct BatchInput {
    pub spans: Vec<SpanData>,
    pub messages: Vec<Vec<RawMessage>>,
    pub tool_definitions: Vec<Vec<RawToolDefinition>>,
    pub tool_names: Vec<Vec<RawToolNames>>,
    pub enrichments: Vec<SpanEnrichment>,
}

/// Prepare spans for persistence: file extraction (CPU only) + flatten.
///
/// Returns NormalizedSpans ready for DuckDB write, plus pending file writes
/// that should be persisted in a background task via `persist_extracted_files`.
///
/// File extraction (CPU phase) replaces base64 data with `#!B64!#` URIs
/// in the JSON fields so DuckDB stores compact references, not raw base64.
/// The actual file I/O (temp write, metadata upsert, finalization) is deferred.
pub(super) fn prepare_batch(
    request: &ExportTraceServiceRequest,
    input: BatchInput,
    files_enabled: bool,
    file_cache: Option<&FileExtractionCache>,
) -> (
    Vec<NormalizedSpan>,
    Vec<PendingFileWrite>,
    Vec<IncomingReference>,
) {
    let mut pending_files = Vec::new();
    let mut incoming_references = Vec::new();

    // Extract files from messages (CPU only: replace base64 with URIs)
    let processed_messages = if files_enabled {
        let (msgs, files) = extract_files_cpu_messages(input.messages, &input.spans, file_cache);
        pending_files = files;
        msgs
    } else {
        input.messages
    };

    // Convert SpanData + Enrichment to NormalizedSpan, build raw span JSON.
    // File extraction from raw_span/tool_definitions/metadata is done inline
    // BEFORE serialization, eliminating the serialize→deserialize→re-serialize round-trip.
    let (db_spans, raw_span_files) = flatten(
        request,
        input.spans,
        processed_messages,
        input.tool_definitions,
        input.tool_names,
        input.enrichments,
        files_enabled,
        file_cache,
    );
    pending_files.extend(raw_span_files);

    // References that arrived already formed, read from the rows *about to be written* rather than from
    // any one extraction step.
    //
    // A reference can appear in messages, tool definitions, raw span JSON or metadata, and each is
    // extracted by a different path - so collecting per path missed whichever path was not covered.
    // Scanning the committed strings states the invariant directly: every reference in a row that is
    // written is either one this batch produced, or one that has been verified.
    // Keyed by `(project, trace, uri)`, not by URI alone.
    //
    // Global, one trace extracting URI `U` exempted *every* row containing `U` from reconciliation - so
    // a trace that supplied `U` already formed got no association, and cross-project it could be
    // dangling immediately. Same project, it survived only until the trace that really produced it was
    // deleted.
    let ours: HashSet<(&str, &str, String)> = pending_files
        .iter()
        .map(|f| {
            (
                f.project_id.as_str(),
                f.trace_id.as_str(),
                crate::utils::file_uri::build_file_uri(&f.hash, f.media_type.as_deref()),
            )
        })
        .collect();
    for span in &db_spans {
        let project_id = span.project_id.as_deref().unwrap_or(DEFAULT_PROJECT_ID);
        let mut present = Vec::new();
        for field in [
            span.messages.as_deref(),
            span.tool_definitions.as_deref(),
            span.raw_span.as_deref(),
            span.metadata.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            collect_file_references_in_str(field, &mut present);
        }
        for uri in present {
            // The whole URI, not the hash: one carrying our hash with a different media type is not
            // ours, and it would also miss the exact-string rewrite if it turned out unbacked.
            if !ours.contains(&(project_id, span.trace_id.as_str(), uri.clone())) {
                incoming_references.push((project_id.to_string(), span.trace_id.clone(), uri));
            }
        }
    }
    incoming_references.sort_unstable();
    incoming_references.dedup();

    (db_spans, pending_files, incoming_references)
}

// ============================================================================
// SSE PUBLISHING
// ============================================================================

/// Publish pre-built SSE events to per-project topics for real-time streaming.
///
/// Uses BroadcastTopic for distributed pub/sub. In Redis mode, events are
/// published via Redis Pub/Sub so all SSE endpoints receive them.
///
/// SSE events are built before DuckDB write so clients only receive notifications
/// after both the DuckDB write and file persistence are complete.
pub(super) async fn publish_sse_events(events: &[SseSpanEvent], topics: &TopicService) {
    for event in events {
        if let Some(ref project_id) = event.project_id {
            let topic_name = format!("sse_spans:{}", project_id);
            let topic = topics.broadcast_topic::<SseSpanEvent>(&topic_name);
            if let Err(e) = topic.publish(event).await {
                tracing::warn!(error = %e, project_id, "Failed to publish SSE event");
            }
        }
    }
}

// ============================================================================
// FILE EXTRACTION
// ============================================================================

/// File pending finalization to permanent storage
struct PendingFinalization {
    project_id: String,
    hash: String,
    temp_path: std::path::PathBuf,
}

/// Finalize pending files to permanent storage with bounded concurrency.
///
/// Uses `buffer_unordered` to limit concurrent I/O operations, preventing
/// resource exhaustion when processing batches with many files.
async fn finalize_pending_files(
    pending: Vec<PendingFinalization>,
    file_service: &Arc<FileService>,
    context: &str,
) -> usize {
    if pending.is_empty() {
        return 0;
    }

    let count = pending.len();
    let storage = file_service.storage();

    // Use buffer_unordered for bounded concurrency instead of unbounded join_all
    let results: Vec<_> = futures::stream::iter(pending.into_iter().map(|pf| {
        let storage = Arc::clone(storage);
        async move {
            let result = storage
                .finalize_temp(&pf.project_id, &pf.hash, &pf.temp_path)
                .await;
            (pf, result)
        }
    }))
    .buffer_unordered(FILES_MAX_CONCURRENT_FINALIZATION)
    .collect()
    .await;

    let mut success_count = 0usize;
    let mut failure_count = 0usize;
    for (pf, result) in results {
        match result {
            Ok(()) => success_count += 1,
            Err(e) => {
                failure_count += 1;
                tracing::warn!(
                    error = %e,
                    hash = %pf.hash,
                    project_id = %pf.project_id,
                    "Failed to finalize file to permanent storage"
                );
            }
        }
    }

    if failure_count > 0 {
        tracing::warn!(
            finalized = success_count,
            failed = failure_count,
            total = count,
            context,
            "File finalization complete with failures"
        );
    } else if success_count > 0 {
        tracing::debug!(
            files = success_count,
            context,
            "Files finalized successfully"
        );
    }

    failure_count
}

/// Convert extracted files to pending writes for a given project/trace.
fn to_pending_files<'a>(
    files: Vec<ExtractedFile>,
    project_id: &'a str,
    trace_id: &'a str,
) -> impl Iterator<Item = PendingFileWrite> + 'a {
    files.into_iter().map(move |f| PendingFileWrite {
        project_id: project_id.to_string(),
        trace_id: trace_id.to_string(),
        hash: f.hash,
        media_type: f.media_type,
        size: f.size,
        data: f.data,
        hash_algo: FILE_HASH_ALGORITHM.to_string(),
    })
}

/// CPU-only: extract files from all messages in a batch.
///
/// Replaces base64 data with `#!B64!#` URIs in message content.
/// Returns modified messages and pending file writes for I/O phase.
fn extract_files_cpu_messages(
    mut messages: Vec<Vec<RawMessage>>,
    spans: &[SpanData],
    cache: Option<&FileExtractionCache>,
) -> (Vec<Vec<RawMessage>>, Vec<PendingFileWrite>) {
    let mut pending = Vec::new();

    for (span_idx, span_messages) in messages.iter_mut().enumerate() {
        let span = &spans[span_idx];
        let project_id = span.project_id.as_deref().unwrap_or(DEFAULT_PROJECT_ID);
        let trace_id = &span.trace_id;

        for raw_message in span_messages.iter_mut() {
            let result = match cache {
                Some(c) => extract_and_replace_files_cached(&mut raw_message.content, c),
                None => extract_and_replace_files(&mut raw_message.content),
            };
            pending.extend(to_pending_files(result.files, project_id, trace_id));
        }
    }

    (messages, pending)
}

/// What became of a batch's files.
///
/// Returned rather than logged, because the caller has a decision to make that a `warn!` cannot: a
/// span row carries `#!B64!#` references, so committing it when the file behind one is missing leaves
/// a reader with a reference to nothing. Backend parity cannot catch that - it proves DuckDB and
/// ClickHouse agree, never that either represents the OTLP input.
#[derive(Default)]
pub(super) struct FilePersistOutcome {
    /// Files that could not be stored: decode, temp write, metadata or finalization failed. Transient
    /// in nature, so the honest response is to fail the batch and let the exporter retry.
    pub failed: usize,
    /// `(project_id, uri)` deliberately not stored because the project is over its storage quota. Not
    /// an error and not retryable, so the batch is still committed - but the references have to be
    /// *rewritten* first, because a reader cannot tell a reference to a rejected file from a broken one.
    ///
    /// The project is part of the key because a batch spans projects and a hash is content-addressed:
    /// the same URI can be rejected in one project and stored in another.
    pub quota_skipped: Vec<(String, String)>,
    /// `(project_id, trace_id, file_hash)` associations this batch created, and no earlier one.
    ///
    /// A file's bytes are written before the analytics row that references them, which is the ordering
    /// that keeps a dangling reference impossible. The cost is that a batch whose analytics write fails
    /// has already created associations for spans that will never land - and those hold `ref_count`
    /// above zero, so the orphan sweeper (which selects on `ref_count = 0`) can never reclaim them. The
    /// file then occupies the project's quota for good.
    ///
    /// Recorded so the failure path can release precisely what it created. Only the *new* ones: an
    /// association that already existed belongs to an earlier committed batch, and releasing it would
    /// orphan that batch's file. A redelivery re-creates these, so the compensation costs nothing when
    /// the retry succeeds.
    pub created_associations: Vec<(String, String, String)>,
}

/// Replace references to files that were not stored with a placeholder a reader can understand.
///
/// A `#!B64!#` reference means "the bytes are in the file store". When a quota rejection means they
/// never got there, committing the reference unchanged leaves a reader unable to tell a *rejected*
/// file from a corrupt one - it renders as a broken image either way, and nothing on the span says
/// which. Neither is acceptable, and keeping the base64 inline is not either: that defeats the quota
/// by moving the bytes into the analytics store, where they are harder to reclaim.
///
/// So the reference is replaced by text that says what happened. It is a string substitution over the
/// already-serialised JSON deliberately: a reference can appear in messages, tool definitions, raw
/// span JSON or metadata, and re-parsing all of them to find it would cost more than the rejection.
pub(super) fn note_unstored_files(
    spans: &mut [NormalizedSpan],
    skipped: &[(String, String)],
) -> usize {
    if skipped.is_empty() {
        return 0;
    }
    let mut rewritten = 0usize;
    let replace = |field: &mut String, uri: &str, note: &str, count: &mut usize| {
        if field.contains(uri) {
            *count += field.matches(uri).count();
            *field = field.replace(uri, note);
        }
    };
    for span in spans.iter_mut() {
        let span_project = span.project_id.as_deref().unwrap_or(DEFAULT_PROJECT_ID);
        for (project_id, uri) in skipped {
            // Only the project whose quota was exceeded. The same content-addressed URI can be stored
            // in another project, and rewriting it there would replace a working reference with a note.
            if project_id != span_project {
                continue;
            }
            // Says only what is known. This set holds two kinds - content rejected for quota, and a
            // reference whose bytes this project does not have - and naming quota for both told readers
            // something that may be untrue of their case.
            let note = format!(
                "[content not stored ({})]",
                crate::utils::file_uri::parse_file_uri(uri)
                    .and_then(|parsed| parsed.media_type.map(str::to_string))
                    .unwrap_or_else(|| "unknown type".to_string())
            );
            for field in [
                span.messages.as_mut(),
                span.tool_definitions.as_mut(),
                span.raw_span.as_mut(),
                span.metadata.as_mut(),
            ]
            .into_iter()
            .flatten()
            {
                replace(field, uri, &note, &mut rewritten);
            }
        }
    }
    rewritten
}

/// A `#!B64!#` reference that arrived already formed: `(project, trace, uri)`.
///
/// The trace is carried because a reference that *is* backed by a stored file still needs an
/// association, or deleting the trace will not release it and the file outlives everything naming it.
pub(super) type IncomingReference = (String, String, String);

/// Check references that arrived already formed against storage, and reconcile them.
///
/// A `#!B64!#` reference is a claim that bytes are in this project's file store. Extraction created
/// most of them and their bytes are in this batch - but a client can also send one directly, or replay
/// one from another project, and nothing verified it. Committing an unverified reference is the same
/// user-visible corruption as committing one whose write failed.
///
/// Storage is the authority, not the metadata table: a row can survive a failed finalisation, so
/// `file_exists` would answer yes for bytes that are not there.
///
/// A reference that *is* backed gets an association, because otherwise deleting the trace will not
/// release it and the file outlives everything that named it. One that is not gets returned, for the
/// caller to replace with a note.
pub(super) async fn reconcile_incoming_references(
    incoming: &[IncomingReference],
    file_service: &Arc<FileService>,
    already_associated: &[(String, String, String)],
) -> (Vec<(String, String)>, usize, Vec<(String, String, String)>) {
    let mut unbacked = Vec::new();
    let mut failed = 0usize;
    // Associations this call created, so the batch's compensation covers a reference that arrived already
    // formed as well as one whose bytes this batch wrote. Both hold a file's reference count above zero, and
    // both belong to a span that may never commit.
    let mut created: Vec<(String, String, String)> = Vec::new();
    if incoming.is_empty() {
        return (unbacked, failed, created);
    }
    let storage = file_service.storage();
    let repo = file_service.database().repository();
    let mut checked: HashMap<(String, String), bool> = HashMap::new();
    // One association per `(project, trace, hash)` per batch, even when two references name the same content
    // through different URIs (e.g. differing MIME prefixes). Each `associate_existing_file` increments
    // `pending_writers`, and the batch resolves each association once - so a second increment for the same
    // tuple would never be decremented on one backend: SQLite's confirm loops per element (decrementing
    // twice), while PostgreSQL's `UNNEST` `IN` touches the row once (decrementing once), drifting the
    // counter apart. Associating once per tuple keeps increment and resolution matched, and is the correct
    // model regardless: one batch is one referencing writer.
    // Seeded with the associations the *file-write* path already made this batch, so a reference naming
    // content this batch just stored does not increment `pending_writers` a second time.
    //
    // `prepare_batch` exempts an incoming reference only when the whole URI matches one it generated -
    // correct for the unbacked-rewrite it also has to do - so a URI carrying the same hash under a different
    // media type (`image/jpeg::H` beside our `image/png::H`) still arrives here. Associating it again made
    // two increments for one `(project, trace, hash)`, while the batch resolves each association once: the
    // surplus increment was never decremented, so the row stayed non-durable at `pending_writers = 1` and
    // held the file's quota permanently. Deduping the merged list afterwards did not fix that - it removed
    // the second *resolution*, not the second increment. One batch is one referencing writer, enforced where
    // the increment happens.
    let mut associated: std::collections::HashSet<(String, String, String)> =
        already_associated.iter().cloned().collect();

    for (project_id, trace_id, uri) in incoming {
        let Some(parsed) = crate::utils::file_uri::parse_file_uri(uri) else {
            continue;
        };
        let key = (project_id.clone(), parsed.hash.to_string());
        let exists = match checked.get(&key) {
            Some(known) => *known,
            None => match storage.exists(project_id, parsed.hash).await {
                Ok(found) => {
                    checked.insert(key, found);
                    found
                }
                // A storage error is not an answer. Reading it as "not found" replaced a *valid*
                // reference with a permanent note over a transient failure - the reference cannot be
                // recovered afterwards, while the batch can be retried.
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        project_id,
                        hash = parsed.hash,
                        "Could not determine whether an incoming file reference is backed"
                    );
                    failed += 1;
                    continue;
                }
            },
        };

        if !exists {
            tracing::warn!(
                project_id,
                hash = parsed.hash,
                "Incoming file reference names content this project does not store"
            );
            unbacked.push((project_id.clone(), uri.clone()));
            continue;
        }

        // Already associated this batch through another URI: the reference is backed (checked above), so
        // nothing more is owed for it - associating again would double-count the writer.
        if !associated.insert((
            project_id.clone(),
            trace_id.clone(),
            parsed.hash.to_string(),
        )) {
            continue;
        }

        // The association is made in one operation that requires the metadata row to exist already.
        //
        // Checking for the row and *then* associating is a race - the row can vanish in between - and
        // `associate_file` would have created one with size 0, undercounting the project's quota forever.
        // Neither the size nor the media type is a fact this process has for a reference it did not
        // produce, so it does not supply them. A file that is missing, or claimed for deletion, refuses:
        // both mean the reference must not be committed.
        match repo
            .associate_existing_file(trace_id, project_id, parsed.hash)
            .await
        {
            Ok(true) => created.push((
                project_id.clone(),
                trace_id.clone(),
                parsed.hash.to_string(),
            )),
            // Bytes without metadata, or a file mid-deletion. Not something to resolve either way here:
            // rewriting would be wrong if it is only being deleted, and committing would be wrong if the
            // bytes are about to go.
            Ok(false) => {
                failed += 1;
                tracing::warn!(
                    project_id,
                    hash = parsed.hash,
                    "Incoming file reference has no metadata or is being deleted"
                );
            }
            // Without the association, deleting this trace never releases the file and it outlives
            // everything that named it. A leak is not something to commit and move on from.
            Err(e) => {
                failed += 1;
                tracing::warn!(
                    error = %e,
                    project_id,
                    trace_id,
                    hash = parsed.hash,
                    "Failed to associate an incoming file reference with its trace"
                );
            }
        }
    }

    unbacked.sort_unstable();
    unbacked.dedup();
    (unbacked, failed, created)
}

/// Persist extracted files to storage (I/O phase).
///
/// Handles temp file writes, metadata upserts, and finalization.
/// Deduplicates by hash within the batch to avoid redundant I/O.
/// Called in parallel with DuckDB write via tokio::join!.
pub(super) async fn persist_extracted_files(
    files: Vec<PendingFileWrite>,
    file_service: &Arc<FileService>,
) -> FilePersistOutcome {
    let mut outcome = FilePersistOutcome::default();
    if files.is_empty() {
        return outcome;
    }

    // Phase 1: Quota filtering
    let (files, project_sizes, skipped) = filter_over_quota(files, file_service).await;
    outcome.quota_skipped = skipped;
    if files.is_empty() {
        return outcome;
    }

    // Phase 2: Decode, write, and record files
    let (pending_finalizations, write_failures, created_associations) =
        write_and_record_files(&files, file_service).await;
    outcome.created_associations = created_associations;

    // Phase 3: Finalize temp files to permanent storage
    let finalize_failures =
        finalize_pending_files(pending_finalizations, file_service, "batch").await;
    outcome.failed = write_failures + finalize_failures;

    // Phase 4: Invalidate quota cache for affected projects
    let affected_projects: Vec<&str> = project_sizes.keys().map(|s| s.as_str()).collect();
    file_service
        .invalidate_quota_cache(&affected_projects)
        .await;

    outcome
}

/// Filter out files from projects that exceed storage quota.
/// Returns filtered files and project size map (for later cache invalidation).
async fn filter_over_quota(
    files: Vec<PendingFileWrite>,
    file_service: &Arc<FileService>,
) -> (
    Vec<PendingFileWrite>,
    HashMap<String, usize>,
    Vec<(String, String)>,
) {
    // Conservative estimate: may over-count when duplicates exist within the batch,
    // but actual dedup happens downstream in write_and_record_files and at the DB layer.
    // Once per distinct file, not once per occurrence. A batch where one image appears in forty spans
    // asked for forty times the space it needs, so a duplicate-heavy batch could be rejected for
    // storage it was never going to use. Whether the file is *already* stored is not visible here, so
    // this is still an over-estimate - but bounded by distinct content rather than by span count.
    let mut project_sizes: HashMap<String, usize> = HashMap::new();
    let mut counted: HashSet<(String, String)> = HashSet::new();
    for f in &files {
        if counted.insert((f.project_id.clone(), f.hash.clone())) {
            *project_sizes.entry(f.project_id.clone()).or_default() += f.size;
        }
    }

    let mut over_quota: HashSet<String> = HashSet::new();
    for (project_id, pending_size) in &project_sizes {
        match file_service
            .check_quota(project_id, *pending_size as i64)
            .await
        {
            Ok(false) => {
                over_quota.insert(project_id.clone());
                tracing::warn!(
                    project_id,
                    pending_bytes = pending_size,
                    "File quota exceeded, skipping file persistence"
                );
            }
            Err(e) => {
                tracing::warn!(
                    project_id,
                    error = %e,
                    "Quota check failed, allowing persistence"
                );
            }
            _ => {}
        }
    }

    // What is being dropped, so the rows that reference it can be corrected rather than committed
    // pointing at a file that will never exist. Carries the project, because a batch spans projects:
    // keyed on the URI alone, one project going over quota rewrote the *same* URI in another project's
    // spans, where the file had been stored perfectly well.
    let mut skipped: Vec<(String, String)> = files
        .iter()
        .filter(|f| over_quota.contains(&f.project_id))
        .map(|f| {
            (
                f.project_id.clone(),
                crate::utils::file_uri::build_file_uri(&f.hash, f.media_type.as_deref()),
            )
        })
        .collect();
    skipped.sort_unstable();
    skipped.dedup();

    let filtered: Vec<_> = files
        .into_iter()
        .filter(|f| !over_quota.contains(&f.project_id))
        .collect();
    (filtered, project_sizes, skipped)
}

/// Decode base64, write temp files, upsert metadata, and record trace associations.
async fn write_and_record_files(
    files: &[PendingFileWrite],
    file_service: &Arc<FileService>,
) -> (
    Vec<PendingFinalization>,
    usize,
    Vec<(String, String, String)>,
) {
    let temp_dir = file_service.temp_dir();
    let repo = file_service.database().repository();
    let mut pending_finalizations: Vec<PendingFinalization> = Vec::new();

    // Deduplicate: only write/upsert once per unique (project_id, hash)
    let mut written_hashes: HashSet<String> = HashSet::new();
    // Deduplicate: once per unique (project, trace, hash).
    //
    // Keyed without the project this suppressed one project's association whenever another project in
    // the same batch had already claimed the same trace id and hash - the same collision the database
    // primary key had, and widening only the key would have left this one in place.
    let mut trace_hashes: HashSet<(String, String, String)> = HashSet::new();
    // Anything that did not reach storage: the row that references it must not be committed.
    let mut failures = 0usize;
    // Associations this batch created, so a failed analytics write can release exactly its own.
    let mut created: Vec<(String, String, String)> = Vec::new();

    let mut cache_hits = 0usize;
    let mut fresh_ok = 0usize;

    // Process files with data (cache misses) before empty-data entries (cache hits).
    // When concurrent extraction threads race on the shared FileExtractionCache,
    // the thread that runs first gets a cache miss (with data) while later threads
    // get hits (empty data). Since results are collected in request order (not
    // execution order), a cache-hit entry may appear before the data-bearing entry
    // for the same file. Processing data-first ensures the fresh entry always wins
    // dedup, preventing files from being silently skipped.
    for file in files
        .iter()
        .filter(|f| !f.data.is_empty())
        .chain(files.iter().filter(|f| f.data.is_empty()))
    {
        let dedup_key = format!("{}:{}", file.project_id, file.hash);
        let is_new = written_hashes.insert(dedup_key);

        if is_new {
            if file.data.is_empty() {
                // Unreachable by design, and a failure rather than an assumption. This branch used to
                // mean "cache hit, so the bytes are already in storage" - a claim the extraction cache
                // could not support, since it is global and keyed on the source text. Cache hits now
                // carry their bytes, so no data means a producer built a `PendingFileWrite` without
                // any, and writing a reference to nothing is exactly what must not happen.
                cache_hits += 1;
                failures += 1;
                tracing::error!(
                    hash = %file.hash,
                    project_id = %file.project_id,
                    "Pending file has no data; refusing to record a reference to it"
                );
                // And *stop*. Counting the failure while falling through to `associate_file` still
                // recorded the association and the reference - the exact thing being refused.
                continue;
            } else {
                // Fresh extraction: decode base64, write temp file, upsert metadata, finalize
                let decoded = match BASE64_STANDARD.decode(&file.data) {
                    Ok(d) => d,
                    Err(e1) => match BASE64_URL_SAFE.decode(&file.data) {
                        Ok(d) => d,
                        Err(e) => {
                            failures += 1;
                            tracing::warn!(
                                error = %e,
                                std_error = %e1,
                                hash = %file.hash,
                                data_len = file.data.len(),
                                project_id = %file.project_id,
                                "Failed to decode base64, skipping file"
                            );
                            continue;
                        }
                    },
                };

                let temp_path = temp_dir.join(format!("{}_{}", file.project_id, file.hash));
                if let Err(e) = tokio::fs::write(&temp_path, &decoded).await {
                    failures += 1;
                    tracing::warn!(
                        error = %e,
                        hash = %file.hash,
                        project_id = %file.project_id,
                        "Failed to write temp file, skipping"
                    );
                    continue;
                }

                // Metadata and the reference count are recorded below, once per trace association.
                fresh_ok += 1;
                pending_finalizations.push(PendingFinalization {
                    project_id: file.project_id.clone(),
                    hash: file.hash.clone(),
                    temp_path,
                });
            }
        } else {
            // Duplicate within batch, already handled above
        }

        // One reference count per trace association, because that is what deletion decrements.
        //
        // `upsert_file` increments `ref_count`, and it used to be called once per batch-unique
        // `(project, hash)` while associations are per `(trace, hash)`. So two traces of one batch
        // sharing a file got two associations and *one* count - and deleting either trace decremented
        // it to zero, deleting a file the other trace still referenced. The invariant is `ref_count`
        // equals the number of associations; incrementing here, beside the association, is what keeps
        // it. The storage write above stays once per `(project, hash)`: the bytes are the same bytes.
        let trace_key = (
            file.project_id.clone(),
            file.trace_id.clone(),
            file.hash.clone(),
        );
        if trace_hashes.insert(trace_key.clone()) {
            match repo
                .associate_file(
                    &file.trace_id,
                    &file.project_id,
                    &file.hash,
                    file.media_type.as_deref(),
                    file.size as i64,
                    &file.hash_algo,
                )
                .await
            {
                // Every reference is remembered, not only a newly-created row. Each one incremented the
                // association's in-flight writer count, so each must be resolved by exactly one confirm (on
                // success) or release (on failure) - a batch that merely *shares* an existing association is
                // now one of its owners, and the release only deletes a non-durable row once the last owner
                // is gone, so tracking a shared one can no longer orphan the batch that created it.
                Ok(true) => created.push(trace_key),
                Ok(false) => {}
                Err(e) => {
                    failures += 1;
                    tracing::warn!(
                        error = %e,
                        hash = %file.hash,
                        trace_id = %file.trace_id,
                        "Failed to associate file with trace"
                    );
                }
            }
        }
    }

    tracing::debug!(
        total = files.len(),
        unique = written_hashes.len(),
        fresh = fresh_ok,
        cached = cache_hits,
        pending = pending_finalizations.len(),
        "File write summary"
    );

    (pending_finalizations, failures, created)
}

// ============================================================================
// DUCKDB WRITES
// ============================================================================

/// Write batch to analytics backend with exponential backoff retry.
///
/// Each attempt clones spans (insert_spans consumes ownership for spawn_blocking).
/// With pre-serialized String fields, clone cost is ~microseconds of memcpy, negligible
/// compared to the DuckDB write which takes milliseconds-to-seconds.
pub(super) async fn write_to_duckdb(
    spans: Vec<NormalizedSpan>,
    analytics: &Arc<AnalyticsService>,
) -> bool {
    let span_count = spans.len();
    let repo = analytics.repository();

    let result = retry_with_backoff_async(DEFAULT_MAX_ATTEMPTS, DEFAULT_BASE_DELAY_MS, || {
        repo.insert_spans(spans.clone())
    })
    .await;

    match result {
        Ok(attempts) => {
            if attempts > 1 {
                tracing::trace!(
                    spans = span_count,
                    attempts,
                    "Wrote traces to analytics backend after retry"
                );
            } else {
                tracing::trace!(spans = span_count, "Wrote traces to analytics backend");
            }
            true
        }
        Err((e, attempts)) => {
            tracing::error!(
                error = %e,
                spans = span_count,
                attempts,
                "Failed to write spans to analytics backend after retries"
            );
            false
        }
    }
}

// ============================================================================
// FLATTEN TO DB FORMAT
// ============================================================================

/// Flatten raw messages, tool definitions, tool names, and enrichments into DB-ready format.
///
/// Iterates request in same order as normalize_batch to match spans with OTLP data.
/// Builds raw span JSON directly from request (no lookup needed).
///
/// When `files_enabled`, extracts base64 files from raw_span, tool_definitions, and
/// metadata JSON values **in-memory before serialization**. This eliminates the
/// serialize→deserialize→re-serialize round-trip that previously happened in
/// `extract_files_cpu_raw_spans`.
///
/// # Panics
/// Debug assertion fails if span counts don't match (indicates pipeline bug).
#[allow(clippy::too_many_arguments)]
fn flatten(
    request: &ExportTraceServiceRequest,
    span_data: Vec<SpanData>,
    messages: Vec<Vec<RawMessage>>,
    tool_definitions: Vec<Vec<RawToolDefinition>>,
    tool_names: Vec<Vec<RawToolNames>>,
    enrichments: Vec<SpanEnrichment>,
    files_enabled: bool,
    file_cache: Option<&FileExtractionCache>,
) -> (Vec<NormalizedSpan>, Vec<PendingFileWrite>) {
    let span_count = span_data.len();
    let mut result = Vec::with_capacity(span_count);
    let mut pending_files: Vec<PendingFileWrite> = Vec::new();
    let mut iter = span_data
        .into_iter()
        .zip(messages)
        .zip(tool_definitions)
        .zip(tool_names)
        .zip(enrichments);

    let extract_fn = |json: &mut JsonValue| match file_cache {
        Some(c) => extract_and_replace_files_cached(json, c),
        None => extract_and_replace_files(json),
    };

    // Iterate request in same order as normalize_batch
    for resource_spans in &request.resource_spans {
        let resource_attrs = resource_spans
            .resource
            .as_ref()
            .map(|r| extract_attributes(&r.attributes))
            .unwrap_or_default();

        for scope_spans in &resource_spans.scope_spans {
            for otlp_span in &scope_spans.spans {
                if let Some(((((mut span, msgs), tools), tnames), enrichment)) = iter.next() {
                    let messages_str =
                        Some(serde_json::to_string(&msgs).expect("JsonValue is always valid JSON"));

                    let mut tool_definitions_json = flatten_tool_definitions(&tools);
                    let tool_names_json = flatten_tool_names(&tnames);
                    let tool_names_str = Some(
                        serde_json::to_string(&tool_names_json)
                            .expect("JsonValue is always valid JSON"),
                    );

                    let mut raw_span_json = build_raw_span_json(otlp_span, &resource_attrs);

                    // Extract files from JSON values in-memory BEFORE serialization.
                    // This avoids the costly serialize→deserialize→re-serialize round-trip.
                    if files_enabled {
                        let project_id = span
                            .project_id
                            .as_deref()
                            .unwrap_or(DEFAULT_PROJECT_ID)
                            .to_string();
                        let trace_id = &span.trace_id;

                        // raw_span
                        pending_files.extend(to_pending_files(
                            extract_fn(&mut raw_span_json).files,
                            &project_id,
                            trace_id,
                        ));

                        // tool_definitions
                        pending_files.extend(to_pending_files(
                            extract_fn(&mut tool_definitions_json).files,
                            &project_id,
                            trace_id,
                        ));

                        // metadata (owned, mutate in place before serialization)
                        pending_files.extend(to_pending_files(
                            extract_fn(&mut span.metadata).files,
                            &project_id,
                            trace_id,
                        ));
                    }

                    // Serialize to strings ONCE (after file extraction)
                    let tool_definitions_str = Some(
                        serde_json::to_string(&tool_definitions_json)
                            .expect("JsonValue is always valid JSON"),
                    );
                    let raw_span_str = Some(
                        serde_json::to_string(&raw_span_json)
                            .expect("JsonValue is always valid JSON"),
                    );

                    result.push(to_normalized_span(
                        span,
                        &enrichment,
                        messages_str,
                        tool_definitions_str,
                        tool_names_str,
                        raw_span_str,
                    ));
                }
            }
        }
    }

    debug_assert_eq!(
        result.len(),
        span_count,
        "Span count mismatch: expected {}, got {}",
        span_count,
        result.len()
    );

    (result, pending_files)
}

/// Flatten tool definitions: extract content from each RawToolDefinition and merge arrays.
/// Input: `[{source: {...}, content: [def1, def2]}, {source: {...}, content: [def3]}]`
/// Output: `[def1, def2, def3]`
fn flatten_tool_definitions(tools: &[RawToolDefinition]) -> JsonValue {
    let mut result: Vec<JsonValue> = Vec::new();
    for tool in tools {
        if let Some(arr) = tool.content.as_array() {
            result.extend(arr.iter().cloned());
        } else {
            result.push(tool.content.clone());
        }
    }
    JsonValue::Array(result)
}

/// Flatten tool names: extract content from each RawToolNames and merge into flat string list.
/// Input: `[{source: {...}, content: ["tool1", "tool2"]}, {source: {...}, content: ["tool3"]}]`
/// Output: `["tool1", "tool2", "tool3"]`
fn flatten_tool_names(tnames: &[RawToolNames]) -> JsonValue {
    let mut result: Vec<JsonValue> = Vec::new();
    for tname in tnames {
        if let Some(arr) = tname.content.as_array() {
            result.extend(arr.iter().cloned());
        } else {
            result.push(tname.content.clone());
        }
    }
    JsonValue::Array(result)
}

/// Convert SpanData to NormalizedSpan and apply enrichment.
fn to_normalized_span(
    span: SpanData,
    enrichment: &SpanEnrichment,
    messages: Option<String>,
    tool_definitions: Option<String>,
    tool_names: Option<String>,
    raw_span: Option<String>,
) -> NormalizedSpan {
    NormalizedSpan {
        // Identity
        //
        // Defaulted here rather than left as it arrived, so the stored value is the one every other
        // path uses. `NULL` was a third state nothing agreed on: project-scoped reads (`WHERE
        // project_id = ?`) could not see such a row, `delete_project_data` could not delete it, and the
        // write fence checked it as `default` - so a span with no project id was stored where nothing
        // could read it and nothing could remove it.
        project_id: Some(
            span.project_id
                .unwrap_or_else(|| DEFAULT_PROJECT_ID.to_string()),
        ),
        trace_id: span.trace_id,
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        trace_state: span.trace_state,

        // Session and user
        session_id: span.session_id,
        user_id: span.user_id,

        // Naming and classification
        span_name: span.span_name,
        span_kind: span.span_kind,
        span_category: span.span_category,
        observation_type: span.observation_type,
        framework: span.framework,
        scope_name: span.scope_name.clone(),
        scope_version: span.scope_version.clone(),
        status_code: span.status_code,
        status_message: span.status_message,
        exception_type: span.exception_type,
        exception_message: span.exception_message,
        exception_stacktrace: span.exception_stacktrace,

        // Time
        timestamp_start: span.timestamp_start,
        timestamp_end: span.timestamp_end,
        duration_ms: span.duration_ms,

        // Environment
        environment: span.environment,

        // GenAI core fields
        gen_ai_system: span.gen_ai_system,
        gen_ai_operation_name: span.gen_ai_operation_name,
        gen_ai_request_model: span.gen_ai_request_model,
        gen_ai_response_model: span.gen_ai_response_model,
        gen_ai_response_id: span.gen_ai_response_id,

        // GenAI request parameters
        gen_ai_temperature: span.gen_ai_temperature,
        gen_ai_top_p: span.gen_ai_top_p,
        gen_ai_top_k: span.gen_ai_top_k,
        gen_ai_max_tokens: span.gen_ai_max_tokens,
        gen_ai_frequency_penalty: span.gen_ai_frequency_penalty,
        gen_ai_presence_penalty: span.gen_ai_presence_penalty,
        gen_ai_stop_sequences: span.gen_ai_stop_sequences,

        // GenAI response
        gen_ai_finish_reasons: span.gen_ai_finish_reasons,

        // GenAI agent fields
        gen_ai_agent_id: span.gen_ai_agent_id,
        gen_ai_agent_name: span.gen_ai_agent_name,

        // GenAI tool fields
        gen_ai_tool_name: span.gen_ai_tool_name,
        gen_ai_tool_call_id: span.gen_ai_tool_call_id,

        // GenAI performance metrics
        gen_ai_server_ttft_ms: span.gen_ai_server_ttft_ms,
        gen_ai_server_request_duration_ms: span.gen_ai_server_request_duration_ms,

        // Token usage
        gen_ai_usage_input_tokens: span.gen_ai_usage_input_tokens,
        gen_ai_usage_output_tokens: span.gen_ai_usage_output_tokens,
        // The extractor's total, unless pricing resolved a provider and so knows which convention applies -
        // see `enrich::corrected_total_tokens`. Without this the charge and the total could describe
        // different calls whenever `gen_ai.system` was absent or spelled in a way the mapper did not know.
        gen_ai_usage_total_tokens: enrichment
            .total_tokens
            .unwrap_or(span.gen_ai_usage_total_tokens),
        gen_ai_usage_cache_read_tokens: span.gen_ai_usage_cache_read_tokens,
        gen_ai_usage_cache_write_tokens: span.gen_ai_usage_cache_write_tokens,
        gen_ai_usage_reasoning_tokens: span.gen_ai_usage_reasoning_tokens,
        gen_ai_usage_details: json_to_pre_serialized(&span.gen_ai_usage_details),

        // Enrichment data (costs)
        gen_ai_cost_input: enrichment.input_cost,
        gen_ai_cost_output: enrichment.output_cost,
        gen_ai_cost_cache_read: enrichment.cache_read_cost,
        gen_ai_cost_cache_write: enrichment.cache_write_cost,
        gen_ai_cost_reasoning: enrichment.reasoning_cost,
        gen_ai_cost_total: enrichment.total_cost,

        // Enrichment data (previews)
        input_preview: enrichment.input_preview.clone(),
        output_preview: enrichment.output_preview.clone(),

        // External services
        http_method: span.http_method,
        http_url: span.http_url,
        http_status_code: span.http_status_code,

        db_system: span.db_system,
        db_name: span.db_name,
        db_operation: span.db_operation,
        db_statement: span.db_statement,

        storage_system: span.storage_system,
        storage_bucket: span.storage_bucket,
        storage_object: span.storage_object,

        messaging_system: span.messaging_system,
        messaging_destination: span.messaging_destination,

        // Tags and metadata
        tags: span.tags,
        metadata: json_to_pre_serialized(&span.metadata),

        // Raw messages (converted to SideML on query)
        messages,

        // Raw tool definitions (separate from conversation messages)
        tool_definitions,

        // Raw tool names (list of tool names, separate from full definitions)
        tool_names,

        // Raw span JSON (includes attributes and resource.attributes)
        raw_span,

        // Ingestion time (populated by DB default, not set during span creation)
        ingested_at: None,
    }
}

// ============================================================================
// RAW SPAN JSON BUILDING
// ============================================================================

/// Build a raw JSON representation of an OTLP span for archival.
/// Uses ordered map for better readability (identity -> timing -> content -> metadata).
fn build_raw_span_json(span: &Span, resource_attrs: &HashMap<String, String>) -> JsonValue {
    let mut map = serde_json::Map::new();

    // Identity fields first
    map.insert("trace_id".into(), json!(hex::encode(&span.trace_id)));
    map.insert("span_id".into(), json!(hex::encode(&span.span_id)));
    map.insert(
        "parent_span_id".into(),
        if span.parent_span_id.is_empty() {
            JsonValue::Null
        } else {
            json!(hex::encode(&span.parent_span_id))
        },
    );
    map.insert("name".into(), json!(&span.name));
    map.insert("kind".into(), json!(span.kind));

    // Timing
    map.insert(
        "start_time_unix_nano".into(),
        json!(span.start_time_unix_nano),
    );
    map.insert("end_time_unix_nano".into(), json!(span.end_time_unix_nano));

    // Status
    map.insert(
        "status".into(),
        span.status
            .as_ref()
            .map(|s| {
                let mut status_map = serde_json::Map::new();
                status_map.insert("code".into(), json!(s.code));
                status_map.insert("message".into(), json!(&s.message));
                JsonValue::Object(status_map)
            })
            .unwrap_or(JsonValue::Null),
    );

    // Attributes
    map.insert("attributes".into(), build_attributes_json(&span.attributes));

    // Events (with ordered fields)
    let events: Vec<JsonValue> = span
        .events
        .iter()
        .map(|e| {
            let mut event_map = serde_json::Map::new();
            event_map.insert("name".into(), json!(&e.name));
            event_map.insert("timestamp".into(), json!(nanos_to_iso(e.time_unix_nano)));
            event_map.insert("attributes".into(), build_attributes_json(&e.attributes));
            event_map.insert(
                "dropped_attributes_count".into(),
                json!(e.dropped_attributes_count),
            );
            JsonValue::Object(event_map)
        })
        .collect();
    map.insert("events".into(), json!(events));

    // Links (with ordered fields)
    let links: Vec<JsonValue> = span
        .links
        .iter()
        .map(|l| {
            let mut link_map = serde_json::Map::new();
            link_map.insert("trace_id".into(), json!(hex::encode(&l.trace_id)));
            link_map.insert("span_id".into(), json!(hex::encode(&l.span_id)));
            link_map.insert("trace_state".into(), json!(&l.trace_state));
            link_map.insert("attributes".into(), build_attributes_json(&l.attributes));
            link_map.insert("flags".into(), json!(l.flags));
            link_map.insert(
                "dropped_attributes_count".into(),
                json!(l.dropped_attributes_count),
            );
            JsonValue::Object(link_map)
        })
        .collect();
    map.insert("links".into(), json!(links));

    // Resource
    let mut resource_map = serde_json::Map::new();
    resource_map.insert(
        "attributes".into(),
        build_resource_attributes(resource_attrs),
    );
    map.insert("resource".into(), JsonValue::Object(resource_map));

    // Metadata (less important, at the end)
    map.insert(
        "trace_state".into(),
        if span.trace_state.is_empty() {
            JsonValue::Null
        } else {
            json!(&span.trace_state)
        },
    );
    map.insert("flags".into(), json!(span.flags));
    map.insert(
        "dropped_attributes_count".into(),
        json!(span.dropped_attributes_count),
    );
    map.insert(
        "dropped_events_count".into(),
        json!(span.dropped_events_count),
    );
    map.insert(
        "dropped_links_count".into(),
        json!(span.dropped_links_count),
    );

    JsonValue::Object(map)
}

/// Build JSON from attributes HashMap (for resource attributes)
fn build_resource_attributes(attrs: &HashMap<String, String>) -> JsonValue {
    let map: serde_json::Map<String, JsonValue> =
        attrs.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
    JsonValue::Object(map)
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::traces::MessageSource;
    use chrono::Utc;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};
    use serde_json::json;

    fn make_span(id: &str) -> SpanData {
        SpanData {
            project_id: Some("test-project".to_string()),
            trace_id: "trace1".to_string(),
            span_id: id.to_string(),
            span_name: format!("span-{}", id),
            timestamp_start: Utc::now(),
            ..Default::default()
        }
    }

    fn make_otlp_span(id: &str) -> Span {
        Span {
            trace_id: b"trace1__________".to_vec(),
            span_id: id.as_bytes().to_vec(),
            name: format!("span-{}", id),
            ..Default::default()
        }
    }

    fn make_request(span_count: usize) -> ExportTraceServiceRequest {
        let spans: Vec<Span> = (0..span_count)
            .map(|i| make_otlp_span(&format!("span{}", i + 1)))
            .collect();
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn make_raw_message(content: &str) -> RawMessage {
        RawMessage {
            source: MessageSource::Attribute {
                key: "test".to_string(),
                time: Utc::now(),
            },
            content: json!({
                "role": "user",
                "content": content
            }),
        }
    }

    fn make_enrichment() -> SpanEnrichment {
        SpanEnrichment::default()
    }

    #[test]
    fn test_flatten_empty() {
        let request = ExportTraceServiceRequest::default();
        let spans: Vec<SpanData> = vec![];
        let messages: Vec<Vec<RawMessage>> = vec![];
        let tool_definitions: Vec<Vec<RawToolDefinition>> = vec![];
        let tool_names: Vec<Vec<RawToolNames>> = vec![];
        let enrichments: Vec<SpanEnrichment> = vec![];
        let (result, _) = flatten(
            &request,
            spans,
            messages,
            tool_definitions,
            tool_names,
            enrichments,
            false,
            None,
        );
        assert!(result.is_empty());
    }

    #[test]
    fn test_flatten_single_span_no_messages() {
        let request = make_request(1);
        let spans = vec![make_span("span1")];
        let messages: Vec<Vec<RawMessage>> = vec![vec![]];
        let tool_definitions: Vec<Vec<RawToolDefinition>> = vec![vec![]];
        let tool_names: Vec<Vec<RawToolNames>> = vec![vec![]];
        let enrichments = vec![make_enrichment()];
        let (result, _) = flatten(
            &request,
            spans,
            messages,
            tool_definitions,
            tool_names,
            enrichments,
            false,
            None,
        );

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].span_id, "span1");
        assert_eq!(result[0].messages.as_deref().unwrap_or("[]"), "[]");
        assert_eq!(result[0].tool_definitions.as_deref().unwrap_or("[]"), "[]");
        assert_eq!(result[0].tool_names.as_deref().unwrap_or("[]"), "[]");
    }

    #[test]
    fn test_flatten_multiple_spans() {
        let request = make_request(3);
        let spans = vec![make_span("span1"), make_span("span2"), make_span("span3")];
        let messages: Vec<Vec<RawMessage>> = vec![vec![], vec![], vec![]];
        let tool_definitions: Vec<Vec<RawToolDefinition>> = vec![vec![], vec![], vec![]];
        let tool_names: Vec<Vec<RawToolNames>> = vec![vec![], vec![], vec![]];
        let enrichments = vec![make_enrichment(), make_enrichment(), make_enrichment()];
        let (result, _) = flatten(
            &request,
            spans,
            messages,
            tool_definitions,
            tool_names,
            enrichments,
            false,
            None,
        );

        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_flatten_stores_raw_messages() {
        let request = make_request(1);
        let spans = vec![make_span("span1")];
        let messages = vec![vec![
            make_raw_message("Hello"),
            make_raw_message("Hi there"),
        ]];
        let tool_definitions: Vec<Vec<RawToolDefinition>> = vec![vec![]];
        let tool_names: Vec<Vec<RawToolNames>> = vec![vec![]];
        let enrichments = vec![make_enrichment()];

        let (result, _) = flatten(
            &request,
            spans,
            messages,
            tool_definitions,
            tool_names,
            enrichments,
            false,
            None,
        );

        assert_eq!(result.len(), 1);

        // Verify messages are stored as pre-serialized JSON string
        let messages_str = result[0].messages.as_deref().unwrap();
        let stored_messages: Vec<serde_json::Value> = serde_json::from_str(messages_str).unwrap();
        assert_eq!(stored_messages.len(), 2);
        // Raw messages have "content" field with original message data
        assert_eq!(stored_messages[0]["content"]["content"], "Hello");
        assert_eq!(stored_messages[1]["content"]["content"], "Hi there");
    }

    #[test]
    fn test_flatten_applies_enrichment() {
        let request = make_request(1);
        let spans = vec![make_span("span1")];
        let messages: Vec<Vec<RawMessage>> = vec![vec![]];
        let tool_definitions: Vec<Vec<RawToolDefinition>> = vec![vec![]];
        let tool_names: Vec<Vec<RawToolNames>> = vec![vec![]];
        let enrichments = vec![SpanEnrichment {
            input_cost: 0.001,
            output_cost: 0.002,
            total_cost: 0.003,
            input_preview: Some("Hello".to_string()),
            output_preview: Some("Hi".to_string()),
            ..Default::default()
        }];

        let (result, _) = flatten(
            &request,
            spans,
            messages,
            tool_definitions,
            tool_names,
            enrichments,
            false,
            None,
        );

        assert_eq!(result[0].gen_ai_cost_input, 0.001);
        assert_eq!(result[0].gen_ai_cost_output, 0.002);
        assert_eq!(result[0].gen_ai_cost_total, 0.003);
        assert_eq!(result[0].input_preview, Some("Hello".to_string()));
        assert_eq!(result[0].output_preview, Some("Hi".to_string()));
    }

    /// A reference to a file that was rejected must not be committed as though the file exists.
    ///
    /// The failure it guards is silent: a `#!B64!#` reference to a quota-rejected file renders exactly
    /// like a reference to a corrupt one, and nothing on the span distinguishes them.
    #[test]
    fn a_rejected_file_reference_is_replaced_with_a_note() {
        let uri = crate::utils::file_uri::build_file_uri("abc123", Some("image/png"));
        let other = crate::utils::file_uri::build_file_uri("def456", Some("image/png"));
        let mut spans = vec![NormalizedSpan {
            project_id: Some("proj".to_string()),
            messages: Some(format!(r#"[{{"content":"{uri}"}}]"#)),
            tool_definitions: Some(format!(r#"[{{"icon":"{uri}"}}]"#)),
            raw_span: Some(format!(r#"{{"attr":"{other}"}}"#)),
            metadata: None,
            ..NormalizedSpan::default()
        }];

        let rewritten = note_unstored_files(&mut spans, &[("proj".to_string(), uri.clone())]);

        assert_eq!(
            rewritten, 2,
            "both references to the rejected file are rewritten"
        );
        let messages = spans[0].messages.as_deref().unwrap();
        assert!(
            !messages.contains(&uri),
            "the rejected reference is still committed: {messages}"
        );
        assert!(
            messages.contains("not stored") && messages.contains("image/png"),
            "the note must say what happened and to what: {messages}"
        );
        assert!(
            spans[0].raw_span.as_deref().unwrap().contains(&other),
            "a reference to a file that *was* stored must be left alone"
        );
    }

    /// Nothing to rewrite must cost nothing and change nothing.
    #[test]
    fn no_rejected_files_leaves_spans_untouched() {
        let uri = crate::utils::file_uri::build_file_uri("abc123", None);
        let mut spans = vec![NormalizedSpan {
            messages: Some(format!(r#"[{{"content":"{uri}"}}]"#)),
            ..NormalizedSpan::default()
        }];
        let before = spans[0].messages.clone();
        assert_eq!(note_unstored_files(&mut spans, &[]), 0);
        assert_eq!(spans[0].messages, before);
    }

    /// A hash is content-addressed, so the same URI can be rejected in one project and stored in
    /// another. Rewriting it everywhere would replace a *working* reference with a note.
    #[test]
    fn a_quota_rejection_only_rewrites_the_project_it_happened_in() {
        let uri = crate::utils::file_uri::build_file_uri("shared", Some("image/png"));
        let mut spans = vec![
            NormalizedSpan {
                project_id: Some("over-quota".to_string()),
                messages: Some(format!(r#"[{{"content":"{uri}"}}]"#)),
                ..NormalizedSpan::default()
            },
            NormalizedSpan {
                project_id: Some("healthy".to_string()),
                messages: Some(format!(r#"[{{"content":"{uri}"}}]"#)),
                ..NormalizedSpan::default()
            },
        ];

        let rewritten = note_unstored_files(&mut spans, &[("over-quota".to_string(), uri.clone())]);

        assert_eq!(
            rewritten, 1,
            "only the rejected project's reference is rewritten"
        );
        assert!(!spans[0].messages.as_deref().unwrap().contains(&uri));
        assert!(
            spans[1].messages.as_deref().unwrap().contains(&uri),
            "the other project stored this file; its reference must be left alone"
        );
    }
}
