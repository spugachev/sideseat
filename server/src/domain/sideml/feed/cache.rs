//! Memoised reconstruction, so the same rows are not normalised twice.
//!
//! Normalisation happens at query time, which is deliberate: a fix to the pipeline applies to history
//! that was ingested before it, with no re-ingestion. The cost is that every read pays for the whole
//! session, and `bench_session_scaling` says what that is - the pipeline is linear in its input at about
//! 27 MB/s, but a framework that re-sends the conversation as its next turn's input makes the *input*
//! quadratic in the turn count. A thousand-turn LangGraph session is 68 MB of telemetry and 2.6 seconds
//! of reconstruction, on every read, for an answer of two thousand blocks.
//!
//! # Why this cannot serve a stale answer
//!
//! The key is a hash of everything the pipeline reads - each row's identity and its payloads - so any
//! change to any row is a different key rather than a stale hit. There is no invalidation to get wrong
//! and no TTL to tune: a re-delivered span rewrites its row, the digest changes, and the old entry is
//! simply never asked for again.
//!
//! Process-local and empty at startup, which is the other half. A cache that outlived the binary would
//! serve reconstructions made by the *previous* version of the pipeline, silently undoing "fixes apply to
//! historical data" - and the alternative, a version constant someone must remember to bump, is a hole
//! rather than a design. A new build is a new process, so it starts from nothing.
//!
//! Keyed by the digest alone, not by the request: the same rows always reconstruct to the same blocks,
//! and callers that narrow the answer afterwards (a trace scoped out of its session, a role filter, a
//! feed page) do that to the cached result.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use moka::sync::Cache;

use super::types::FeedResult;
use crate::core::constants::{RECONSTRUCTION_CACHE_IDLE_SECS, RECONSTRUCTION_CACHE_MAX_ENTRIES};
use crate::data::types::MessageSpanRow;

/// Which reconstruction the cached answer came from.
///
/// Two callers ask the pipeline different questions of the same rows: `process_spans` builds a
/// chronological trace / session view, while `process_feed` builds the newest-first project feed.
/// Keying only on the rows made the two collide - whichever closure filled the cache first served the
/// other one's request, so a session query could receive feed ordering (one response reversed against
/// the others) or a feed could receive session ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reconstruction {
    /// `process_spans` output: chronological, forward.
    Spans,
    /// `process_feed` output: responses newest-first, forward within each.
    Feed,
}

/// A memo over the pipeline output, keyed by the content of the rows and the reconstruction mode.
#[derive(Clone)]
pub struct ReconstructionCache {
    entries: Cache<([u8; 32], Reconstruction), Arc<FeedResult>>,
}

impl Default for ReconstructionCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ReconstructionCache {
    pub fn new() -> Self {
        Self {
            entries: Cache::builder()
                .max_capacity(RECONSTRUCTION_CACHE_MAX_ENTRIES)
                .time_to_idle(Duration::from_secs(RECONSTRUCTION_CACHE_IDLE_SECS))
                .build(),
        }
    }

    /// The reconstruction of these rows, computing it only if these exact rows have not been seen.
    ///
    /// `reconstruct` takes the rows by value because the pipeline consumes them; it runs only on a miss,
    /// and only in **one** caller when several arrive together.
    ///
    /// That last part is the difference from a check-then-compute pair. A cold instance is the normal
    /// state under ephemeral scaling - every new replica starts empty, and a deploy replaces them all -
    /// so the concurrent-miss case is not an edge: eight readers arriving at a fresh replica each found
    /// no entry, each reconstructed the same session, and each paid the full cost. On a thousand-turn
    /// session that is eight simultaneous 2.3-second reconstructions of one answer. `get_with` admits
    /// one and hands the rest its result, which turns the worst case from N× the work into 1×.
    pub fn get_or_reconstruct(
        &self,
        mode: Reconstruction,
        rows: Vec<MessageSpanRow>,
        reconstruct: impl FnOnce(Vec<MessageSpanRow>) -> FeedResult,
    ) -> Arc<FeedResult> {
        self.get_or_reconstruct_grouped(mode, rows, &HashMap::new(), reconstruct)
    }

    /// As [`Self::get_or_reconstruct`], with a caller-supplied trace → session grouping.
    ///
    /// The grouping is **in the key**, because the pipeline reads it and the rule here is that the key
    /// covers everything reconstruction reads. It is not redundant with the rows: the grouping comes from
    /// the store and therefore knows about spans the content filter removed, so a contentless root span
    /// whose session changed alters the answer while leaving every row identical. Keyed, that is a
    /// different entry; unkeyed, it would be a stale hit that no invalidation could reach.
    pub fn get_or_reconstruct_grouped(
        &self,
        mode: Reconstruction,
        rows: Vec<MessageSpanRow>,
        session_of_trace: &HashMap<String, String>,
        reconstruct: impl FnOnce(Vec<MessageSpanRow>) -> FeedResult,
    ) -> Arc<FeedResult> {
        let key = (digest_with(&rows, session_of_trace), mode);
        self.entries.get_with(key, || Arc::new(reconstruct(rows)))
    }

    /// How many reconstructions are held. For tests and diagnostics.
    pub fn len(&self) -> u64 {
        self.entries.run_pending_tasks();
        self.entries.entry_count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A hash of everything reconstruction reads from these rows.
///
/// Hashing the payloads rather than a cheap proxy like `ingested_at`, because a proxy is a guess about
/// when content changes and this is the one place that must not guess. It is also affordable by a wide
/// margin: BLAKE3 runs at gigabytes per second, so digesting the 68 MB that a thousand-turn replaying
/// session carries costs tens of milliseconds against the 2.6 seconds it saves.
///
/// Order matters and is included: the pipeline sorts internally, but two different row orders are two
/// different inputs as far as this memo is concerned, and treating them as one would be a claim about the
/// pipeline that this file has no business making.
fn digest_with(rows: &[MessageSpanRow], session_of_trace: &HashMap<String, String>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();

    // Sorted, so the same grouping hashes the same however the map iterated. Length-prefixed, so
    // `("ab","c")` and `("a","bc")` cannot produce the same bytes.
    let mut grouping: Vec<(&str, &str)> = session_of_trace
        .iter()
        .map(|(t, s)| (t.as_str(), s.as_str()))
        .collect();
    grouping.sort_unstable();
    hasher.update(&(grouping.len() as u64).to_le_bytes());
    for (trace, session) in grouping {
        hasher.update(&(trace.len() as u64).to_le_bytes());
        hasher.update(trace.as_bytes());
        hasher.update(&(session.len() as u64).to_le_bytes());
        hasher.update(session.as_bytes());
    }

    hasher.update(&(rows.len() as u64).to_le_bytes());
    for row in rows {
        // Present-or-absent is hashed as well as the value. Mapping `None` to `""` made them the same
        // input, and an empty attribute is accepted - so a span whose `status_code` went from absent to
        // empty (or the reverse) shared a key with its old self, and a warm instance would answer with the
        // field missing where a cold one reconstructs it as present-and-empty.
        for field in [
            Some(row.trace_id.as_str()),
            Some(row.span_id.as_str()),
            row.parent_span_id.as_deref(),
            row.session_id.as_deref(),
            Some(row.messages_json.as_str()),
            Some(row.tool_definitions_json.as_str()),
            Some(row.tool_names_json.as_str()),
            row.model.as_deref(),
            row.provider.as_deref(),
            row.status_code.as_deref(),
            row.exception_type.as_deref(),
            row.exception_message.as_deref(),
            row.exception_stacktrace.as_deref(),
            row.observation_type.as_deref(),
            row.scope_name.as_deref(),
            row.scope_version.as_deref(),
            row.span_name.as_deref(),
            row.framework.as_deref(),
            row.response_model.as_deref(),
            row.response_id.as_deref(),
            row.finish_reasons.as_deref(),
        ] {
            match field {
                // Length-prefixed, so `("ab", "c")` and `("a", "bc")` are different inputs.
                Some(value) => {
                    hasher.update(&[1u8]);
                    hasher.update(&(value.len() as u64).to_le_bytes());
                    hasher.update(value.as_bytes());
                }
                None => {
                    hasher.update(&[0u8]);
                }
            }
        }
        hasher.update(&row.span_timestamp.timestamp_micros().to_le_bytes());
        hasher.update(
            &row.span_end_timestamp
                .map(|t| t.timestamp_micros())
                .unwrap_or(i64::MIN)
                .to_le_bytes(),
        );
        // `ingested_at` is read, not merely stored: `group_and_sort_traces` breaks a tie between two
        // traces with the same earliest span timestamp by the earliest ingestion time, and that decides
        // which trace's history the other one strips against. Two row sets differing only here are
        // genuinely different inputs.
        hasher.update(&row.ingested_at.timestamp_micros().to_le_bytes());
        hasher.update(&row.input_tokens.to_le_bytes());
        hasher.update(&row.output_tokens.to_le_bytes());
        hasher.update(&row.total_tokens.to_le_bytes());
        hasher.update(&row.cost_total.to_le_bytes());
        // The envelope scalars. They do not change reconstruction itself, but they change the
        // *response* - a corrected temperature or a re-priced cost split must not be served from a
        // stale entry under an unchanged key, which is exactly the widening rule the design record
        // states: a field that affects the answer must reach the digest.
        hasher.update(&row.temperature.unwrap_or(f64::MIN).to_le_bytes());
        hasher.update(&row.top_p.unwrap_or(f64::MIN).to_le_bytes());
        hasher.update(&row.max_tokens.unwrap_or(i64::MIN).to_le_bytes());
        hasher.update(&row.cache_read_tokens.to_le_bytes());
        hasher.update(&row.cache_write_tokens.to_le_bytes());
        hasher.update(&row.reasoning_tokens.to_le_bytes());
        hasher.update(&row.cost_input.to_le_bytes());
        hasher.update(&row.cost_output.to_le_bytes());
    }
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn row(span: &str, messages: &str) -> MessageSpanRow {
        let t = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).single().unwrap();
        MessageSpanRow {
            trace_id: "trace-1".to_string(),
            span_id: span.to_string(),
            parent_span_id: None,
            span_timestamp: t,
            span_end_timestamp: Some(t),
            messages_json: messages.to_string(),
            tool_definitions_json: "[]".to_string(),
            tool_names_json: "[]".to_string(),
            model: None,
            provider: None,
            status_code: None,
            exception_type: None,
            exception_message: None,
            exception_stacktrace: None,
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            cost_total: 0.0,
            observation_type: Some("generation".to_string()),
            session_id: None,
            ingested_at: t,
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
        }
    }

    /// Every field of a row reaches the digest.
    ///
    /// The module's claim is that a stale hit is impossible because the key covers everything
    /// reconstruction reads. That holds only while the digest and `MessageSpanRow` agree, and a field added
    /// to the row compiles, passes every behavioural test here, and is silently outside the key - which is
    /// the one failure this design exists to make impossible: an answer built from a value the key does not
    /// mention, with no invalidation that could ever reach it.
    ///
    /// Read from the source text, because there is no way to enumerate a struct's fields at runtime. A field
    /// the pipeline genuinely does not read still has to be hashed or explicitly excused here, which is the
    /// right default: hashing a field that turns out to be irrelevant costs a cache miss, and omitting one
    /// that is relevant costs a wrong answer.
    #[test]
    fn every_field_of_a_row_reaches_the_digest() {
        let types = include_str!("../../../data/types/messages.rs");
        let start = types
            .find("pub struct MessageSpanRow {")
            .expect("the row struct");
        let body = &types[start..start + types[start..].find("\n}").expect("its end")];

        let fields: Vec<&str> = body
            .lines()
            .map(str::trim)
            .filter(|line| line.starts_with("pub ") && line.contains(':'))
            .filter_map(|line| line.strip_prefix("pub "))
            .filter_map(|rest| rest.split(':').next())
            .map(str::trim)
            .collect();
        assert!(
            fields.len() > 15,
            "the struct was not parsed: {fields:?} - if its shape changed, fix this test rather than \
             letting it pass vacuously"
        );

        let digest = {
            let source = include_str!("cache.rs");
            let from = source.find("fn digest_with(").expect("the digest");
            let to = from
                + source[from..]
                    .find("\n#[cfg(test)]")
                    .unwrap_or(source.len() - from);
            &source[from..to]
        };
        for field in fields {
            assert!(
                digest.contains(&format!("row.{field}")),
                "`{field}` is a field of MessageSpanRow that the reconstruction digest does not hash, so \
                 two row sets differing only in it share a cache entry"
            );
        }
    }

    /// The same rows are reconstructed once; different rows are not confused for them.
    #[test]
    fn a_hit_needs_the_same_rows_and_a_change_of_any_kind_misses() {
        let cache = ReconstructionCache::new();
        let runs = std::cell::Cell::new(0);
        let reconstruct = |_rows: Vec<MessageSpanRow>| {
            runs.set(runs.get() + 1);
            FeedResult::default()
        };

        let rows = vec![row("s1", "[]")];
        cache.get_or_reconstruct(Reconstruction::Spans, rows.clone(), reconstruct);
        cache.get_or_reconstruct(Reconstruction::Spans, rows.clone(), reconstruct);
        assert_eq!(runs.get(), 1, "the second read of identical rows is a hit");

        // A changed payload is a different input, so it must not read the first answer.
        cache.get_or_reconstruct(Reconstruction::Spans, vec![row("s1", "[{}]")], reconstruct);
        assert_eq!(runs.get(), 2, "a payload change misses");

        // So is an added row, and so is a different span carrying the same payload.
        cache.get_or_reconstruct(
            Reconstruction::Spans,
            vec![row("s1", "[]"), row("s2", "[]")],
            reconstruct,
        );
        assert_eq!(runs.get(), 3, "an added row misses");
        cache.get_or_reconstruct(Reconstruction::Spans, vec![row("s2", "[]")], reconstruct);
        assert_eq!(runs.get(), 4, "a different span misses");

        // And the order of the rows is part of the input.
        cache.get_or_reconstruct(
            Reconstruction::Spans,
            vec![row("s2", "[]"), row("s1", "[]")],
            reconstruct,
        );
        assert_eq!(runs.get(), 5, "a different order misses");
    }

    /// Two reconstruction modes of the same rows are cached separately.
    ///
    /// The defect this pins: keying only on the rows meant a session request and a feed request of the
    /// same telemetry shared a slot, and whichever closure filled it first served the other. A session
    /// then received newest-first-by-response ordering; a feed received chronological. Neither read
    /// showed anything obviously wrong - it was the ordering of what it showed that was another view's.
    #[test]
    fn two_reconstruction_modes_do_not_share_a_slot() {
        let cache = ReconstructionCache::new();
        let spans_calls = std::cell::Cell::new(0);
        let feed_calls = std::cell::Cell::new(0);
        let rows = vec![row("s1", "[]")];

        // Distinct answers per mode, so a slot collision reads as the wrong count of blocks - which is
        // what a session receiving feed ordering looks like from the outside.
        let spans = cache.get_or_reconstruct(Reconstruction::Spans, rows.clone(), |_| {
            spans_calls.set(spans_calls.get() + 1);
            let mut r = FeedResult::default();
            r.tool_names.push("spans".to_string());
            r
        });
        let feed = cache.get_or_reconstruct(Reconstruction::Feed, rows.clone(), |_| {
            feed_calls.set(feed_calls.get() + 1);
            let mut r = FeedResult::default();
            r.tool_names.push("feed-a".to_string());
            r.tool_names.push("feed-b".to_string());
            r
        });

        assert_eq!(spans.tool_names.len(), 1, "spans mode gets its own answer");
        assert_eq!(feed.tool_names.len(), 2, "feed mode gets its own answer");
        assert_eq!(spans_calls.get(), 1);
        assert_eq!(feed_calls.get(), 1);

        // The second Spans read is a hit for Spans, not for whichever mode filled first.
        let repeat = cache.get_or_reconstruct(Reconstruction::Spans, rows, |_| {
            spans_calls.set(spans_calls.get() + 1);
            FeedResult::default()
        });
        assert_eq!(spans_calls.get(), 1, "repeat did not re-run");
        assert_eq!(repeat.tool_names.len(), 1, "and returned the spans answer");
    }

    /// Concurrent readers of a cold cache reconstruct once, not once each.
    ///
    /// The ephemeral-scaling case, and the one a check-then-compute pair gets wrong: a replica starts
    /// empty, so every reader of a session that nobody has asked for yet is a miss. With eight arriving
    /// together the old form did the same expensive work eight times and inserted the same answer eight
    /// times. The reconstruction here blocks until every thread has arrived, so the test fails by
    /// *timing out* if the calls do not overlap - a sequential implementation would satisfy a plain
    /// "ran once" assertion trivially.
    #[test]
    fn concurrent_readers_of_a_cold_cache_reconstruct_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc as StdArc, Barrier};

        const READERS: usize = 8;
        let cache = ReconstructionCache::new();
        let runs = StdArc::new(AtomicUsize::new(0));
        // One fewer than the reader count: the thread that wins the race is inside `reconstruct` and
        // will never reach the barrier, so the others must be waiting *for it* rather than computing.
        let arrived = StdArc::new(Barrier::new(READERS));
        let rows = vec![row("s1", "[]")];

        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..READERS {
                let cache = cache.clone();
                let runs = StdArc::clone(&runs);
                let arrived = StdArc::clone(&arrived);
                let rows = rows.clone();
                handles.push(scope.spawn(move || {
                    // Every thread reaches the cache at about the same moment, which is the shape of a
                    // burst of readers hitting a replica that has just started.
                    arrived.wait();
                    cache.get_or_reconstruct(Reconstruction::Spans, rows, |_rows| {
                        runs.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        FeedResult::default()
                    })
                }));
            }
            for handle in handles {
                handle.join().expect("no reader panicked");
            }
        });

        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "{READERS} concurrent readers of the same uncached rows must reconstruct once between them"
        );
    }

    /// An absent field and an empty one are different inputs.
    ///
    /// Mapping `None` to `""` made them the same key, and an empty attribute is accepted - so a row whose
    /// field went from absent to empty shared a key with its old self, and a warm instance would answer
    /// with the field missing where a cold one reconstructs it present-and-empty. Two instances
    /// disagreeing about the same row is exactly what the digest exists to prevent.
    #[test]
    fn an_absent_field_is_not_an_empty_one() {
        let cache = ReconstructionCache::new();
        let runs = std::cell::Cell::new(0);
        let reconstruct = |_rows: Vec<MessageSpanRow>| {
            runs.set(runs.get() + 1);
            FeedResult::default()
        };

        let absent = row("s1", "[]");
        assert!(absent.status_code.is_none());
        cache.get_or_reconstruct(Reconstruction::Spans, vec![absent.clone()], reconstruct);

        let mut empty = absent.clone();
        empty.status_code = Some(String::new());
        cache.get_or_reconstruct(Reconstruction::Spans, vec![empty], reconstruct);
        assert_eq!(runs.get(), 2, "absent and empty are not the same input");
    }

    /// Ingestion time is part of the input, because the pipeline reads it.
    ///
    /// `group_and_sort_traces` orders trace groups by earliest span timestamp and then by earliest
    /// ingestion time, and that order decides which trace strips its history against which. A digest
    /// that ignored it would answer a differently-ordered session from the first one it saw.
    #[test]
    fn ingestion_time_is_part_of_the_key() {
        let cache = ReconstructionCache::new();
        let runs = std::cell::Cell::new(0);
        let reconstruct = |_rows: Vec<MessageSpanRow>| {
            runs.set(runs.get() + 1);
            FeedResult::default()
        };

        let first = row("s1", "[]");
        cache.get_or_reconstruct(Reconstruction::Spans, vec![first.clone()], reconstruct);
        let mut later = first.clone();
        later.ingested_at = first.ingested_at + chrono::Duration::seconds(30);
        cache.get_or_reconstruct(Reconstruction::Spans, vec![later], reconstruct);
        assert_eq!(
            runs.get(),
            2,
            "a different ingestion time is a different input"
        );
    }

    /// A re-delivered span changes its row, so the digest changes with it.
    ///
    /// This is what replaces invalidation: ingestion rewrites the row of a span it has seen before, and
    /// the pipeline reads that row, so there is nothing to remember to expire.
    #[test]
    fn a_redelivered_span_is_a_different_input() {
        let cache = ReconstructionCache::new();
        let runs = std::cell::Cell::new(0);
        let reconstruct = |_rows: Vec<MessageSpanRow>| {
            runs.set(runs.get() + 1);
            FeedResult::default()
        };

        let mut first = row("s1", "[]");
        first.total_tokens = 100;
        cache.get_or_reconstruct(Reconstruction::Spans, vec![first.clone()], reconstruct);

        let mut corrected = first.clone();
        corrected.total_tokens = 900;
        cache.get_or_reconstruct(Reconstruction::Spans, vec![corrected], reconstruct);
        assert_eq!(
            runs.get(),
            2,
            "the corrected row is not answered from the old one"
        );
    }
}
