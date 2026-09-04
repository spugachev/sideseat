//! SideML Feed Pipeline
//!
//! Reconstructs conversation timelines from OTEL spans that may contain
//! duplicated messages (history duplication) from multiple AI frameworks.
//!
//! # The Problem
//!
//! OTEL traces often contain duplicate messages because:
//! - **Event-based frameworks** (Strands): Child spans re-emit parent events
//! - **Attribute-based frameworks** (LangGraph): Message arrays accumulate history
//! - **Session history**: Previous turns passed as context to new LLM calls
//! - **Tool chains**: ToolUse → Tool execution → ToolResult need logical ordering
//!
//! # The Solution
//!
//! ## Output Classification
//!
//! First, classify each block as OUTPUT or INPUT:
//! - **OUTPUT**: LLM responses that should NEVER be marked as history
//!   - `gen_ai.choice` events (always output, regardless of span type)
//!   - Assistant text/thinking blocks
//!   - ToolUse from generation spans (LLM decided to call tool)
//! - **INPUT**: Everything else (user messages, system, tool results, history)
//!
//! ## Eight-Phase History Detection
//!
//! See `history.rs` for the full algorithm. Key phases:
//! 0. **Output Protection**: OUTPUT blocks are NEVER marked as history
//! 2. **Timestamp-based**: Message timestamp < span start → historical context
//! 3. **Accumulator span input**: Input events from non-root accumulator spans
//! 4. **Intermediate text**: Assistant text from generation spans (event-based frameworks)
//!    - **(4b) Input-source assistant**: Assistant from input attrs in non-root gen spans
//! 5. **Multi-turn history**: All unprotected content in generation spans with tool_results
//! 6. **Orphan tool_results**: Tool_results with unknown tool_use_id
//! 7. **Deduplication**: Later occurrences of same content within trace
//!
//! ## Content-Based Identity (mostly not ID-based)
//!
//! - Tool calls: `hash(name + input)` — call_id ignored (regenerated in history)
//! - Tool results: `tool_use_id` when present, `hash(content)` otherwise. Correlation (below)
//!   supplies the id for frameworks that omit it, so this is the usual case rather than the
//!   fallback.
//! - Regular: `hash(trace_id + role + content)`
//! - Structured JSON answers: members with no value are dropped before hashing, so a
//!   schema-filled object and the model's raw one are one answer. Tool inputs and results keep
//!   the distinction — an empty collection there is an answer.
//!
//! ## Quality Scoring
//!
//! Picks best version when deduplicating:
//! - Non-history (+100), finish_reason (+10), enrichment (+5), output-source (+4),
//!   tool-span (+3), event source (+2), model info (+1)
//!
//! # Pipeline Stages
//!
//! ```text
//! 1. PARSE       Vec<MessageSpanRow> → SideML messages
//! 2. FLATTEN     One ContentBlock per BlockEntry with all metadata; never filtered
//! 3. CORRELATE   id-less tool results adopt their call's id (see correlate.rs)
//! 4. CLASSIFY    Determine uses_span_end for each block
//! 5. MARK HISTORY Eight-phase detection (see history.rs)
//! 6. DEDUP       Identity-based, keep highest quality version
//! 7. WITHDRAW    Clear a correlated id whose call did not survive dedup
//! 8. SORT        (birth_time, message_index, entry_index)
//! 9. ROLE FILTER `?role=` applied here, to the finished feed, on each block's derived role
//! 10. RETURN     FeedResult with blocks, tool_definitions, metadata
//! ```
//!
//! Stages 3 and 5-6 all decide what counts as the same tool result, and all three need the call
//! reference, which is why correlation precedes them.
//!
//! ## Known limit: identical repeats within one trace
//!
//! Two tool calls with the same name and arguments, or two messages with the same role and text,
//! are treated as one within a trace. That is not incidental - a framework re-sending its history
//! is indistinguishable from a genuine repeat once content is all there is, and re-sends are what
//! this pipeline exists to collapse. Telling them apart would need a per-call id that survives
//! re-sending, which no framework in the fixture suite provides. So a conversation that really
//! ran the same tool twice with the same arguments shows it once.
//!
//! # Shape of the pipeline
//!
//! ```mermaid
//! flowchart TD
//!     rows[MessageSpanRow set<br/>one span per row] --> parse[parse_span_rows<br/>JSON to SideML]
//!     parse --> flatten[flatten_to_blocks<br/>one BlockEntry per ContentBlock]
//!     flatten --> corr[correlate_tool_results<br/>id-less result adopts its call's id]
//!     corr --> classify[classify_blocks<br/>uses_span_end, then eight-phase history]
//!     classify --> evidence[collect_order_evidence<br/>the facts the resolver reads]
//!     classify --> dedup[process_dedup_with_lineage<br/>identity, quality, birth times]
//!     dedup --> withdraw[withdraw_unbacked_ids]
//!     withdraw --> resolve[order_graph::resolve<br/>partial order over units]
//!     evidence -.observations.-> resolve
//!     dedup -.lineage.-> resolve
//!     resolve --> out[FeedResult]
//! ```
//!
//! The dotted edges are why the pre-dedup stages are kept: the resolver places *survivors* using
//! evidence from *every* observation, and the lineage is what connects the two. Neither can be
//! re-derived afterwards - dedup collapses on a key carrying a call's rank across the whole input, and
//! withdrawal changes identities and drops blocks.
//!
//! ## Ordering is not downstream of deduplication across traces
//!
//! Within one trace it is: dedup picks survivors, then the resolver orders them. Across traces of one
//! session it is circular, and that is worth stating because it makes every ordering change riskier
//! than it looks:
//!
//! ```mermaid
//! flowchart LR
//!     t1[trace 1<br/>reconstruct] --> p1[accumulated prefix<br/>role + content, in order]
//!     p1 --> t2[trace 2<br/>mark re-sent prefix as history]
//!     t2 --> d2[dedup keeps the genuine copy]
//!     d2 --> o2[resolve orders trace 2]
//!     o2 --> p2[prefix grows]
//!     p2 --> t3[trace 3 ...]
//! ```
//!
//! The prefix is accumulated from each trace's *finished, ordered* messages, and the next trace's scan
//! consumes it as a sequence. So a change to presentation order changes what the next trace strips,
//! which changes its message *set*: promoting the generation-dataflow constraint moved
//! `adk/tool_use`'s session view from 24 messages to 29 by this route alone. A session's
//! deduplication should not be a function of a sibling trace's presentation order; until that is
//! separated, `promoted_constraints_do_not_change_which_messages_appear` can only hold the
//! single-trace path.
//!
//! # Framework Compatibility
//!
//! Works for all frameworks without special cases:
//! - **With history**: Strands, LangGraph, LangChain (duplicates detected/filtered)
//! - **Without history**: AutoGen, CrewAI (passes through unchanged)

pub mod cache;
mod classify;
mod correlate;
mod dedup;
mod history;
// The order resolver of the reconstruction redesign. Production runs `Constraints::PRODUCTION`; the
// all-off `Constraints::NEUTRAL` is provably unable to move a block and is what the neutrality
// property test checks, so the machinery stays verifiable as classes are promoted one at a time.
mod order_graph;
#[cfg(test)]
mod props;
mod types;

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde_json::{Value as JsonValue, json};

use super::normalize::to_sideml_with_context;
use super::provenance::PositionPath;
use super::tools::{extract_tool_name, normalize_tools, tool_definition_quality};
use super::types::ContentBlock;
use crate::data::types::{MessageCategory, MessageSpanRow};
use crate::domain::traces::{MessageSource, RawMessage};

use classify::uses_span_end;
use dedup::{
    FeedPosition, SpanTimestamps, feed_positions, hash_json_into, hash_structured_json_into,
    hash_tool_result_content_into,
};
use history::mark_history;

// Re-exports for public API
pub use types::{BlockEntry, ExtractedTools, FeedMetadata, FeedOptions, FeedResult};

// The dedup tie-break hook, for the test that varies which copy survives.
#[cfg(test)]
pub(crate) use dedup::PREFER_LATER_ON_TIE;

// ============================================================================
// SHARED CONSTANTS
// ============================================================================

/// Observation type values (used for span classification).
pub(crate) mod obs_type {
    pub const GENERATION: &str = "generation";
    pub const TOOL: &str = "tool";
    pub const AGENT: &str = "agent";
    pub const SPAN: &str = "span";
    pub const CHAIN: &str = "chain";
}

/// Source type values (event vs attribute).
pub(crate) mod source_type {
    pub const EVENT: &str = "event";
    pub const ATTRIBUTE: &str = "attribute";
}

/// Status code values.
pub(crate) mod status {
    pub const ERROR: &str = "ERROR";
}

/// GenAI output event names (OpenTelemetry semantic conventions).
/// These represent completion events that should use span_end timestamp.
///
/// `gen_ai.output.messages` is the bundled form the current conventions use, carried on the
/// `gen_ai.client.inference.operation.details` event. Without it here, a bundled output was not
/// recognised as output at all: it did not take the span-end timestamp, it was not protected from
/// history marking, and it shared a response with the input event emitted at the same instant - so
/// it reported the input's time, which is the defect the direction-keyed batching fixes for
/// attribute sources.
pub(crate) const GENAI_OUTPUT_EVENTS: &[&str] = &[
    "gen_ai.choice",
    "gen_ai.content.completion",
    "gen_ai.output.messages",
];

/// GenAI input event names (OpenTelemetry semantic conventions).
/// These represent context/input that may be history copies.
pub(crate) const GENAI_INPUT_EVENTS: &[&str] = &[
    "gen_ai.user.message",
    "gen_ai.assistant.message",
    "gen_ai.system.message",
    "gen_ai.tool.message",
    "gen_ai.content.prompt",
    // The bundled form, paired with gen_ai.output.messages above.
    "gen_ai.input.messages",
];

// ============================================================================
// INTERMEDIATE TYPE FOR PARSING
// ============================================================================

/// Intermediate message after parsing, before flattening.
#[derive(Debug, Clone)]
struct ParsedMessage {
    /// Where this message sat in its span's stored payload - see `sideml::provenance`.
    position: PositionPath,
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    session_id: Option<String>,
    message_index: i32,
    timestamp: DateTime<Utc>,
    source: MessageSource,
    message: super::types::ChatMessage,
    category: MessageCategory,
    model: Option<String>,
    provider: Option<String>,
    status_code: Option<String>,
    total_tokens: i64,
    cost_total: f64,
    observation_type: Option<String>,
}

/// What earlier traces of this session already showed, as a relation rather than a sequence.
///
/// A framework re-sends the conversation so far as the input of its next turn, and the same turn does
/// not come back in the order it was emitted: a provider serialises a turn's *parallel* tool calls into
/// a linear message list, so `call, call, result, result` is replayed as `call, result, call, result`.
/// Matching that against a stored linearisation with one forward cursor reads as a mismatch at the
/// second call - and since a mismatch ends the prefix, everything after it leaks. `adk/tool_use` is the
/// corpus witness: its second trace re-executes the first turn, and the session view showed that turn
/// twice, the second time in the provider's order.
///
/// So the prefix is stored as each prior trace's *precedence relation* plus the occurrences it holds,
/// and a replay is accepted when it is some linear extension of that relation - which is what a
/// provider's serialisation is. Injectively: each replayed block consumes one distinct prior
/// occurrence, so a turn holding two identical calls is matched by two, never twice by one.
///
/// Across traces the relation is the trace order itself. Traces of a session are successive turns, so a
/// replay listing turn 2's messages before turn 1's is not a linear extension of anything - and using
/// the sequence there costs nothing, because history is replayed in order between turns even when it is
/// reordered within one.
#[derive(Debug, Default)]
struct CrossTracePrefixState {
    entries: Vec<PriorOccurrence>,
    /// Where each `(role, content hash)` occurs among the entries, so a candidate lookup does not scan.
    by_identity: HashMap<(super::types::ChatRole, String), Vec<usize>>,
    /// One relation per contributing trace, indexed by that trace's transcript position.
    relations: Vec<order_graph::Precedence>,
    /// Each trace's transcript identities, by position - what a relation's nodes *are*, which the relation
    /// itself does not know. Read only to look one step ahead when choosing between candidates that
    /// nothing else distinguishes.
    identities: Vec<Vec<(super::types::ChatRole, String)>>,
}

/// One occurrence an earlier trace established, and where to find it in that trace's relation.
#[derive(Debug)]
struct PriorOccurrence {
    trace: usize,
    /// Index into the trace's transcript, which is what its `Precedence` is indexed by.
    position: usize,
}

impl CrossTracePrefixState {
    #[inline]
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Add a trace's transcript and the relation the resolver derived over it.
    ///
    /// System blocks are skipped: a system prompt is per-trace framing that every turn re-sends, so it
    /// is not evidence of history and matching it would consume a prefix entry for nothing.
    fn push_trace(&mut self, transcript: &[BlockEntry], relation: order_graph::Precedence) {
        let trace = self.relations.len();
        self.relations.push(relation);
        self.identities.push(
            transcript
                .iter()
                .map(|block| (block.role, block.content_hash.clone()))
                .collect(),
        );
        for (position, block) in transcript.iter().enumerate() {
            if block.role == super::types::ChatRole::System {
                continue;
            }
            let entry = self.entries.len();
            self.entries.push(PriorOccurrence { trace, position });
            self.by_identity
                .entry((block.role, block.content_hash.clone()))
                .or_default()
                .push(entry);
        }
    }

    /// The longest prefix of `replay` that can be matched to distinct prior occurrences, in an order the
    /// evidence permits.
    ///
    /// Returns the entry each matched block claimed. `replay` is one span's strippable blocks in payload
    /// order, as `(role, content hash)`.
    ///
    /// # Why this searches instead of choosing
    ///
    /// Taking the first locally permitted candidate is wrong, and not subtly. Two unordered branches
    /// establish `callA -> resultA` and `callB -> resultB`, and the two results carry the same identity -
    /// two tools that both answered `"ok"`, which is ordinary. Replayed as `callB, resultB, callA,
    /// resultA`, a valid linear extension, the greedy step matches `resultB` against *`resultA`* because
    /// it comes first among equals. That assignment then requires `callA` to have come earlier, `callA` is
    /// refused, and the prefix ends: the rest of the turn is duplicated in the session. Choosing
    /// `resultB` instead matches everything. The identities are interchangeable, so only the order
    /// constraints can tell the two choices apart, and that is a search.
    ///
    /// Bounded, because a matching problem with interchangeable candidates is exponential in the worst
    /// case. `MATCH_BUDGET` caps the assignments tried; exceeding it returns the longest prefix found so
    /// far, which under-strips rather than over-strips - the failure a user sees as duplicates rather
    /// than as missing messages. In practice the first candidate is right and the search is linear: the
    /// budget is never approached by any corpus fixture.
    fn longest_matching_prefix(
        &self,
        replay: &[(super::types::ChatRole, &str)],
    ) -> (Vec<usize>, bool) {
        /// Assignments tried before the search gives up and reports what it has.
        const MATCH_BUDGET: u32 = 20_000;

        let mut search = PrefixSearch {
            state: self,
            consumed: vec![false; self.entries.len()],
            consumed_bits: vec![0u64; self.entries.len().div_ceil(64)],
            must_precede: HashMap::new(),
            chosen: Vec::with_capacity(replay.len()),
            best: Vec::new(),
            failed: HashSet::new(),
            budget: MATCH_BUDGET,
        };
        search.extend(replay, 0);
        let exhaustive = search.budget > 0;
        if !exhaustive {
            tracing::warn!(
                replay = replay.len(),
                prior = self.entries.len(),
                matched = search.best.len(),
                "Cross-trace replay matching hit its budget; some history may be shown twice"
            );
        }
        (search.best, exhaustive)
    }
}

/// One depth-first search for the longest matchable prefix of a replay.
///
/// State is mutated and undone as the search backtracks, so each block and edge of a trace's relation is
/// walked once per path rather than once per candidate.
struct PrefixSearch<'a> {
    state: &'a CrossTracePrefixState,
    /// Which prior occurrences the current partial assignment has claimed. Injectivity, and what keeps a
    /// genuinely repeated question from collapsing onto the first ask.
    consumed: Vec<bool>,
    /// `consumed` as a bitset, maintained **incrementally** rather than rebuilt.
    ///
    /// The signature is taken once per recursion, and rebuilding it walked all `N` prior occurrences each
    /// time - so a trace replaying `N` messages did `N` scans of `N` entries, and a replaying session (where
    /// every generation span re-sends the whole conversation) trended cubic in turn count. The assignment
    /// budget does not bound this, because it counts *assignments*, not the work spent describing a state.
    /// Flipping two words on claim and release keeps the signature exact - no hashing, so no collision can
    /// make a dead end look already-explored - at 1/64th of the reads.
    consumed_bits: Vec<u64>,
    /// Per trace, everything that must precede something already matched.
    must_precede: HashMap<usize, HashSet<u32>>,
    chosen: Vec<usize>,
    best: Vec<usize>,
    /// States already known not to extend to a full match, as `(replay position, claimed occurrences)`.
    ///
    /// Without this the search re-explores the same dead end once per path that reaches it, which is what
    /// made a budget necessary at all: nine interchangeable calls have `9!` orderings of the same set, and
    /// every one of them fails identically. With it each distinct state is explored once, so the shapes
    /// that used to exhaust the budget finish immediately - and the budget becomes a guard against
    /// pathological input rather than the thing that decides the answer.
    failed: HashSet<(usize, Vec<u64>)>,
    budget: u32,
}

impl PrefixSearch<'_> {
    fn extend(&mut self, replay: &[(super::types::ChatRole, &str)], min_trace: usize) {
        if self.chosen.len() > self.best.len() {
            self.best = self.chosen.clone();
        }
        let Some(&(role, content_hash)) = replay.get(self.chosen.len()) else {
            return; // the whole replay matched
        };
        // Seen this exact state fail before? Then it fails again - which set of occurrences has been
        // claimed is all that matters, not the order they were claimed in.
        let signature = (self.chosen.len(), self.consumed_signature());
        if self.failed.contains(&signature) {
            return;
        }
        let Some(candidates) = self
            .state
            .by_identity
            .get(&(role, content_hash.to_string()))
        else {
            return; // nothing prior looks like this block, so the prefix ends here
        };
        // Permitted candidates, most-constrained first.
        //
        // The order matters as much as the search does, because the budget is finite. Ten independent
        // branches whose results are all `"ok"`, replayed in reverse, give every step ten permitted
        // candidates that differ only in which call they answer - and taking them in stored order picks
        // the wrong one nine times out of ten, so the search spends its whole budget backtracking and
        // gives up part way through a replay it should have matched entirely.
        //
        // "Most constrained" is: how many of the candidate's own ancestors are still unmatched. The one
        // whose call the replay has *just* matched has none, and it is exactly the right choice, so the
        // common shapes are matched first-try and the search never branches. This is a heuristic on the
        // order of exploration, not on the answer: what is permitted is unchanged, so a shape the
        // heuristic guesses wrong is still found by backtracking.
        let mut permitted: Vec<(u8, usize, usize)> = Vec::new();
        for &entry in candidates {
            if self.consumed[entry] {
                continue;
            }
            let occurrence = &self.state.entries[entry];
            if occurrence.trace < min_trace {
                continue; // a later turn's message cannot be replayed before an earlier turn's
            }
            // Would taking this candidate claim an order the evidence contradicts? It would exactly when
            // the candidate must precede something the replay has already matched.
            if self
                .must_precede
                .get(&occurrence.trace)
                .is_some_and(|ancestors| ancestors.contains(&(occurrence.position as u32)))
            {
                continue;
            }
            permitted.push((
                // Does taking this candidate line up with what the replay says comes next? Zero first.
                self.lookahead_cost(replay, occurrence.trace, occurrence.position),
                self.unmatched_ancestors(occurrence.trace, occurrence.position),
                entry,
            ));
        }
        permitted.sort_unstable();

        for (_, _, entry) in permitted {
            if self.budget == 0 {
                return;
            }
            let occurrence = &self.state.entries[entry];

            self.budget -= 1;
            self.consumed[entry] = true;
            self.consumed_bits[entry / 64] |= 1u64 << (entry % 64);
            self.chosen.push(entry);
            let added = self.add_ancestors(occurrence.trace, occurrence.position);

            self.extend(replay, occurrence.trace);

            for node in added {
                self.must_precede
                    .get_mut(&occurrence.trace)
                    .expect("the set this step added to")
                    .remove(&node);
            }
            self.chosen.pop();
            self.consumed[entry] = false;
            self.consumed_bits[entry / 64] &= !(1u64 << (entry % 64));

            if self.best.len() == replay.len() {
                return; // nothing beats a full match
            }
        }

        // Every candidate was tried and none led to a full match, so this state never will.
        if self.best.len() < replay.len() {
            self.failed.insert(signature);
        }
    }

    /// The set of claimed occurrences, as a bitset - the part of the state that decides whether a dead
    /// end is the same dead end.
    ///
    /// A copy of the incrementally-maintained `consumed_bits`, not a fresh walk of `consumed`: see that
    /// field for why rebuilding it made the search quadratic in the replay length.
    fn consumed_signature(&self) -> Vec<u64> {
        self.consumed_bits.clone()
    }

    /// Whether this candidate's immediate successors include what the replay asks for next: `0` if they
    /// do, `1` if they do not.
    ///
    /// One step of lookahead, and it separates candidates that nothing else can. A tool call's identity
    /// excludes the provider's call id, so nine calls of the same tool with the same input - a model
    /// retrying - are one identity; and a call has no ancestors, so "fewest unmatched ancestors" ties
    /// across all nine. What distinguishes them is which result follows, and the replay says which result
    /// is next.
    ///
    /// A heuristic on the order of exploration only: what is permitted is unchanged, so a shape it guesses
    /// wrong is still found by backtracking.
    fn lookahead_cost(
        &self,
        replay: &[(super::types::ChatRole, &str)],
        trace: usize,
        position: usize,
    ) -> u8 {
        let Some(&(next_role, next_hash)) = replay.get(self.chosen.len() + 1) else {
            return 0; // nothing follows, so nothing to line up with
        };
        let identities = &self.state.identities[trace];
        let lines_up = self.state.relations[trace]
            .successors_of(position)
            .iter()
            .any(|&successor| {
                identities
                    .get(successor as usize)
                    .is_some_and(|(role, hash)| *role == next_role && hash == next_hash)
            });
        u8::from(!lines_up)
    }

    /// How many of this occurrence's ancestors the replay has not matched yet.
    ///
    /// Zero means everything it depends on is already accounted for, which is what makes it the right
    /// candidate among interchangeable ones - see the ordering in `extend`.
    fn unmatched_ancestors(&self, trace: usize, position: usize) -> usize {
        let mut ancestors: HashSet<u32> = HashSet::new();
        self.state.relations[trace].collect_ancestors(position, &mut ancestors);
        ancestors
            .iter()
            .filter(|&&node| {
                // An ancestor counts as unmatched when no chosen entry is that occurrence.
                !self.chosen.iter().any(|&chosen| {
                    let occurrence = &self.state.entries[chosen];
                    occurrence.trace == trace && occurrence.position as u32 == node
                })
            })
            .count()
    }

    /// Record everything that must precede this occurrence, returning what was newly added so the
    /// caller can undo it.
    fn add_ancestors(&mut self, trace: usize, position: usize) -> Vec<u32> {
        let mut reached: HashSet<u32> = HashSet::new();
        self.state.relations[trace].collect_ancestors(position, &mut reached);
        let known = self.must_precede.entry(trace).or_default();
        let mut added = Vec::new();
        for node in reached {
            if known.insert(node) {
                added.push(node);
            }
        }
        added
    }
}

// ============================================================================
// PUBLIC API
// ============================================================================

/// Process span rows through the complete feed pipeline.
///
/// Routes to `process_trace_spans` for single-trace data, or
/// `process_multi_trace_spans` for multi-trace data (cross-trace prefix stripping).
pub fn process_spans(rows: Vec<MessageSpanRow>, options: &FeedOptions) -> FeedResult {
    apply_role_filter(process_spans_unfiltered(rows), options.role.as_deref())
}

/// [`process_spans`], memoised on the rows - see [`cache::ReconstructionCache`] for why that is safe.
///
/// The *unfiltered* reconstruction is what is remembered, and the role filter is applied to a copy of
/// it: the filter narrows an answer rather than changing it, so one cached reconstruction serves every
/// role a caller asks for.
pub fn process_spans_cached(
    cache: &cache::ReconstructionCache,
    rows: Vec<MessageSpanRow>,
    options: &FeedOptions,
) -> FeedResult {
    let reconstructed =
        cache.get_or_reconstruct(cache::Reconstruction::Spans, rows, process_spans_unfiltered);
    apply_role_filter((*reconstructed).clone(), options.role.as_deref())
}

/// [`process_feed`], memoised on the rows, exactly as [`process_spans_cached`] is.
pub fn process_feed_cached(
    cache: &cache::ReconstructionCache,
    rows: Vec<MessageSpanRow>,
    options: &FeedOptions,
) -> FeedResult {
    // The grouping is passed through, and is part of the cache key: it is the caller's authoritative
    // trace → session mapping, and the reconstruction's answer depends on it. Reconstructing with a bare
    // `FeedOptions::new()` silently discarded it, so the route's fix had no effect on the cached path -
    // which is every production read.
    let grouping = options.session_of_trace.clone();
    let reconstructed = cache.get_or_reconstruct_grouped(
        cache::Reconstruction::Feed,
        rows,
        &options.session_of_trace,
        move |rows| process_feed(rows, &FeedOptions::new().with_session_of_trace(grouping)),
    );
    apply_role_filter((*reconstructed).clone(), options.role.as_deref())
}

/// [`process_spans`] without the role filter, for callers that filter once at their own boundary.
fn process_spans_unfiltered(rows: Vec<MessageSpanRow>) -> FeedResult {
    process_spans_unfiltered_with(rows, order_graph::Constraints::PRODUCTION)
}

/// As [`process_spans_unfiltered`], with the ordering constraints named explicitly.
///
/// The parameter exists so a test can hold everything else fixed and vary only the presentation
/// constraints - which is the acceptance property for this whole redesign: changing them must preserve
/// which messages a session returns. Production always passes `PRODUCTION`.
fn process_spans_unfiltered_with(
    rows: Vec<MessageSpanRow>,
    constraints: order_graph::Constraints,
) -> FeedResult {
    // Detect multi-trace: if all rows share the same trace_id, single-trace path
    let is_multi_trace = rows.len() > 1
        && rows
            .first()
            .map(|first| rows.iter().any(|r| r.trace_id != first.trace_id))
            .unwrap_or(false);

    if is_multi_trace {
        process_multi_trace_spans(rows, constraints)
    } else {
        reconstruct_trace(rows, None, constraints, false).0
    }
}

/// Process span rows from a single trace through the complete feed pipeline.
///
/// This is the core pipeline for processing raw message data from the database.
/// Raw messages are converted to SideML at query time, then flattened to blocks.
///
/// # Pipeline
///
/// 1. Parse raw messages from JSON and convert to SideML
/// 2. Flatten to individual content blocks with metadata
/// 3. Deduplicate by identity (collapse history to first occurrence)
/// 4. Sort by birth time + semantic order
/// 5. Return FeedResult with blocks, tool definitions, and metadata
pub fn process_trace_spans(rows: Vec<MessageSpanRow>, options: &FeedOptions) -> FeedResult {
    apply_role_filter(
        process_trace_spans_core(rows, None),
        options.role.as_deref(),
    )
}

/// Core pipeline with optional cross-trace prefix marking.
///
/// When `cross_trace_prefix` is provided, input-source blocks matching the
/// accumulated prefix from previous traces are marked as history BEFORE dedup.
/// This allows within-trace dedup to correctly preserve genuine repeated content
/// (the non-history copy wins via +100 quality bonus) while stripping the
/// history re-send copy.
/// Stages 1-4 of the single-trace pipeline: the classified, pre-dedup blocks and the span
/// timestamps, which is the complete evidence set before any observation is collapsed to a
/// representative. `process_trace_spans_core` continues from here into dedup and sorting; the shadow
/// order resolver reads the same output so it judges the evidence production actually reconstructs.
fn classify_span_blocks(
    rows: &[MessageSpanRow],
    cross_trace_prefix: Option<&CrossTracePrefixState>,
) -> (Vec<BlockEntry>, HashMap<String, SpanTimestamps>, bool) {
    // Build span hierarchy for span_path computation
    let span_hierarchy = build_span_hierarchy(rows);

    // Build span timestamps map for birth time computation
    let span_timestamps = build_span_timestamps(rows);

    // Stage 1: Parse raw messages and convert to SideML
    let mut parsed_messages = parse_span_rows(rows);

    // Stage 1b: Append error messages from leaf error spans
    append_error_messages(&mut parsed_messages, rows);

    // Stage 2: Flatten to individual blocks with metadata
    // All blocks start with is_history = false
    let mut blocks = flatten_to_blocks(parsed_messages, &span_hierarchy);

    // Stage 2.5: Cross-trace prefix marking (multi-trace sessions only)
    // MUST run BEFORE classify_blocks (which includes Phase 7 duplicate detection).
    // If run after, Phase 7 would mark the second occurrence as history, then
    // cross-trace would mark the first → both become history → genuine content lost.
    // Running before ensures Phase 7 sees the first copy as already-history and
    // skips it, preserving the genuine (second) copy.
    let replay_matching_complete = match cross_trace_prefix {
        Some(prefix) => mark_cross_trace_prefix(&mut blocks, prefix),
        None => true,
    };

    // Stage 2.6: Correlate tool results to their calls.
    //
    // Runs BEFORE classification and dedup, because both decide what is a duplicate tool result
    // and both need the call reference to do it. Two results with the same text are either one
    // call re-sent or two different calls, and only the call tells them apart - so a result that
    // reaches either stage without its call's id has both of them fall back to text, and a
    // genuine second result is dropped from the feed. Correlation needs the blocks in source
    // order, which is what they are in right after flattening.
    correlate::correlate_tool_results(&mut blocks);

    // Stages 3-4: Classify blocks and mark history
    // - uses_span_end: determines timestamp strategy (span_end vs event_time)
    // - is_history: marks non-authoritative blocks for filtering
    classify_blocks(&mut blocks, &span_timestamps);

    (blocks, span_timestamps, replay_matching_complete)
}

/// The shadow resolver's order over one trace's blocks — the redesign's partial-order timeline,
/// built from the same evidence and survivors production uses, but consumed by nothing yet. Tests
/// assert it derives orders the scalar sort key gets wrong.
/// Stage timings for one trace's pipeline, for the performance bench.
#[cfg(test)]
pub(crate) fn stage_timings(rows: Vec<MessageSpanRow>) -> Vec<(&'static str, std::time::Duration)> {
    let mut out = Vec::new();
    let t = std::time::Instant::now();
    let span_hierarchy = build_span_hierarchy(&rows);
    let span_timestamps = build_span_timestamps(&rows);
    out.push(("  hierarchy+timestamps", t.elapsed()));

    let t = std::time::Instant::now();
    let mut parsed_messages = parse_span_rows(&rows);
    out.push(("  parse_span_rows", t.elapsed()));

    let t = std::time::Instant::now();
    append_error_messages(&mut parsed_messages, &rows);
    out.push(("  append_error_messages", t.elapsed()));

    let t = std::time::Instant::now();
    let mut blocks = flatten_to_blocks(parsed_messages, &span_hierarchy);
    out.push(("  flatten_to_blocks", t.elapsed()));

    let t = std::time::Instant::now();
    correlate::correlate_tool_results(&mut blocks);
    out.push(("  correlate", t.elapsed()));

    let t = std::time::Instant::now();
    classify_blocks(&mut blocks, &span_timestamps);
    out.push(("  classify_blocks(history)", t.elapsed()));

    let t = std::time::Instant::now();
    let evidence = order_graph::collect_order_evidence(&blocks, &span_timestamps);
    out.push(("collect_evidence", t.elapsed()));

    let t = std::time::Instant::now();
    let (survivors, lineage) = survivors_with_lineage(blocks, &span_timestamps);
    out.push(("dedup+withdraw", t.elapsed()));

    let t = std::time::Instant::now();
    let resolved = order_graph::resolve(
        &evidence,
        &survivors,
        &lineage,
        &span_timestamps,
        order_graph::Constraints::NEUTRAL,
    );
    out.push(("resolve", t.elapsed()));

    let t = std::time::Instant::now();
    let _ = compute_metadata(&resolved, &rows, true);
    out.push(("metadata", t.elapsed()));
    out
}

/// The classified, pre-dedup blocks, for diagnosing what the resolver is given.
#[cfg(test)]
pub(crate) fn classified_blocks_for_test(rows: Vec<MessageSpanRow>) -> Vec<BlockEntry> {
    classify_span_blocks(&rows, None).0
}

/// One view built twice: with generation dataflow expressed through a barrier node, and as the product
/// of each span's inputs and outputs.
///
/// The barrier exists only to bound the edge count; it must not change the answer.
#[cfg(test)]
pub(crate) fn barrier_and_pairwise_order(
    rows: Vec<MessageSpanRow>,
) -> (Vec<BlockEntry>, Vec<BlockEntry>) {
    let barrier = process_spans_unfiltered_with(rows.clone(), order_graph::Constraints::PRODUCTION);
    let pairwise = process_spans_unfiltered_with(
        rows,
        order_graph::Constraints {
            pairwise_dataflow_edges: true,
            ..order_graph::Constraints::PRODUCTION
        },
    );
    (barrier.messages, pairwise.messages)
}

/// One view built twice: with the constraints production enforces, and with every class off.
///
/// This is the acceptance property of the ordering redesign, as a function a test can call: the two
/// runs must return the *same messages*, differing only in their order. It takes the whole row set, so
/// it exercises the multi-trace path where deduplication and ordering were coupled - a session's
/// cross-trace replay stripping used to consume the presented order, so promoting a constraint changed
/// which messages a later trace kept.
#[cfg(test)]
pub(crate) fn presented_and_unconstrained(
    rows: Vec<MessageSpanRow>,
) -> (Vec<BlockEntry>, Vec<BlockEntry>) {
    let presented =
        process_spans_unfiltered_with(rows.clone(), order_graph::Constraints::PRODUCTION);
    let unconstrained = process_spans_unfiltered_with(rows, order_graph::Constraints::NEUTRAL);
    (presented.messages, unconstrained.messages)
}

/// The order before the resolver, and the order the resolver produces with every class off.
///
/// The two must be identical: that is what "the resolver cannot move a block by itself" means, and it
/// has to be a property rather than an observation about the current goldens, which are regenerable.
#[cfg(test)]
pub(crate) fn legacy_and_neutral_order(
    rows: Vec<MessageSpanRow>,
) -> (Vec<BlockEntry>, Vec<BlockEntry>) {
    let (blocks, span_timestamps, _) = classify_span_blocks(&rows, None);
    let evidence = order_graph::collect_order_evidence(&blocks, &span_timestamps);
    let (legacy, lineage) = survivors_with_lineage(blocks, &span_timestamps);
    let scaffold = order_graph::resolve(
        &evidence,
        &legacy,
        &lineage,
        &span_timestamps,
        order_graph::Constraints::NEUTRAL,
    );
    (legacy, scaffold)
}

#[cfg(test)]
pub(crate) fn shadow_resolved_order(rows: Vec<MessageSpanRow>) -> Vec<BlockEntry> {
    let (blocks, span_timestamps, _) = classify_span_blocks(&rows, None);
    let evidence = order_graph::collect_order_evidence(&blocks, &span_timestamps);
    let (survivors, lineage) = survivors_with_lineage(blocks, &span_timestamps);
    order_graph::resolve(
        &evidence,
        &survivors,
        &lineage,
        &span_timestamps,
        order_graph::Constraints::FULL,
    )
}

/// Dedup and withdrawal, with a lineage from each pre-dedup observation to the block it became.
///
/// Both stages change the mapping and neither can be inverted afterwards: dedup collapses on a key
/// that carries a call's rank across the whole input, and withdrawal clears ids *and* drops blocks. So
/// the two remaps are composed here, once, for every caller that needs to trace evidence.
fn survivors_with_lineage(
    blocks: Vec<BlockEntry>,
    span_timestamps: &HashMap<String, SpanTimestamps>,
) -> (Vec<BlockEntry>, Vec<Option<usize>>) {
    let (blocks, dedup_lineage) =
        dedup::process_dedup_with_lineage(blocks, span_timestamps.clone());
    let (blocks, withdrawal_remap) = correlate::withdraw_unbacked_ids_with_remap(blocks);
    let lineage = dedup_lineage
        .into_iter()
        .map(|survivor| survivor.and_then(|s| withdrawal_remap.get(s).copied().flatten()))
        .collect();
    (blocks, lineage)
}

fn process_trace_spans_core(
    rows: Vec<MessageSpanRow>,
    cross_trace_prefix: Option<&CrossTracePrefixState>,
) -> FeedResult {
    reconstruct_trace(
        rows,
        cross_trace_prefix,
        order_graph::Constraints::PRODUCTION,
        false,
    )
    .0
}

/// The trace's answer, and the **causal transcript** a following trace matches its replay against.
///
/// Two separate outputs on purpose. The transcript is the survivors in the order reconstruction
/// established *before* the order resolver applies any presentation constraint, so it is a function of
/// the evidence alone. A session's deduplication must not depend on how the messages are laid out for
/// a reader: accumulating the prefix from the presented order made promoting an ordering constraint
/// change which messages a *later* trace kept, and `adk/tool_use`'s session view gained five replayed
/// messages that way - the previous turns' tool results, re-appearing because the presented sequence no
/// longer lined up with what ADK replays.
fn reconstruct_trace(
    rows: Vec<MessageSpanRow>,
    cross_trace_prefix: Option<&CrossTracePrefixState>,
    constraints: order_graph::Constraints,
    needs_replay_relation: bool,
) -> (FeedResult, Vec<BlockEntry>, order_graph::Precedence) {
    // Extract tools from all rows
    let extracted_tools = extract_tools_from_rows(&rows);

    // Stages 1-4: parse, flatten, correlate and classify - everything the pipeline knows before
    // dedup collapses the observations to one representative each. Kept, because the order resolver
    // reads the *evidence*: the emission binding a turn's intro text to its call is on one span while
    // dedup may keep a re-listed copy of that text from another, and only the pre-dedup set says so.
    let (blocks, span_timestamps, replay_matching_complete) =
        classify_span_blocks(&rows, cross_trace_prefix);
    // Reduced to what the resolver reads, from the borrowed slice: holding the blocks themselves
    // would clone every message's content, which on a trace carrying base64 images dominates the
    // whole request and is never read.
    let evidence = order_graph::collect_order_evidence(&blocks, &span_timestamps);

    // Stages 5-6: Deduplicate by identity and sort by birth time, then stage 6.5, withdrawing a
    // correlated id whose call did not survive.
    //
    // Correlation only ever links to a call in the same block list, but dedup and history marking can
    // drop that call afterwards - leaving the result pointing at something the response does not
    // contain. Clearing the id restores "honestly uncorrelated"; keeping the block, because the
    // result's content is real either way. Only correlated ids are withdrawn: a provider's own id may
    // legitimately reference a call outside the requested scope.
    //
    // The two run together because the resolver below needs the *lineage* across both: each says which
    // block an observation became, and neither mapping can be re-derived afterwards.
    let (blocks, lineage) = survivors_with_lineage(blocks, &span_timestamps);

    // Stage 6.6: Resolve the order as a partial order rather than a scalar key.
    //
    // Under `SCAFFOLD` this cannot move a block - it enforces only the constraints the sort above
    // already satisfies - so it is live and exercised on every request while the answer stays exactly
    // what it was. `the_scaffold_reproduces_the_existing_order` holds it to that across the corpus.
    // Runs after id withdrawal so the call/result edges see the ids the view will actually show.
    // The transcript: survivors as reconstruction established them, before any presentation choice.
    let transcript = blocks.clone();

    // The relation a following trace matches its replay against, over the *pre-resolve* survivors -
    // which is what the transcript is. Built only when a later trace can use it: for a single-trace
    // request nothing ever asks.
    let replay_relation = if needs_replay_relation {
        order_graph::causal_precedence(&evidence, &blocks, &lineage)
    } else {
        order_graph::Precedence::default()
    };

    let blocks = order_graph::resolve(&evidence, &blocks, &lineage, &span_timestamps, constraints);

    // Debug: Log block counts after dedup
    if tracing::enabled!(tracing::Level::DEBUG) {
        let dedup_count_by_type: HashMap<_, usize> = blocks
            .iter()
            .map(|b| b.entry_type.as_str())
            .fold(HashMap::new(), |mut acc, t| {
                *acc.entry(t).or_insert(0) += 1;
                acc
            });
        tracing::trace!(
            total = blocks.len(),
            by_type = ?dedup_count_by_type,
            "Feed: after process_dedup"
        );
    }

    // Stage 7: Compute metadata and return
    let metadata = compute_metadata(&blocks, &rows, replay_matching_complete);

    (
        FeedResult {
            messages: blocks,
            tool_definitions: extracted_tools.tool_definitions,
            tool_names: extracted_tools.tool_names,
            metadata,
        },
        transcript,
        replay_relation,
    )
}

/// Process spans from multiple traces with cross-trace prefix marking.
///
/// Groups rows by trace_id, sorts traces chronologically, processes each through
/// the within-trace pipeline with accumulated prefix entries from prior traces.
/// The prefix marking happens BEFORE within-trace dedup (in `process_trace_spans_core`),
/// so genuine repeated content (same content as prior trace) is preserved: the history
/// re-send copy is marked as `is_history`, while the genuine copy stays non-history
/// and wins dedup via +100 quality bonus.
///
/// # Accumulated Prefix
///
/// All non-System blocks are accumulated as `(role, content_hash)` entries.
/// Role-aware matching prevents cross-role false matches when content repeats.
/// The prefix scan handles both:
/// - **Root gen spans**: No Phase 4b, all input-source blocks (including assistant)
///   are matched directly against accumulated.
/// - **Non-root gen spans**: Phase 4b marks assistant input-source blocks as history.
///   Prefix scan consumes matched Phase 4b entries without re-marking.
fn process_multi_trace_spans(
    rows: Vec<MessageSpanRow>,
    constraints: order_graph::Constraints,
) -> FeedResult {
    let trace_groups = group_and_sort_traces(rows);

    let mut accumulated = CrossTracePrefixState::default();
    let mut all_blocks: Vec<BlockEntry> = Vec::new();
    let mut all_tool_defs: Vec<serde_json::Value> = Vec::new();
    let mut all_tool_names: Vec<String> = Vec::new();
    let mut total_tokens: i64 = 0;
    let mut total_cost: f64 = 0.0;
    // One incomplete match anywhere makes the session's answer possibly-repeating, so it is reported.
    let mut replay_matching_complete = true;

    let trace_count = trace_groups.len();
    for (trace_idx, trace_rows) in trace_groups.into_iter().enumerate() {
        // The last trace's relation is never consulted, and building one is a second graph over the
        // whole trace.
        let more_traces_follow = trace_idx + 1 < trace_count;
        // Once per span, not once per row, for the same reason `compute_metadata` does it: a
        // re-ingested span is two rows in the DuckDB row set (that query reads the raw table,
        // ClickHouse reads it with FINAL), and summing rows billed the retry as a second call.
        // The session view was the last place still summing rows, so a session and the traces
        // inside it disagreed about their totals whenever a delivery had been retried.
        let mut counted: HashSet<(&str, &str)> = HashSet::new();
        let mut trace_tokens = 0i64;
        let mut trace_cost = 0.0f64;
        for row in &trace_rows {
            if counted.insert((row.trace_id.as_str(), row.span_id.as_str())) {
                trace_tokens += row.total_tokens;
                trace_cost += row.cost_total;
            }
        }

        // First trace: no prefix. Subsequent traces: pass accumulated prefix
        // for pre-dedup marking of history re-sends.
        let cross_trace_prefix = if trace_idx == 0 {
            None
        } else {
            Some(&accumulated)
        };

        let (result, transcript, relation) = reconstruct_trace(
            trace_rows,
            cross_trace_prefix,
            constraints,
            more_traces_follow,
        );

        // First trace always contributes. Subsequent traces contribute only if
        // they have new non-system content (pure replay traces are skipped).
        let has_new_content = trace_idx == 0
            || transcript
                .iter()
                .any(|b| b.role != super::types::ChatRole::System);

        replay_matching_complete &= result.metadata.replay_matching_complete;

        if has_new_content {
            // From the transcript and its relation, never from `result.messages`: what a later trace
            // strips must not depend on how this one is presented, and the relation is exactly the part
            // a linearisation throws away.
            accumulated.push_trace(&transcript, relation);
            all_blocks.extend(result.messages);
            all_tool_defs.extend(result.tool_definitions);
            all_tool_names.extend(result.tool_names);
        }

        // Counted whether or not the trace contributed a message. Cost is what the spans in scope
        // were billed, not what survived history removal: a trace that only re-sent an earlier turn
        // still called the model, and skipping it reported a session as cheaper than it was.
        total_tokens += trace_tokens;
        total_cost += trace_cost;
    }

    let block_count = all_blocks.len();
    let span_count = all_blocks
        .iter()
        .map(|b| (&b.trace_id, &b.span_id))
        .collect::<HashSet<_>>()
        .len();
    let tool_definitions = deduplicate_tools(all_tool_defs);
    let tool_names = deduplicate_names(all_tool_names);

    FeedResult {
        messages: all_blocks,
        tool_definitions,
        tool_names,
        metadata: FeedMetadata {
            block_count,
            span_count,
            total_tokens,
            total_cost,
            // False if any trace's replay matching was cut short - the session's answer may then repeat
            // history, and saying so is the point.
            replay_matching_complete,
        },
    }
}

/// Mark input-source blocks that replay what earlier traces already showed.
///
/// Runs BEFORE `classify_blocks` (before Phase 4b and Phase 7) so that:
/// - Phase 7 (duplicate detection) sees the marked copies as history and skips them,
///   preserving the genuine copy when content repeats.
/// - Phase 4b and other history phases layer on top correctly.
///
/// # Algorithm
///
/// 1. **Guard**: if there are no attribute-sourced input blocks, skip. Event-based frameworks (Strands
///    Python) keep original timestamps, so Phase 2 handles their history within each trace and they
///    stay trace-independent.
/// 2. **Per-span injective match**: for each span, walk its strippable input blocks in payload order and
///    match each against a distinct prior occurrence whose position the relation permits (see
///    [`CrossTracePrefixState`]). Stop at the first block nothing matches - past that point the span is
///    sending new content, not history.
///
/// Returns whether every span's replay was matched exhaustively. `false` means a search hit its budget, so
/// some history may be shown twice - which the answer reports rather than hides.
fn mark_cross_trace_prefix(blocks: &mut [BlockEntry], accumulated: &CrossTracePrefixState) -> bool {
    if accumulated.is_empty() {
        return true;
    }

    // A block is "cross-trace strippable" if it represents history re-sent to a new LLM call:
    // - Attribute-sourced input (LangGraph, ADK, Vercel, etc.)
    // - gen_ai.input.messages event (Strands JS bundled format: all messages share event_time
    //   so timestamp-based Phase 2 can't detect history within a single span)
    // Pure per-message event frameworks (Strands Python: gen_ai.user.message etc.) are excluded
    // because they preserve original timestamps and Phase 2 handles them within each trace.
    let is_strippable = |b: &BlockEntry| {
        (b.is_input_source() && b.source_type == source_type::ATTRIBUTE)
            || b.event_name.as_deref() == Some("gen_ai.input.messages")
    };

    let input_source_count = blocks.iter().filter(|b| b.is_input_source()).count();
    let strippable_input_count = blocks.iter().filter(|b| is_strippable(b)).count();
    if strippable_input_count == 0 {
        return true;
    }
    let mut replay_matching_complete = true;

    // Per span, because each generation span of a trace replays the history independently - ADK and
    // LangGraph re-send it at the start of every one, not only at the trace's start. The blocks are
    // gathered first and matched as a whole, because the choice made for one block can depend on the
    // blocks after it (see `longest_matching_prefix`).
    let mut marked = 0;
    let mut spans_scanned = 0;
    let mut span_start = 0usize;
    while span_start < blocks.len() {
        let span_id = blocks[span_start].span_id.clone();
        let mut span_end = span_start;
        while span_end < blocks.len() && blocks[span_end].span_id == span_id {
            span_end += 1;
        }
        spans_scanned += 1;

        // The span's replayable blocks, in payload order. System prompts are per-trace framing rather
        // than history, so they are transparent: skipped without ending the prefix.
        let replayable: Vec<usize> = (span_start..span_end)
            .filter(|&i| {
                is_strippable(&blocks[i]) && blocks[i].role != super::types::ChatRole::System
            })
            .collect();
        let identities: Vec<(super::types::ChatRole, &str)> = replayable
            .iter()
            .map(|&i| (blocks[i].role, blocks[i].content_hash.as_str()))
            .collect();

        let (matched, exhaustive) = accumulated.longest_matching_prefix(&identities);
        replay_matching_complete &= exhaustive;
        for &i in replayable.iter().take(matched.len()) {
            blocks[i].is_history = true;
            marked += 1;
        }

        span_start = span_end;
    }

    tracing::debug!(
        accumulated_len = accumulated.len(),
        input_source_count,
        strippable_input_count,
        spans_scanned,
        marked,
        replay_matching_complete,
        "cross-trace prefix marking complete"
    );
    replay_matching_complete
}

/// Group rows by trace_id and sort trace groups chronologically.
///
/// Sort key: (min span_timestamp, min ingested_at, first_seen_row_index, trace_id).
/// The first-seen index preserves caller/query order when timestamps tie, which
/// keeps cross-trace prefix stripping stable for same-timestamp traces.
fn group_and_sort_traces(rows: Vec<MessageSpanRow>) -> Vec<Vec<MessageSpanRow>> {
    let mut by_trace: HashMap<String, (usize, Vec<MessageSpanRow>)> = HashMap::new();
    for (row_index, row) in rows.into_iter().enumerate() {
        let entry = by_trace
            .entry(row.trace_id.clone())
            .or_insert_with(|| (row_index, Vec::new()));
        entry.1.push(row);
    }

    let mut trace_groups: Vec<_> = by_trace
        .into_iter()
        .map(|(trace_id, (first_seen_index, rows))| {
            let min_ts = rows.iter().map(|r| r.span_timestamp).min().unwrap();
            let min_ingest = rows.iter().map(|r| r.ingested_at).min().unwrap();
            (trace_id, min_ts, min_ingest, first_seen_index, rows)
        })
        .collect();

    trace_groups.sort_by(|a, b| {
        a.1.cmp(&b.1)
            .then_with(|| a.2.cmp(&b.2))
            .then_with(|| a.3.cmp(&b.3))
            .then_with(|| a.0.cmp(&b.0))
    });

    trace_groups
        .into_iter()
        .map(|(_, _, _, _, rows)| rows)
        .collect()
}

/// Process spans from multiple conversations for a feed.
///
/// Groups spans by conversation boundary (session_id or trace_id),
/// processes each conversation separately, then merges results.
pub fn process_feed(rows: Vec<MessageSpanRow>, options: &FeedOptions) -> FeedResult {
    // A trace's session is resolved from any of its rows that names one, then applied to all of
    // them.
    //
    // Reading each row's own session id split a conversation in half whenever the id is recorded on
    // the root span only, which is how several frameworks record it: the root went to the session
    // group and its children to a trace group, so history detection ran on the two halves
    // separately and had nothing to recognise a re-send against.
    // The caller's mapping first, when it supplied one: it comes from the store, so it survives the content
    // filter that may have removed every row naming the session. See `FeedOptions::session_of_trace`.
    let mut session_of_trace: HashMap<&str, &str> = options
        .session_of_trace
        .iter()
        .map(|(trace, session)| (trace.as_str(), session.as_str()))
        .collect();
    for row in &rows {
        if let Some(session) = row.session_id.as_deref().filter(|s| !s.is_empty()) {
            session_of_trace.entry(&row.trace_id).or_insert(session);
        }
    }
    // A typed key, not a formatted string. `format!("trace:{id}")` shared a namespace with real session
    // ids, so a trace whose session was literally named `trace:B` grouped with the sessionless trace B -
    // two unrelated conversations reconstructed as one, where either can strip the other's messages as
    // replayed history. Session ids come from the client, so that is a collision a caller can cause.
    #[derive(Clone, PartialEq, Eq, Hash)]
    enum Conversation {
        Session(String),
        LoneTrace(String),
    }

    let conversation_of_trace: HashMap<String, Conversation> = rows
        .iter()
        .map(|row| {
            let key = session_of_trace
                .get(row.trace_id.as_str())
                .map(|session| Conversation::Session((*session).to_string()))
                .unwrap_or_else(|| Conversation::LoneTrace(row.trace_id.clone()));
            (row.trace_id.clone(), key)
        })
        .collect();

    let mut spans_by_conversation: HashMap<Conversation, Vec<MessageSpanRow>> = HashMap::new();
    for row in rows {
        let key = conversation_of_trace
            .get(&row.trace_id)
            .cloned()
            .unwrap_or_else(|| Conversation::LoneTrace(row.trace_id.clone()));
        spans_by_conversation.entry(key).or_default().push(row);
    }

    // Process each conversation separately
    let mut all_blocks: Vec<BlockEntry> = Vec::new();
    let mut all_tool_defs: Vec<JsonValue> = Vec::new();
    let mut all_tool_names: Vec<String> = Vec::new();
    let mut total_tokens: i64 = 0;
    let mut total_cost: f64 = 0.0;
    let mut span_ids: HashSet<(String, String)> = HashSet::new();
    // One conversation's incomplete match makes the page possibly-repeating, so the page says so.
    let mut replay_matching_complete = true;

    for (_, conversation_spans) in spans_by_conversation {
        for row in &conversation_spans {
            // Once per span: a re-ingested span appears twice in the DuckDB row set, and summing
            // rows doubled the page's tokens and cost.
            if span_ids.insert((row.trace_id.clone(), row.span_id.clone())) {
                total_tokens += row.total_tokens;
                total_cost += row.cost_total;
            }
        }
        let processed = process_spans_unfiltered(conversation_spans);
        replay_matching_complete &= processed.metadata.replay_matching_complete;
        all_blocks.extend(processed.messages);
        all_tool_defs.extend(processed.tool_definitions);
        all_tool_names.extend(processed.tool_names);
    }

    let all_blocks = sort_feed_newest_first(all_blocks);

    // Deduplicate tools across conversations
    let tool_definitions = deduplicate_tools(all_tool_defs);
    let tool_names = deduplicate_names(all_tool_names);
    let block_count = all_blocks.len();

    apply_role_filter(
        FeedResult {
            messages: all_blocks,
            tool_definitions,
            tool_names,
            metadata: FeedMetadata {
                block_count,
                span_count: span_ids.len(),
                total_tokens,
                total_cost,
                replay_matching_complete,
            },
        },
        options.role.as_deref(),
    )
}

/// Order a project feed newest-first: **responses** descending, each response forward inside.
///
/// The feed is the one view that is not chronological, and that is a statement about *responses*, not
/// about blocks: a turn's intro text still precedes the call it introduces, the call still precedes
/// its result. So the newest-first part is applied by reversing the order of responses and leaving
/// each response's own order alone.
///
/// Taking each response's internal order from the reconstruction, rather than re-deriving it from
/// positions, is what lets the order resolver reach this view at all. Re-deriving it meant the feed
/// recomputed intra-response order from `(span, message_index, entry_index, after_call, hash)` - the
/// same terms the old scalar key used - so any ordering the resolver improves would have landed in the
/// three chronological views and silently vanished here.
///
/// A response is `(trace, order_time)`: `order_time` is the response's anchor, and it is `order_time`
/// rather than the displayed `timestamp` because only one of them *means* "where this sorts"
/// (`the_displayed_time_does_not_decide_the_order`). Keyed by trace as well, because two traces can
/// share an anchor and blocks of different conversations must not interleave - which is the bug the
/// previous explicit key was written to fix, where two blocks in different traces sharing a span id
/// and a time compared equal and their order followed HashMap iteration.
fn sort_feed_newest_first(blocks: Vec<BlockEntry>) -> Vec<BlockEntry> {
    // The chronological order first, exactly as a trace view would build it, so each response's
    // internal order is the reconstructed one.
    let positions = feed_positions(&blocks, |i| blocks[i].order_time);
    let mut keyed: Vec<(FeedPosition, BlockEntry)> = positions.into_iter().zip(blocks).collect();
    keyed.sort_by(|(a_pos, a), (b_pos, b)| {
        a.order_time
            .cmp(&b.order_time)
            .then_with(|| a_pos.span.cmp(&b_pos.span))
            .then_with(|| a_pos.message_index.cmp(&b_pos.message_index))
            .then_with(|| a_pos.entry_index.cmp(&b_pos.entry_index))
            .then_with(|| a_pos.after_call.cmp(&b_pos.after_call))
            .then_with(|| a.content_hash.cmp(&b.content_hash))
    });
    let chronological: Vec<BlockEntry> = keyed.into_iter().map(|(_, block)| block).collect();

    // Then responses, newest first. Maximal runs sharing `(trace, order_time)`, reversed as wholes.
    let mut responses: Vec<Vec<BlockEntry>> = Vec::new();
    for block in chronological {
        let same_response = responses
            .last()
            .and_then(|run: &Vec<BlockEntry>| run.last())
            .is_some_and(|last| {
                last.trace_id == block.trace_id && last.order_time == block.order_time
            });
        if same_response {
            responses
                .last_mut()
                .expect("a run exists when same_response is true")
                .push(block);
        } else {
            responses.push(vec![block]);
        }
    }
    responses.reverse();
    responses.into_iter().flatten().collect()
}

/// Keep only the blocks inside a requested time window.
///
/// A window is a filter on the answer, not on the input. Applying it to the *rows* - which is what
/// passing it to the message query does for `from` - removes the earlier traces that history
/// detection and cross-trace prefix stripping read, and those stages then have nothing to
/// recognise a re-send against: a later turn's request comes back showing the whole conversation
/// again as new messages. The lower bound therefore belongs here, after the pipeline has seen the
/// context. The upper bound is still applied to the query as well, because everything after it is
/// irrelevant to what came before and there is no reason to load it.
///
/// Compares the timestamps the API returns, and is half-open: `from <= t < to`, as the queries are.
pub fn apply_time_window(
    result: FeedResult,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
) -> FeedResult {
    if from.is_none() && to.is_none() {
        return result;
    }

    let messages: Vec<BlockEntry> = result
        .messages
        .into_iter()
        .filter(|b| from.is_none_or(|from| b.timestamp >= from))
        // Half-open at the top, matching the `timestamp_start < to` the message queries apply:
        // with `<=` here, a message exactly on the bound was returned when its span started
        // earlier and dropped when its span started on the bound too.
        .filter(|b| to.is_none_or(|to| b.timestamp < to))
        .collect();
    let span_count = messages
        .iter()
        .map(|b| (&b.trace_id, &b.span_id))
        .collect::<HashSet<_>>()
        .len();

    FeedResult {
        metadata: FeedMetadata {
            block_count: messages.len(),
            span_count,
            ..result.metadata
        },
        messages,
        ..result
    }
}

/// Keep only blocks whose role matches `role`, if one was requested.
///
/// Applied to the finished feed, never during flattening. The role a block reports is derived
/// from its content, not from the raw message role - a Gemini or ADK tool result arrives inside a
/// `user` message - so the filter has to see the derived role, which is why it once lived in
/// `flatten_to_blocks`. Filtering there removes blocks that later stages read:
///
/// - `role=tool` deletes the assistant `ToolUse` blocks that `correlate_tool_results` uses to give
///   an id-less result its call's id. Without the id, dedup falls back to content identity and
///   collapses two results of two different calls into one.
/// - history detection reads user and system messages to decide what is a re-send, so filtering
///   them away changes which of the *remaining* blocks are marked history.
///
/// The filter is a view over the finished feed, so it is applied to the finished feed. Block and
/// span counts are restated from the blocks that survive, so they describe the response rather
/// than the scope that was scanned. Token and cost totals are left as span-level sums: they are
/// the cost of producing the conversation, which filtering the view does not reduce.
fn apply_role_filter(result: FeedResult, role: Option<&str>) -> FeedResult {
    let Some(role) = role else {
        return result;
    };

    let messages: Vec<BlockEntry> = result
        .messages
        .into_iter()
        .filter(|b| b.role.as_str() == role)
        .collect();
    let span_count = messages
        .iter()
        .map(|b| (&b.trace_id, &b.span_id))
        .collect::<HashSet<_>>()
        .len();

    FeedResult {
        metadata: FeedMetadata {
            block_count: messages.len(),
            span_count,
            ..result.metadata
        },
        messages,
        ..result
    }
}

// ============================================================================
// INTERNAL: PARSING
// ============================================================================

/// Parse span rows into parsed messages.
fn parse_span_rows(rows: &[MessageSpanRow]) -> Vec<ParsedMessage> {
    let mut messages: Vec<ParsedMessage> = Vec::with_capacity(rows.len() * 4);

    for row in rows {
        // Determine if this is a tool execution span
        let is_tool_span = row.observation_type.as_deref() == Some(obs_type::TOOL);

        // Parse raw messages and convert to SideML
        match serde_json::from_str::<Vec<RawMessage>>(&row.messages_json) {
            Ok(raw_msgs) => {
                // Debug: Log raw message count
                tracing::trace!(
                    span_id = %row.span_id,
                    raw_msg_count = raw_msgs.len(),
                    "parse_span_rows: raw messages parsed"
                );
                let sideml_msgs = to_sideml_with_context(&raw_msgs, is_tool_span);
                tracing::trace!(
                    span_id = %row.span_id,
                    sideml_msg_count = sideml_msgs.len(),
                    "parse_span_rows: SideML conversion done"
                );
                for (index, msg) in sideml_msgs.into_iter().enumerate() {
                    let timestamp = msg.timestamp;
                    messages.push(ParsedMessage {
                        position: msg.position.clone(),
                        trace_id: row.trace_id.clone(),
                        span_id: row.span_id.clone(),
                        parent_span_id: row.parent_span_id.clone(),
                        session_id: row.session_id.clone(),
                        message_index: index as i32,
                        timestamp,
                        source: msg.source,
                        message: msg.sideml,
                        category: msg.category,
                        model: row.model.clone(),
                        provider: row.provider.clone(),
                        status_code: row.status_code.clone(),
                        total_tokens: row.total_tokens,
                        cost_total: row.cost_total,
                        observation_type: row.observation_type.clone(),
                    });
                }
            }
            Err(e) => {
                tracing::debug!(
                    span_id = %row.span_id,
                    error = %e,
                    "Failed to parse messages JSON"
                );
            }
        }
    }

    messages
}

/// Extract tool definitions and names from span rows.
///
/// Standalone function decoupled from message parsing so handlers can
/// scope tool extraction to specific rows (e.g., a single trace).
pub fn extract_tools_from_rows<'a>(
    rows: impl IntoIterator<Item = &'a MessageSpanRow>,
) -> ExtractedTools {
    let mut tool_defs: Vec<JsonValue> = Vec::new();
    let mut tool_names_raw: Vec<String> = Vec::new();

    for row in rows {
        match serde_json::from_str::<Vec<JsonValue>>(&row.tool_definitions_json) {
            Ok(defs) => tool_defs.extend(defs),
            Err(e) => {
                tracing::debug!(
                    span_id = %row.span_id,
                    error = %e,
                    "Failed to parse tool definitions JSON"
                );
            }
        }

        match serde_json::from_str::<Vec<String>>(&row.tool_names_json) {
            Ok(names) => tool_names_raw.extend(names),
            Err(e) => {
                tracing::debug!(
                    span_id = %row.span_id,
                    error = %e,
                    "Failed to parse tool names JSON"
                );
            }
        }
    }

    ExtractedTools {
        tool_definitions: deduplicate_tools(tool_defs),
        tool_names: deduplicate_names(tool_names_raw),
    }
}

/// Compose error display text from structured exception fields.
/// Presentation logic at query time — raw data preserved in DB columns.
fn compose_error_text(
    exception_type: Option<&str>,
    exception_message: Option<&str>,
    exception_stacktrace: Option<&str>,
) -> Option<String> {
    let header = match (exception_type, exception_message) {
        (Some(t), Some(m)) if !t.is_empty() && !m.is_empty() => Some(format!("{t}: {m}")),
        (_, Some(m)) if !m.is_empty() => Some(m.to_string()),
        (Some(t), _) if !t.is_empty() => Some(t.to_string()),
        _ => None,
    };

    let stacktrace = exception_stacktrace.filter(|s| !s.is_empty());

    match (header, stacktrace) {
        (Some(h), Some(st)) => Some(format!("{h}\n\n```\n{st}\n```")),
        (Some(h), None) => Some(h),
        (None, Some(st)) => Some(format!("```\n{st}\n```")),
        (None, None) => None,
    }
}

/// Append error messages from leaf error spans.
///
/// Creates ParsedMessage objects from exception fields of ERROR spans.
/// These flow through flatten_to_blocks -> classify -> dedup naturally.
/// Only leaf error spans get messages (deepest ERROR in hierarchy).
///
/// Leaf detection is scoped by trace_id to prevent cross-trace collisions
/// when process_feed groups multiple traces into one session.
fn append_error_messages(messages: &mut Vec<ParsedMessage>, rows: &[MessageSpanRow]) {
    // A child suppresses its parent only when the child can actually *say* what went wrong.
    //
    // Testing `status == ERROR` alone deferred to a child that had nothing to render, and then rendered
    // nothing: in `strands/error` the innermost span is a failed `chat` carrying ERROR status and no
    // exception fields, so it silenced its parent and contributed no message, and the same applied one
    // level further up. The trace of a failed run showed `system, user` and **no error at all**, while
    // three separate span views each displayed the `ValidationException`. Deferring to a child is only
    // sound if the child will report.
    let spans_with_error_children: HashSet<(&str, &str)> = rows
        .iter()
        .filter(|r| {
            r.status_code.as_deref() == Some(status::ERROR)
                && r.parent_span_id.is_some()
                && compose_error_text(
                    r.exception_type.as_deref(),
                    r.exception_message.as_deref(),
                    r.exception_stacktrace.as_deref(),
                )
                .is_some()
        })
        .filter_map(|r| {
            r.parent_span_id
                .as_deref()
                .map(|p| (r.trace_id.as_str(), p))
        })
        .collect();

    for row in rows {
        if row.status_code.as_deref() != Some(status::ERROR) {
            continue;
        }
        let error_msg = match compose_error_text(
            row.exception_type.as_deref(),
            row.exception_message.as_deref(),
            row.exception_stacktrace.as_deref(),
        ) {
            Some(m) => m,
            None => continue,
        };
        // Skip non-leaf: this span has an ERROR child within the same trace
        if spans_with_error_children.contains(&(row.trace_id.as_str(), row.span_id.as_str())) {
            continue;
        }

        let timestamp = row.span_end_timestamp.unwrap_or(row.span_timestamp);
        let max_msg_idx = messages
            .iter()
            .filter(|m| m.span_id == row.span_id)
            .map(|m| m.message_index)
            .max()
            .unwrap_or(-1);

        messages.push(ParsedMessage {
            // Composed from the span's exception fields rather than read out of a payload, so there
            // is no position to record. `is_empty()` is how a consumer tells the two apart.
            position: PositionPath::default(),
            trace_id: row.trace_id.clone(),
            span_id: row.span_id.clone(),
            parent_span_id: row.parent_span_id.clone(),
            session_id: row.session_id.clone(),
            message_index: max_msg_idx + 1,
            timestamp,
            source: MessageSource::Attribute {
                key: "exception".to_string(),
                time: timestamp,
            },
            message: super::types::ChatMessage {
                role: super::types::ChatRole::Assistant,
                content: vec![ContentBlock::Text { text: error_msg }],
                finish_reason: Some(super::types::FinishReason::Error),
                ..Default::default()
            },
            category: MessageCategory::Exception,
            model: row.model.clone(),
            provider: row.provider.clone(),
            status_code: row.status_code.clone(),
            total_tokens: 0,
            cost_total: 0.0,
            observation_type: row.observation_type.clone(),
        });
    }
}

// ============================================================================
// INTERNAL: FLATTENING
// ============================================================================

/// Build span hierarchy map for span_path computation.
///
/// Includes cycle detection to prevent infinite loops from malformed data.
fn build_span_hierarchy(span_rows: &[MessageSpanRow]) -> HashMap<String, Vec<String>> {
    let parent_map: HashMap<_, _> = span_rows
        .iter()
        .filter_map(|s| {
            s.parent_span_id
                .as_ref()
                .map(|p| (s.span_id.clone(), p.clone()))
        })
        .collect();

    let mut paths = HashMap::new();
    let max_depth = span_rows.len().max(256); // Floor for partial views (single-span queries)

    for span in span_rows {
        let mut path = vec![span.span_id.clone()];
        let mut current = span.span_id.clone();
        let mut visited = HashSet::with_capacity(max_depth.min(32));
        visited.insert(current.clone());

        while let Some(parent) = parent_map.get(&current) {
            // Cycle detection: stop if we've seen this parent before
            if !visited.insert(parent.clone()) {
                tracing::warn!(
                    span_id = %span.span_id,
                    cycle_at = %parent,
                    "Cycle detected in span hierarchy, truncating path"
                );
                break;
            }

            // Depth limit: prevent runaway in malformed data
            if path.len() >= max_depth {
                tracing::warn!(
                    span_id = %span.span_id,
                    depth = path.len(),
                    "Span hierarchy depth exceeded limit, truncating path"
                );
                break;
            }

            path.push(parent.clone());
            current = parent.clone();
        }

        path.reverse(); // [root, ..., current]
        paths.insert(span.span_id.clone(), path);
    }

    paths
}

/// Build span timestamps map for birth time computation.
fn build_span_timestamps(span_rows: &[MessageSpanRow]) -> HashMap<String, SpanTimestamps> {
    span_rows
        .iter()
        .map(|row| {
            (
                row.span_id.clone(),
                SpanTimestamps {
                    span_start: row.span_timestamp,
                    span_end: row.span_end_timestamp,
                },
            )
        })
        .collect()
}

/// Derive role from content block type, overriding raw message role when needed.
///
/// This handles provider-specific message formats where tool-related content
/// may come with unexpected roles:
/// - ADK/Gemini: ToolResult in "user" role messages (Gemini protocol)
/// - All: ToolUse should always be "assistant" (LLM decided to call)
///
/// For regular content types (text, image, etc.), the original role is preserved.
fn derive_role_from_content(
    block: &ContentBlock,
    original_role: super::types::ChatRole,
) -> super::types::ChatRole {
    match block {
        // Tool results MUST be "tool" role, regardless of raw message
        // Gemini stores these in user messages, but semantically they're tool outputs
        ContentBlock::ToolResult { .. } => super::types::ChatRole::Tool,
        // Tool calls MUST be "assistant" role (LLM decided to call a tool)
        ContentBlock::ToolUse { .. } => super::types::ChatRole::Assistant,
        // All other content types preserve original role
        _ => original_role,
    }
}

/// Flatten parsed messages into individual content blocks.
///
/// All blocks start with `is_history = false`. History detection is done
/// separately by `mark_history()` based on actual
/// content duplication across spans.
///
/// Deliberately unfiltered: every block the spans contain reaches the later stages, because
/// correlation, history detection and dedup all read blocks they do not return. See
/// [`apply_role_filter`].
fn flatten_to_blocks(
    messages: Vec<ParsedMessage>,
    span_hierarchy: &HashMap<String, Vec<String>>,
) -> Vec<BlockEntry> {
    let mut blocks = Vec::new();

    for msg in messages {
        // Skip empty messages
        if msg.message.content.is_empty() {
            tracing::trace!(
                span_id = %msg.span_id,
                role = ?msg.message.role,
                "flatten_to_blocks: skipping empty message"
            );
            continue;
        }

        // Skip spurious tool input JSON blocks from tool spans
        // These are tool invocation parameters that shouldn't appear as messages.
        // Exception: output.value attributes may contain legitimate structured output.
        let is_tool_span = msg.observation_type.as_deref() == Some(obs_type::TOOL);
        let is_output_attr = matches!(
            &msg.source,
            MessageSource::Attribute { key, .. } if key == "output.value" || key.starts_with("output.")
        );
        if is_tool_span
            && !is_output_attr
            && msg.message.content.len() == 1
            && matches!(msg.message.content.first(), Some(ContentBlock::Json { .. }))
        {
            continue;
        }

        let span_path = span_hierarchy
            .get(&msg.span_id)
            .cloned()
            .unwrap_or_default();

        // Source type, event name, and attribute key
        let (src_type, event_name, source_attribute) = match &msg.source {
            MessageSource::Event { name, .. } => (source_type::EVENT, Some(name.clone()), None),
            MessageSource::Attribute { key, .. } => {
                (source_type::ATTRIBUTE, None, Some(key.clone()))
            }
        };

        // Flatten each content block into its own BlockEntry
        // is_history starts as false; will be set by mark_history()
        for (entry_index, block) in msg.message.content.iter().enumerate() {
            let entry_type = block.block_type().to_string();
            let tool_use_id =
                extract_tool_use_id_from_block(block).or_else(|| msg.message.tool_use_id.clone());
            let tool_name = extract_tool_name_from_block(block);
            let content_hash = compute_block_hash(block);
            let is_semantic = block.is_semantic();

            // Derive role from content type, not raw message role.
            // This is critical for frameworks like ADK/Gemini where:
            // - ToolResult comes in "user" role messages (Gemini protocol)
            // - ToolUse should always be "assistant" (LLM decided to call tool)
            let role = derive_role_from_content(block, msg.message.role);

            blocks.push(BlockEntry {
                // The block's own position: the message's path plus which content block this is. Two
                // blocks of one message therefore differ, and so do two identical calls a model made
                // in one response - the thing content alone cannot tell apart.
                position: msg.position.child_index(entry_index),
                entry_type,
                content: block.clone(),
                role,

                trace_id: msg.trace_id.clone(),
                span_id: msg.span_id.clone(),
                session_id: msg.session_id.clone(),
                message_index: msg.message_index,
                entry_index: entry_index as i32,

                parent_span_id: msg.parent_span_id.clone(),
                span_path: span_path.clone(),

                timestamp: msg.timestamp,
                order_time: msg.timestamp,

                observation_type: msg.observation_type.clone(),

                model: msg.model.clone(),
                provider: msg.provider.clone(),

                name: msg.message.name.clone(),
                finish_reason: msg.message.finish_reason,

                tool_use_id,
                tool_name,

                tokens: Some(msg.total_tokens),
                cost: Some(msg.cost_total),

                status_code: msg.status_code.clone(),
                is_error: msg.status_code.as_deref() == Some(status::ERROR),

                source_type: src_type.to_string(),
                event_name: event_name.clone(),
                source_attribute: source_attribute.clone(),
                category: msg.category,

                content_hash: format!("{:016x}", content_hash),
                is_semantic,
                uses_span_end: false, // Will be set by classify_blocks()
                is_history: false,    // Will be set by classify_blocks()
                tool_use_id_correlated: false, // Will be set by correlate_tool_results()
                promoted_to_span_output: false, // Will be set by classify_blocks()
            });
        }
    }

    blocks
}

// ============================================================================
// BLOCK CLASSIFICATION
// ============================================================================

/// Classify blocks and detect history.
///
/// This function performs two key operations:
///
/// 1. **Timestamp classification** (`uses_span_end`): Determines whether each block
///    uses span_end or event_time for ordering. See `classify` module.
///
/// 2. **History detection** (`is_history`): Marks blocks that should be filtered
///    (context copies, intermediate output, duplicates). See `history` module.
///
/// # Pipeline Position
///
/// This runs after flattening and before dedup/sort:
/// ```text
/// Parse → Flatten → [CLASSIFY] → Dedup → Sort
/// ```
fn classify_blocks(blocks: &mut [BlockEntry], span_timestamps: &HashMap<String, SpanTimestamps>) {
    // Step 1: Classify timestamp strategy for each block
    let mut output_count = 0;
    for block in blocks.iter_mut() {
        block.uses_span_end = uses_span_end(block);
        if block.uses_span_end {
            output_count += 1;
        }
    }

    // Step 1b: Promote assistant messages in choiceless generation spans.
    // Logfire/OpenAI Agents store LLM output as gen_ai.assistant.message (not gen_ai.choice).
    // Without promotion, these sort by array index alongside input events → wrong order.
    // Promoting to uses_span_end + GenAIChoice category fixes ordering and history protection.
    //
    // Check at TRACE level: if any span in the trace has gen_ai.choice, skip promotion
    // for the entire trace. This prevents promoting intermediate assistant text in
    // frameworks like Strands where gen_ai.choice lives in a parent/sibling span.
    let traces_with_choice: HashSet<String> = blocks
        .iter()
        .filter(|b| b.is_output_event())
        .map(|b| b.trace_id.clone())
        .collect();

    let mut promoted = 0;
    for block in blocks.iter_mut() {
        if block.is_generation_span()
            && !block.is_tool_use()
            && !traces_with_choice.contains(&block.trace_id)
            && block.event_name.as_deref() == Some("gen_ai.assistant.message")
        {
            block.uses_span_end = true;
            block.category = MessageCategory::GenAIChoice;
            // Effective direction, in one place: the order resolver reads this to know the span
            // produced the block, which its carrier does not say.
            block.promoted_to_span_output = true;
            // Update timestamp to span_end so the block exits the same-batch group
            // (Logfire emits all events at span_start, so without this the sort
            // would preserve array index order instead of using birth_time).
            if let Some(ts) = span_timestamps.get(&block.span_id)
                && let Some(end) = ts.span_end
            {
                block.timestamp = end;
            }
            output_count += 1;
            promoted += 1;
        }
    }

    tracing::trace!(
        total = blocks.len(),
        output_count,
        promoted,
        "timestamp classification complete"
    );

    // Step 2: Detect and mark history blocks
    let stats = mark_history(blocks, span_timestamps);

    tracing::trace!(
        total_history = stats.total_history(),
        "history detection complete"
    );
}

/// Extract tool_use_id from a content block if applicable.
fn extract_tool_use_id_from_block(block: &ContentBlock) -> Option<String> {
    match block {
        ContentBlock::ToolUse { id, .. } => id.clone(),
        ContentBlock::ToolResult { tool_use_id, .. } => tool_use_id.clone(),
        _ => None,
    }
}

/// Extract tool name from a content block if applicable.
fn extract_tool_name_from_block(block: &ContentBlock) -> Option<String> {
    match block {
        ContentBlock::ToolUse { name, .. } => Some(name.clone()),
        _ => None,
    }
}

/// Hash binary content for deduplication - all of it.
///
/// This used to hash the length plus the first and last 128 bytes, which is not a digest but a
/// sample: two assets of the same size whose first and last 128 bytes agree are *deterministically*
/// identical to dedup, so one of them silently disappears from the feed. Three images generated from
/// one prompt in one turn are exactly that shape - same encoder, same dimensions, same header and
/// trailer - and every invariant in the suite still passes, because one image vanishing is
/// indistinguishable from a framework that only reported two.
///
/// Hashing the whole payload is what makes identity mean identity. It is affordable because nothing
/// here allocates: the bytes go straight into the hasher, at gigabytes per second, and a content hash
/// is computed once per block rather than once per comparison.
#[inline]
fn hash_binary_content<H: std::hash::Hasher>(data: &[u8], hasher: &mut H) {
    use std::hash::Hash;

    // Length first, so that appending to a payload cannot collide with the payload itself.
    data.len().hash(hasher);
    hasher.write(data);
}

/// Compute a hash for a content block.
fn compute_block_hash(block: &ContentBlock) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();

    // Hash based on block type and key content
    match block {
        ContentBlock::Text { text } => {
            "text".hash(&mut hasher);
            text.trim().hash(&mut hasher); // Normalize whitespace
        }
        ContentBlock::ToolUse { name, input, .. } => {
            // Hash by name + normalized input only (not id)
            "tool_use".hash(&mut hasher);
            name.hash(&mut hasher);
            hash_json_into(input, &mut hasher);
        }
        ContentBlock::ToolResult {
            name,
            content,
            is_error,
            ..
        } => {
            // Hash by tool name, error flag and normalized content - not tool_use_id, which a
            // history re-send regenerates.
            //
            // Content alone made every "ok" the same message: two tools both reporting success,
            // or a success and a failure whose text happens to match, collapsed into one wherever
            // this hash is the identity - which is the case for a result with no id.
            "tool_result".hash(&mut hasher);
            name.hash(&mut hasher);
            is_error.hash(&mut hasher);
            hash_tool_result_content_into(content, &mut hasher);
        }
        ContentBlock::Thinking { text, .. } => {
            "thinking".hash(&mut hasher);
            text.trim().hash(&mut hasher); // Normalize whitespace
        }
        ContentBlock::RedactedThinking { data } => {
            "redacted_thinking".hash(&mut hasher);
            data.hash(&mut hasher);
        }
        ContentBlock::Image { source, data, .. } => {
            "image".hash(&mut hasher);
            source.hash(&mut hasher);
            hash_binary_content(data.as_bytes(), &mut hasher);
        }
        ContentBlock::Audio { source, data, .. } => {
            "audio".hash(&mut hasher);
            source.hash(&mut hasher);
            hash_binary_content(data.as_bytes(), &mut hasher);
        }
        ContentBlock::Video { source, data, .. } => {
            "video".hash(&mut hasher);
            source.hash(&mut hasher);
            hash_binary_content(data.as_bytes(), &mut hasher);
        }
        ContentBlock::Document {
            source, data, name, ..
        } => {
            "document".hash(&mut hasher);
            source.hash(&mut hasher);
            name.hash(&mut hasher);
            hash_binary_content(data.as_bytes(), &mut hasher);
        }
        ContentBlock::File {
            source, data, name, ..
        } => {
            "file".hash(&mut hasher);
            source.hash(&mut hasher);
            name.hash(&mut hasher);
            hash_binary_content(data.as_bytes(), &mut hasher);
        }
        ContentBlock::ToolDefinitions { tools, .. } => {
            "tool_definitions".hash(&mut hasher);
            tools.len().hash(&mut hasher);
        }
        ContentBlock::Context { data, context_type } => {
            "context".hash(&mut hasher);
            context_type.hash(&mut hasher);
            hash_json_into(data, &mut hasher); // canonical key order
        }
        ContentBlock::Refusal { message } => {
            "refusal".hash(&mut hasher);
            message.hash(&mut hasher);
        }
        ContentBlock::Json { data } => {
            "json".hash(&mut hasher);
            // Structured normalization: a schema-filled answer and the model's raw one are the
            // same answer. See normalize_structured_json_for_hash.
            hash_structured_json_into(data, &mut hasher);
        }
        ContentBlock::Unknown { raw } => {
            "unknown".hash(&mut hasher);
            hash_json_into(raw, &mut hasher); // canonical key order
        }
    }

    hasher.finish()
}

// ============================================================================
// INTERNAL: METADATA
// ============================================================================

/// Compute metadata from processed blocks.
fn compute_metadata(
    blocks: &[BlockEntry],
    span_rows: &[MessageSpanRow],
    replay_matching_complete: bool,
) -> FeedMetadata {
    // Keyed by (trace, span): a span id is unique only within a trace, and a session view holds
    // several traces, so counting by span id alone under-reported the span count.
    let span_ids: HashSet<_> = blocks.iter().map(|b| (&b.trace_id, &b.span_id)).collect();

    // Summed once per span, not once per row. A re-ingested span appears twice in the DuckDB row
    // set - that query reads the raw table, while ClickHouse reads it with FINAL - so summing rows
    // doubled the tokens and cost of a conversation whose spans had been delivered twice, even
    // though the messages themselves are deduplicated and appear once.
    let mut counted: HashSet<(&str, &str)> = HashSet::new();
    let mut total_tokens = 0i64;
    let mut total_cost = 0.0f64;
    for row in span_rows {
        if counted.insert((row.trace_id.as_str(), row.span_id.as_str())) {
            total_tokens += row.total_tokens;
            total_cost += row.cost_total;
        }
    }

    FeedMetadata {
        block_count: blocks.len(),
        span_count: span_ids.len(),
        total_tokens,
        total_cost,
        replay_matching_complete,
    }
}

// ============================================================================
// INTERNAL: DEDUPLICATION
// ============================================================================

/// Deduplicate tool definitions by name, sort alphabetically.
///
/// Strategy:
/// 1. Normalize provider-specific formats to OpenAI-style tool definitions.
/// 2. Merge definitions with the same name to preserve complementary fields.
/// 3. Use quality score only to choose merge base / break ties.
pub fn deduplicate_tools(raw: Vec<JsonValue>) -> Vec<JsonValue> {
    let mut by_name: HashMap<String, JsonValue> = HashMap::with_capacity(raw.len());

    for def in raw {
        let normalized = normalize_tools(&def);
        let defs = match normalized {
            JsonValue::Array(arr) => arr,
            single => vec![single],
        };

        for tool in defs {
            let canonical = canonicalize_tool_definition(tool);
            if let Some(name) = extract_tool_name(&canonical) {
                by_name
                    .entry(name)
                    .and_modify(|existing| {
                        let merged = merge_tool_definitions(existing.clone(), canonical.clone());
                        *existing = merged;
                    })
                    .or_insert(canonical);
            }
        }
    }

    let mut tools: Vec<(String, JsonValue)> = by_name.into_iter().collect();
    tools.sort_by(|a, b| a.0.cmp(&b.0));
    tools.into_iter().map(|(_, def)| def).collect()
}

fn canonicalize_tool_definition(tool: JsonValue) -> JsonValue {
    if tool.get("function").is_some() {
        return tool;
    }

    let Some(name) = tool.get("name").and_then(|n| n.as_str()) else {
        return tool;
    };
    let mut function = json!({ "name": name });
    if let Some(desc) = tool.get("description") {
        function["description"] = desc.clone();
    }
    if let Some(params) = tool
        .get("parameters")
        .or_else(|| tool.get("input_schema"))
        .or_else(|| tool.get("inputSchema"))
    {
        function["parameters"] = params.clone();
    }

    let mut canonical = json!({
        "type": "function",
        "function": function
    });
    if let Some(strict) = tool.get("strict") {
        canonical["strict"] = strict.clone();
    }
    canonical
}

fn function_map(def: &JsonValue) -> Option<&serde_json::Map<String, JsonValue>> {
    def.get("function")
        .and_then(|f| f.as_object())
        .or_else(|| def.as_object())
}

fn function_map_mut(def: &mut JsonValue) -> Option<&mut serde_json::Map<String, JsonValue>> {
    if def.get("function").and_then(|f| f.as_object()).is_some() {
        return def.get_mut("function").and_then(|f| f.as_object_mut());
    }
    def.as_object_mut()
}

fn is_weak_description(desc: &str) -> bool {
    let d = desc.trim();
    d.is_empty()
        || d.eq_ignore_ascii_case("none")
        || d.eq_ignore_ascii_case("n/a")
        || d.eq_ignore_ascii_case("unknown")
        || d.eq_ignore_ascii_case("no description")
}

fn merge_tool_definitions(a: JsonValue, b: JsonValue) -> JsonValue {
    let qa = tool_definition_quality(&a);
    let qb = tool_definition_quality(&b);

    let (mut primary, secondary) = if qb > qa { (b, a) } else { (a, b) };

    let secondary_func = function_map(&secondary).cloned();
    let Some(secondary_func) = secondary_func else {
        return primary;
    };

    let Some(primary_func) = function_map_mut(&mut primary) else {
        return primary;
    };

    if let Some(secondary_desc) = secondary_func.get("description").and_then(|d| d.as_str()) {
        let primary_desc = primary_func
            .get("description")
            .and_then(|d| d.as_str())
            .unwrap_or("");
        if is_weak_description(primary_desc) && !is_weak_description(secondary_desc) {
            primary_func.insert(
                "description".to_string(),
                JsonValue::String(secondary_desc.to_string()),
            );
        }
    }

    if let Some(secondary_params) = secondary_func.get("parameters") {
        match primary_func.get_mut("parameters") {
            Some(primary_params) => merge_json_schema(primary_params, secondary_params),
            None => {
                primary_func.insert("parameters".to_string(), secondary_params.clone());
            }
        }
    }

    if let Some(strict_val) = secondary.get("strict").and_then(|v| v.as_bool())
        && strict_val
    {
        primary["strict"] = JsonValue::Bool(true);
    }

    primary
}

fn merge_json_schema(primary: &mut JsonValue, secondary: &JsonValue) {
    let (Some(primary_obj), Some(secondary_obj)) = (primary.as_object_mut(), secondary.as_object())
    else {
        if primary.is_null() && !secondary.is_null() {
            *primary = secondary.clone();
        }
        return;
    };

    for (key, secondary_val) in secondary_obj {
        match key.as_str() {
            "properties" => merge_properties(primary_obj, secondary_val),
            "required" => merge_required(primary_obj, secondary_val),
            _ => match primary_obj.get_mut(key) {
                Some(primary_val) => {
                    if primary_val.is_null() {
                        *primary_val = secondary_val.clone();
                    } else if primary_val.is_object() && secondary_val.is_object() {
                        merge_json_schema(primary_val, secondary_val);
                    }
                }
                None => {
                    primary_obj.insert(key.clone(), secondary_val.clone());
                }
            },
        }
    }
}

fn merge_properties(
    primary_obj: &mut serde_json::Map<String, JsonValue>,
    secondary_props: &JsonValue,
) {
    let Some(secondary_props_obj) = secondary_props.as_object() else {
        return;
    };

    match primary_obj.get_mut("properties") {
        Some(JsonValue::Object(primary_props_obj)) => {
            for (prop_name, secondary_prop) in secondary_props_obj {
                match primary_props_obj.get_mut(prop_name) {
                    Some(primary_prop) => merge_property_schema(primary_prop, secondary_prop),
                    None => {
                        primary_props_obj.insert(prop_name.clone(), secondary_prop.clone());
                    }
                }
            }
        }
        _ => {
            primary_obj.insert(
                "properties".to_string(),
                JsonValue::Object(secondary_props_obj.clone()),
            );
        }
    }
}

fn merge_property_schema(primary_prop: &mut JsonValue, secondary_prop: &JsonValue) {
    let (Some(primary_obj), Some(secondary_obj)) =
        (primary_prop.as_object_mut(), secondary_prop.as_object())
    else {
        if primary_prop.is_null() && !secondary_prop.is_null() {
            *primary_prop = secondary_prop.clone();
        }
        return;
    };

    for (key, secondary_val) in secondary_obj {
        match primary_obj.get_mut(key) {
            Some(primary_val) => {
                if key == "description" {
                    let current = primary_val.as_str().unwrap_or("");
                    let incoming = secondary_val.as_str().unwrap_or("");
                    if is_weak_description(current) && !is_weak_description(incoming) {
                        *primary_val = JsonValue::String(incoming.to_string());
                    }
                    continue;
                }

                if primary_val.is_null() {
                    *primary_val = secondary_val.clone();
                } else if primary_val.is_object() && secondary_val.is_object() {
                    merge_json_schema(primary_val, secondary_val);
                }
            }
            None => {
                primary_obj.insert(key.clone(), secondary_val.clone());
            }
        }
    }
}

fn merge_required(primary_obj: &mut serde_json::Map<String, JsonValue>, secondary_req: &JsonValue) {
    let Some(secondary_arr) = secondary_req.as_array() else {
        return;
    };

    let mut merged: Vec<JsonValue> = primary_obj
        .get("required")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    for req in secondary_arr {
        if !merged.iter().any(|r| r == req) {
            merged.push(req.clone());
        }
    }

    if !merged.is_empty() {
        primary_obj.insert("required".to_string(), JsonValue::Array(merged));
    }
}

/// Deduplicate tool names, sort alphabetically.
pub fn deduplicate_names(raw: Vec<String>) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::with_capacity(raw.len());
    let mut names: Vec<String> = Vec::with_capacity(raw.len());

    for name in raw {
        if seen.insert(name.clone()) {
            names.push(name);
        }
    }

    names.sort();
    names
}

#[cfg(test)]
mod tests;
