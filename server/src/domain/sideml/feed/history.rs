//! History Detection for Feed Pipeline
//!
//! This module detects and marks historical/intermediate content that should
//! be filtered from the conversation timeline.
//!
//! # Design Principles
//!
//! 1. **No cross-trace deduplication**: Different traces in a session can
//!    legitimately have the same message content. All deduplication happens
//!    within a single trace only.
//!
//! 2. **Tool linking via tool_use_id**: Tool calls and results are linked.
//!    If a tool_use is history, its tool_result is also history.
//!
//! 3. **Universal signals**: Detection uses OTel conventions, span structure,
//!    and timestamps - not framework-specific logic.
//!
//! # What is "History"?
//!
//! In AI agent traces, the same content often appears multiple times:
//! - **Session history**: Previous turns re-sent as context to LLM calls
//! - **Context copies**: Parent span messages duplicated in child spans
//! - **Intermediate output**: Non-final responses during tool-use loops
//!
//! # Detection Strategy
//!
//! The algorithm uses multiple signals to identify history:
//!
//! 1. **Protected = Current**: GenAIChoice, finish_reason → always kept
//! 2. **Timestamp-based**: Message timestamp < span start → historical context
//! 3. **Tool linking**: Tool_results are current iff their tool_use_id is current
//! 4. **Intermediate filtering**: Assistant text in generation spans (when agent
//!    spans exist) without finish_reason → intermediate output
//!
//! # Eight-Phase Detection
//!
//! 1. **Build current tool_use_id set**: From protected tool_uses and agent spans
//! 2. **Timestamp-based**: Mark messages with timestamp < span_start
//! 3. **Accumulator span input**: Mark input events from non-root accumulator spans
//! 4. **Intermediate text**: Mark assistant text from generation spans (when has_agent_spans)
//!    - **(4b) Input-source assistant**: Mark assistant from input attrs in non-root gen spans
//! 5. **Multi-turn history**: Mark all unprotected content in generation spans with tool_results
//! 6. **Orphan tool_results**: Mark tool_results with unknown tool_use_id
//! 7. **Deduplication**: Mark duplicate content by identity (keep earliest)

use std::collections::{HashMap, HashSet};

use super::dedup::{
    SpanTimestamps, compute_tool_call_hash, effective_timestamp, hash_tool_result_content_into,
};
use super::types::BlockEntry;
use crate::domain::sideml::types::{ChatRole, ContentBlock};

// ============================================================================
// TOOL USE ID MAP
// ============================================================================

/// Build a map of tool_use_ids to their "current" status, **per trace**.
///
/// A tool_use is "current" (not history) if it appears in a protected block
/// (GenAIChoice or finish_reason). This identifies tool calls that are part of
/// the current turn's LLM output.
///
/// Returns: Map of trace_id -> Set of tool_use_ids that are current (not history)
///
/// IMPORTANT: This must be per-trace to avoid cross-trace contamination when
/// processing sessions. Tool_use_ids from previous traces should not be
/// considered "current" for subsequent traces.
///
/// NOTE: We intentionally use ONLY protected blocks (not all agent span blocks).
/// Event-based frameworks (Strands) bubble ALL events (including historical ones
/// from previous turns) up to the root agent span. Including agent span blocks
/// would incorrectly collect historical tool_use_ids as "current", causing
/// Phase 6 to skip marking their tool_results as orphans.
fn build_current_tool_use_ids(blocks: &[BlockEntry]) -> HashMap<String, HashSet<String>> {
    let mut map: HashMap<String, HashSet<String>> = HashMap::new();

    for block in blocks {
        // Only protected tool_uses (gen_ai.choice or finish_reason) are current.
        // Agent span tool_uses are excluded because bubbled-up historical events
        // would contaminate the set with tool_use_ids from previous turns.
        if !block.is_protected() {
            continue;
        }

        if let ContentBlock::ToolUse { id: Some(id), .. } = &block.content {
            map.entry(block.trace_id.clone())
                .or_default()
                .insert(id.clone());
        }
    }

    map
}

/// Session history detection result.
#[derive(Debug, Default)]
struct SessionHistoryInfo {
    /// Has agent spans (Strands-like structure)
    has_agent_spans: bool,
    /// Has event-based messages (Strands pattern with gen_ai.* events)
    /// When true, Phase 4 intermediate filtering applies (events bubble up)
    /// When false, generation spans hold authoritative output (LangGraph pattern)
    has_event_based_messages: bool,
    /// Traces that have multi-turn history (tool_results in generation spans)
    /// IMPORTANT: This is per-trace to avoid cross-trace contamination
    traces_with_multi_turn_history: HashSet<String>,
}

/// Check if traces have session history.
///
/// Returns info about what kind of history exists:
/// - `has_agent_spans`: Strands-like structure with authoritative root
/// - `has_event_based_messages`: Whether trace uses event-based (Strands) or
///   attribute-based (LangGraph) message pattern
/// - `traces_with_multi_turn_history`: Per-trace detection of multi-turn history
///
/// IMPORTANT: Multi-turn history detection must be per-trace. A trace has
/// multi-turn history if it has tool_results in generation spans, which indicates
/// the LLM was sent previous turn context.
fn detect_session_history(blocks: &[BlockEntry]) -> SessionHistoryInfo {
    let has_agent_spans = blocks.iter().any(BlockEntry::is_agent_span);

    // Detect event-based vs attribute-based message pattern
    // Event-based (Strands): Messages come from gen_ai.* events, bubble up to root
    // Attribute-based (LangGraph): Messages in llm.output_messages attributes, gen spans authoritative
    let has_event_based_messages = blocks
        .iter()
        .any(|b| b.is_from_event() && (b.is_output_event() || b.is_input_event()));

    // Detect multi-turn history PER TRACE
    // A trace has multi-turn history if it has tool_results in generation spans
    let mut traces_with_multi_turn_history = HashSet::new();

    // Only detect multi-turn history for event-based frameworks
    // Attribute-based frameworks don't have this pattern
    if has_agent_spans && has_event_based_messages {
        for block in blocks {
            if block.is_generation_span() && block.is_tool_result() {
                traces_with_multi_turn_history.insert(block.trace_id.clone());
            }
        }
    }

    SessionHistoryInfo {
        has_agent_spans,
        has_event_based_messages,
        traces_with_multi_turn_history,
    }
}

// ============================================================================
// HISTORY DETECTION
// ============================================================================

/// Mark blocks as history based on universal signals.
///
/// # Algorithm
///
/// 1. Build set of current tool_use_ids (from protected blocks and agent spans)
/// 2. Timestamp-based: Mark messages with timestamp < span_start
/// 3. Accumulator spans: Mark input events from non-root accumulator spans
/// 4. Intermediate text: Mark assistant text from generation spans (when has_agent_spans)
/// 5. Multi-turn: If tool_results in generation, mark all unprotected generation content
/// 6. Orphan tool_results: Mark tool_results with unknown tool_use_id
/// 7. Deduplicate remaining blocks
pub fn mark_history(
    blocks: &mut [BlockEntry],
    span_timestamps: &HashMap<String, SpanTimestamps>,
) -> HistoryStats {
    let mut stats = HistoryStats {
        protected: blocks.iter().filter(|b| b.is_protected()).count(),
        ..Default::default()
    };

    // Phase 1: Detect session history and build tool_use_id map
    let current_tool_ids = build_current_tool_use_ids(blocks);
    let history_info = detect_session_history(blocks);

    tracing::trace!(
        current_tool_ids = current_tool_ids.len(),
        has_agent_spans = history_info.has_agent_spans,
        has_event_based = history_info.has_event_based_messages,
        traces_with_multi_turn = history_info.traces_with_multi_turn_history.len(),
        "history detection: analysis complete"
    );

    // Phase 2: Mark timestamp-based history in child generation spans
    // Messages with timestamp < span_start are historical context passed to the span.
    // This handles both simple history (previous turn) and complex multi-turn history.
    for block in blocks.iter_mut() {
        if block.is_protected() || block.is_history {
            continue;
        }

        // Only child spans (has parent) - root span content is authoritative
        if block.is_root_span() {
            continue;
        }

        // Only generation spans contain session history context
        if !block.is_generation_span() {
            continue;
        }

        // Check if block timestamp is before span start
        if let Some(span_ts) = span_timestamps.get(&block.span_id)
            && block.timestamp < span_ts.span_start
        {
            block.is_history = true;
            stats.generation_history += 1;
            tracing::trace!(
                span_id = %block.span_id,
                block_time = %block.timestamp,
                span_start = %span_ts.span_start,
                "marked as history (timestamp < span_start)"
            );
        }
    }

    // Phase 3: Filter intermediate state from spans
    //
    // This phase handles clear intermediate state that should be filtered:
    // 1. Raw JSON output from chain spans (framework state) - even root
    // 2. Input events from non-root accumulator spans (context copies)
    //
    // We DON'T aggressively filter all input-source content because:
    // - Phase 2 (timestamp) already catches messages predating the span
    // - Phase 7 (dedup) catches duplicate content
    // - Some input sources contain unique authoritative messages
    for block in blocks.iter_mut() {
        if block.is_protected() || block.is_history {
            continue;
        }

        // Tool results from execution should be kept (unless orphan - handled in Phase 6)
        if block.is_tool_result() {
            continue;
        }

        // Raw JSON output from chain spans = framework state, not semantic messages
        // This applies to ALL chain spans including root because:
        // - LangGraph root span output.value = raw graph state
        // - Actual semantic messages are in child generation spans
        // - This must be checked BEFORE the root span skip
        if block.observation_type.as_deref() == Some("chain") && block.entry_type == "json" {
            block.is_history = true;
            stats.accumulator_history += 1;
            continue;
        }

        // Root span content is generally authoritative (except JSON handled above)
        if block.is_root_span() {
            continue;
        }

        // Accumulator spans (agent/chain/span) pass through messages
        // Their input events are context copies, not authoritative
        if block.is_accumulator_span() && block.is_input_event() {
            block.is_history = true;
            stats.accumulator_history += 1;
        }
    }

    // Phase 4: Event-based framework intermediate content filtering
    //
    // For frameworks using OTEL events (gen_ai.choice, gen_ai.user.message):
    // - Events bubble up from child spans to root agent span
    // - Root agent span has authoritative current-turn messages
    // - Child generation span content is intermediate, duplicated at root
    //
    // This phase only applies when BOTH conditions are true:
    // - has_agent_spans: Root span is an agent that collects events
    // - has_event_based_messages: Framework uses gen_ai.* events
    //
    // For attribute-based frameworks (LangGraph, OpenInference):
    // - Generation spans have authoritative output in llm.output_messages
    // - No event bubbling, child generation spans ARE the source of truth
    // - This phase is SKIPPED
    let mut generation_marked_users: Vec<usize> = Vec::new();
    if history_info.has_agent_spans && history_info.has_event_based_messages {
        for (index, block) in blocks.iter_mut().enumerate() {
            if block.is_protected() || block.is_history {
                continue;
            }

            // Only non-root generation spans
            if !block.is_generation_span() || block.is_root_span() {
                continue;
            }

            // Filter based on role (both event and attribute sources are intermediate)
            match block.role {
                // User/System in child generation spans = history context copies
                ChatRole::User | ChatRole::System => {
                    block.is_history = true;
                    stats.generation_history += 1;
                    if block.role == ChatRole::User {
                        generation_marked_users.push(index);
                    }
                }
                // Assistant text/thinking = intermediate output (final at root)
                ChatRole::Assistant if block.is_text() || block.is_thinking() => {
                    block.is_history = true;
                    stats.generation_history += 1;
                }
                // Tool role and ToolUse preserved for matching
                _ => {}
            }
        }
    }

    // Phase 4b: Input-source assistant history
    //
    // For attribute-based frameworks (ADK, Vercel, LiveKit, etc.):
    // A non-root generation span's INPUT attributes (e.g. llm_request) re-send
    // previous assistant responses as context. The current response comes from
    // OUTPUT attributes (e.g. llm_response / gen_ai.choice).
    //
    // This phase marks assistant content from input sources in non-root generation
    // spans as history. It catches re-sent assistant text/thinking/tool_use that
    // Phase 7 (hash dedup) misses because the LLM regenerates different text.
    for block in blocks.iter_mut() {
        if block.is_protected() || block.is_history {
            continue;
        }
        if !block.is_generation_span() || block.is_root_span() {
            continue;
        }
        if block.role != ChatRole::Assistant {
            continue;
        }
        if block.is_from_event() {
            continue;
        }
        if !block.is_input_source() {
            continue;
        }
        block.is_history = true;
        stats.input_source_history += 1;
    }

    // Phase 5: Multi-turn history - filter ALL unprotected generation span content
    // When tool_results exist in generation spans, it indicates full history re-send
    // IMPORTANT: Check per-trace to avoid cross-trace contamination
    for block in blocks.iter_mut() {
        if block.is_protected() || block.is_history {
            continue;
        }

        if !block.is_generation_span() {
            continue;
        }

        // Only filter if THIS trace has multi-turn history
        if !history_info
            .traces_with_multi_turn_history
            .contains(&block.trace_id)
        {
            continue;
        }

        block.is_history = true;
        stats.generation_history += 1;
    }

    // Phase 6: Mark orphan tool_results
    // Tool_results whose tool_use_id is not in current set FOR THE SAME TRACE are history
    // IMPORTANT: Check against the same trace's tool_use_ids only to avoid cross-trace contamination
    // IMPORTANT: Only applies to traces with multi-turn history
    for block in blocks.iter_mut() {
        if block.is_protected() || block.is_history {
            continue;
        }

        // Only for traces with multi-turn history
        if !history_info
            .traces_with_multi_turn_history
            .contains(&block.trace_id)
        {
            continue;
        }

        // Only applies to tool_results with a tool_use_id the framework itself sent. A
        // correlated id was taken from a call in this same trace, so "names no current call"
        // cannot be read as "belongs to a past turn" - before correlation ran this early, such
        // results reached this phase with no id and were skipped, and they must stay skipped.
        if block.tool_use_id_correlated {
            continue;
        }
        let tool_use_id = match &block.content {
            ContentBlock::ToolResult {
                tool_use_id: Some(id),
                ..
            } => id,
            _ => continue,
        };

        // If tool_use_id not in current set FOR THIS TRACE, it's orphan
        let trace_tool_ids = current_tool_ids.get(&block.trace_id);
        let is_orphan = trace_tool_ids
            .map(|ids| !ids.contains(tool_use_id))
            .unwrap_or(true); // No tool_ids for this trace = all are orphan

        if is_orphan {
            block.is_history = true;
            stats.orphan_tool_results += 1;
            tracing::trace!(
                span_id = %block.span_id,
                trace_id = %block.trace_id,
                tool_use_id = %tool_use_id,
                "marked as history (orphan tool_result)"
            );
        }
    }

    // Phase 6b: a turn that happened must survive somewhere.
    //
    // Phases 3 and 4 both rest on the same assumption - that a copy on the *root* agent span is the
    // authoritative one, so a child's copy is an intermediate duplicate. Where that holds, marking the
    // child costs nothing. `strands-js/swarm` is where it does not: its root agent span carries
    // `system, assistant` and never re-lists the user's request, so phase 4 marked the chat span's copy
    // and phase 3 the accumulator's, every copy became history, and the history-only filter dropped the
    // class entirely. The trace and the feed showed a plan with no request, while one span view still
    // displayed it.
    //
    // So one user witness is kept when nothing non-history is left to carry the turn. Two conditions,
    // and the second is what makes it safe:
    //
    // - it was marked by the **child-generation** phase specifically, not by the accumulator phase. That
    //   matters because a span view loads one span, where "nothing else carries the turn" is trivially
    //   true - rescuing accumulator-marked blocks there gave langgraph's `tools` span views a message
    //   they had never shown. The generation phase only runs when an agent span is in scope, so it is
    //   scope-safe by construction;
    // - the block's own time is **at or after** its span's start, which is what distinguishes an input
    //   this span was given from a previous turn re-sent into it. A genuine re-send predates the span it
    //   was sent to, so this rescue cannot resurrect one;
    // - nothing non-history in the trace already carries that role, so where the root copy *does* exist
    //   this changes nothing at all.
    //
    // The earliest surviving candidate is chosen, and only one, so a trace whose question was re-sent to
    // nine generation spans still shows it once.
    let traces_needing_a_user: HashSet<String> = {
        let mut with = HashSet::new();
        let mut without: HashSet<String> = HashSet::new();
        for block in blocks.iter() {
            if block.role != ChatRole::User {
                continue;
            }
            if block.is_history {
                without.insert(block.trace_id.clone());
            } else {
                with.insert(block.trace_id.clone());
            }
        }
        without.difference(&with).cloned().collect()
    };
    if !traces_needing_a_user.is_empty() {
        let mut rescued: HashSet<String> = HashSet::new();
        let mut candidates: Vec<usize> = generation_marked_users
            .iter()
            .copied()
            .filter(|&i| {
                let block = &blocks[i];
                block.is_history
                    && block.role == ChatRole::User
                    && traces_needing_a_user.contains(&block.trace_id)
                    && span_timestamps
                        .get(&block.span_id)
                        .is_none_or(|t| block.timestamp >= t.span_start)
            })
            .collect();
        candidates.sort_by_key(|&i| (blocks[i].timestamp, blocks[i].span_id.clone()));
        for i in candidates {
            if rescued.insert(blocks[i].trace_id.clone()) {
                blocks[i].is_history = false;
                tracing::debug!(
                    span_id = %blocks[i].span_id,
                    trace_id = %blocks[i].trace_id,
                    "kept a user turn that every phase had marked history - nothing else carried it"
                );
            }
        }
    }

    // Phase 7: Deduplicate remaining blocks
    let duplicate_indices = find_duplicate_indices(blocks, span_timestamps);
    for idx in duplicate_indices {
        blocks[idx].is_history = true;
        stats.duplicates += 1;
    }

    tracing::trace!(
        protected = stats.protected,
        accumulator = stats.accumulator_history,
        generation = stats.generation_history,
        input_source = stats.input_source_history,
        orphan_results = stats.orphan_tool_results,
        duplicates = stats.duplicates,
        "history detection complete"
    );

    stats
}

/// The key Phase 7 groups by: what makes two blocks the same message.
///
/// Content for everything except a tool result, which is keyed by the *call it answers*.
#[derive(PartialEq, Eq, Hash)]
enum DuplicateKey<'a> {
    Content(&'a str, &'a str),
    /// (trace_id, hash of the answered call's name + input, hash of the result text)
    ///
    /// The result's *text*, not its whole block identity. The block identity also covers the tool
    /// name and the error flag, which is what distinguishes two results that name no call - but
    /// once the call is known those are redundant, and a framework that includes the tool name on
    /// the original and omits it on the re-send would produce two keys for one message.
    ToolResultForCall(&'a str, u64, u64),
}

/// Hash a tool result's text alone, for comparing two results of the same call.
fn hash_tool_result_text(content: &serde_json::Value) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hash_tool_result_content_into(content, &mut hasher);
    hasher.finish()
}

/// Tool-call identity, by call id, for every call in the block set.
///
/// A history re-send regenerates the call id but not the name or input, so two re-sends of one
/// call map to the same hash - which is what lets a re-sent result be recognised as a duplicate
/// while two results answering genuinely different calls are not.
fn tool_call_identities(blocks: &[BlockEntry]) -> HashMap<(&str, &str), u64> {
    let mut identities = HashMap::new();
    for block in blocks {
        if let ContentBlock::ToolUse { id, name, input } = &block.content
            && let Some(id) = id.as_deref().filter(|s| !s.is_empty())
        {
            identities.insert(
                (block.trace_id.as_str(), id),
                compute_tool_call_hash(name, input),
            );
        }
    }
    identities
}

/// Find indices of duplicate blocks that should be marked as history.
fn find_duplicate_indices(
    blocks: &[BlockEntry],
    span_timestamps: &HashMap<String, SpanTimestamps>,
) -> Vec<usize> {
    let call_identities = tool_call_identities(blocks);
    // The same call rank the final dedup uses, so the two stages agree about what "the same message"
    // is. Without it this phase marked the second of two identical calls in one response as history,
    // and the final dedup then had nothing to keep it for.
    let ordinals = super::dedup::call_repeat_ordinals(blocks);
    let mut blocks_by_key: HashMap<(DuplicateKey<'_>, u32), Vec<usize>> = HashMap::new();

    for (idx, block) in blocks.iter().enumerate() {
        if block.is_protected() || block.is_history {
            continue;
        }
        // A tool result is keyed by the call it answers *and* its text, not by text alone.
        //
        // Keyed by text alone, two results with the same text collapsed into one - and identical
        // text is ordinary ("ok", "[]", the same search hit). Keyed by the call alone, two calls
        // that happen to be identical (same tool, same input, run twice) collapsed their two
        // different results into one. Both parts are needed:
        //
        // - Strands re-sends one call's result with a regenerated id: same call identity, same
        //   text -> collapsed, which is why this phase exists.
        // - Two different calls returning the same text: different call identity -> both kept.
        // - One call shape run twice returning different text: same identity, different text ->
        //   both kept.
        //
        // Falls back to text when the result names no call in this trace, the only signal left.
        let key = match &block.content {
            ContentBlock::ToolResult {
                tool_use_id,
                content,
                ..
            } => tool_use_id
                .as_deref()
                .filter(|s| !s.is_empty())
                .and_then(|id| call_identities.get(&(block.trace_id.as_str(), id)))
                .map(|&call_hash| {
                    DuplicateKey::ToolResultForCall(
                        &block.trace_id,
                        call_hash,
                        hash_tool_result_text(content),
                    )
                })
                .unwrap_or(DuplicateKey::Content(&block.trace_id, &block.content_hash)),
            _ => DuplicateKey::Content(&block.trace_id, &block.content_hash),
        };
        blocks_by_key
            .entry((key, ordinals[idx]))
            .or_default()
            .push(idx);
    }

    let mut to_mark = Vec::new();

    for indices in blocks_by_key.into_values() {
        if indices.len() <= 1 {
            continue;
        }

        // Sort: output-source DESC, uses_span_end DESC, then timestamp ASC
        // Output-source blocks are preferred over input-source copies to ensure
        // Phase 7 keeps the authoritative version (e.g. llm_response over llm_request)
        let mut sorted: Vec<_> = indices
            .iter()
            .map(|&i| {
                let is_output = blocks[i].is_output_source();
                let uses_span_end = blocks[i].uses_span_end;
                let effective = effective_timestamp(&blocks[i], span_timestamps);
                (i, is_output, uses_span_end, effective)
            })
            .collect();

        sorted.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| a.3.cmp(&b.3))
        });

        // Keep first (best), mark others
        to_mark.extend(sorted.into_iter().skip(1).map(|(idx, _, _, _)| idx));
    }

    to_mark
}

// ============================================================================
// STATISTICS
// ============================================================================

/// Statistics from history detection.
#[derive(Debug, Default)]
pub struct HistoryStats {
    /// Blocks protected from filtering
    pub protected: usize,
    /// Accumulator span input events (history context)
    pub accumulator_history: usize,
    /// Generation span history (session history context)
    pub generation_history: usize,
    /// Input-source assistant history (Phase 4b)
    pub input_source_history: usize,
    /// Orphan tool results (tool_use_id not in current set)
    pub orphan_tool_results: usize,
    /// Duplicate content within trace
    pub duplicates: usize,
}

impl HistoryStats {
    /// Total blocks marked as history.
    pub fn total_history(&self) -> usize {
        self.accumulator_history
            + self.generation_history
            + self.input_source_history
            + self.orphan_tool_results
            + self.duplicates
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::types::MessageCategory;
    use crate::domain::sideml::provenance::PositionPath;
    use crate::domain::sideml::types::FinishReason;
    use chrono::Utc;

    fn make_block(
        entry_type: &str,
        observation_type: Option<&str>,
        event_name: Option<&str>,
        category: MessageCategory,
        finish_reason: Option<FinishReason>,
    ) -> BlockEntry {
        let content = match entry_type {
            "tool_use" => ContentBlock::ToolUse {
                id: Some("call_1".to_string()),
                name: "test".to_string(),
                input: serde_json::json!({}),
            },
            "tool_result" => ContentBlock::ToolResult {
                tool_use_id: Some("call_1".to_string()),
                name: None,
                content: serde_json::json!("result"),
                is_error: false,
            },
            _ => ContentBlock::Text {
                text: "test".to_string(),
            },
        };

        BlockEntry {
            position: PositionPath::default(),
            entry_type: entry_type.to_string(),
            content,
            role: ChatRole::Assistant,
            trace_id: "trace1".to_string(),
            span_id: "span1".to_string(),
            session_id: None,
            message_index: 0,
            entry_index: 0,
            parent_span_id: Some("parent".to_string()),
            span_path: vec!["span1".to_string()],
            timestamp: Utc::now(),
            order_time: Utc::now(),
            observation_type: observation_type.map(String::from),
            model: None,
            provider: None,
            name: None,
            finish_reason,
            tool_use_id: None,
            tool_name: None,
            tokens: None,
            cost: None,
            status_code: None,
            is_error: false,
            source_type: "event".to_string(),
            event_name: event_name.map(String::from),
            source_attribute: None,
            category,
            content_hash: "hash".to_string(),
            is_semantic: true,
            uses_span_end: false,
            is_history: false,
            tool_use_id_correlated: false,
            promoted_to_span_output: false,
        }
    }

    #[test]
    fn test_gen_ai_choice_is_protected() {
        let block = make_block(
            "text",
            Some("generation"),
            Some("gen_ai.choice"),
            MessageCategory::GenAIChoice,
            Some(FinishReason::Stop),
        );
        assert!(block.is_protected());
    }

    #[test]
    fn test_finish_reason_is_protected() {
        let block = make_block(
            "text",
            Some("generation"),
            None,
            MessageCategory::GenAIAssistantMessage,
            Some(FinishReason::Stop),
        );
        assert!(block.is_protected());
    }

    #[test]
    fn test_intermediate_text_is_not_protected() {
        let block = make_block(
            "text",
            Some("generation"),
            None,
            MessageCategory::GenAIAssistantMessage,
            None,
        );
        assert!(!block.is_protected());
    }

    use std::sync::atomic::{AtomicU32, Ordering};
    static BLOCK_COUNTER: AtomicU32 = AtomicU32::new(0);

    fn make_block_with_source(
        entry_type: &str,
        observation_type: Option<&str>,
        event_name: Option<&str>,
        source_type: &str,
        category: MessageCategory,
        role: ChatRole,
    ) -> BlockEntry {
        let counter = BLOCK_COUNTER.fetch_add(1, Ordering::SeqCst);
        let content = match entry_type {
            "tool_use" => ContentBlock::ToolUse {
                id: Some(format!("call_{counter}")),
                name: "test".to_string(),
                input: serde_json::json!({}),
            },
            "tool_result" => ContentBlock::ToolResult {
                tool_use_id: Some(format!("call_{counter}")),
                name: None,
                content: serde_json::json!("result"),
                is_error: false,
            },
            _ => ContentBlock::Text {
                text: format!("test_{counter}"),
            },
        };

        BlockEntry {
            position: PositionPath::default(),
            entry_type: entry_type.to_string(),
            content,
            role,
            trace_id: "trace1".to_string(),
            span_id: format!("span_{counter}"),
            session_id: None,
            message_index: 0,
            entry_index: 0,
            parent_span_id: Some("parent".to_string()),
            span_path: vec![format!("span_{counter}")],
            timestamp: Utc::now(),
            order_time: Utc::now(),
            observation_type: observation_type.map(String::from),
            model: None,
            provider: None,
            name: None,
            finish_reason: None,
            tool_use_id: None,
            tool_name: None,
            tokens: None,
            cost: None,
            status_code: None,
            is_error: false,
            source_type: source_type.to_string(),
            event_name: event_name.map(String::from),
            source_attribute: None,
            category,
            content_hash: format!("hash_{counter}"),
            is_semantic: true,
            uses_span_end: false,
            is_history: false,
            tool_use_id_correlated: false,
            promoted_to_span_output: false,
        }
    }

    #[test]
    fn test_detect_event_based_strands() {
        // Strands pattern: has agent spans + event-based messages (gen_ai.choice)
        let blocks = vec![
            make_block_with_source(
                "text",
                Some("agent"),
                None,
                "attribute",
                MessageCategory::GenAIUserMessage,
                ChatRole::User,
            ),
            make_block_with_source(
                "text",
                Some("generation"),
                Some("gen_ai.choice"),
                "event",
                MessageCategory::GenAIChoice,
                ChatRole::Assistant,
            ),
        ];
        let info = detect_session_history(&blocks);
        assert!(info.has_agent_spans);
        assert!(info.has_event_based_messages);
    }

    #[test]
    fn test_detect_attribute_based_langgraph() {
        // LangGraph pattern: has agent spans + attribute-based messages (no gen_ai.* events)
        let blocks = vec![
            make_block_with_source(
                "text",
                Some("agent"),
                None,
                "attribute",
                MessageCategory::GenAIUserMessage,
                ChatRole::User,
            ),
            make_block_with_source(
                "text",
                Some("generation"),
                None, // no event name - from llm.output_messages attribute
                "attribute",
                MessageCategory::GenAIAssistantMessage,
                ChatRole::Assistant,
            ),
        ];
        let info = detect_session_history(&blocks);
        assert!(info.has_agent_spans);
        assert!(!info.has_event_based_messages); // key difference: no event-based messages
    }

    #[test]
    fn test_langgraph_assistant_text_not_marked_history() {
        // LangGraph: assistant text from generation span should NOT be marked as history
        // because it's the actual LLM output, not intermediate
        let mut blocks = vec![
            make_block_with_source(
                "text",
                Some("agent"),
                None,
                "attribute",
                MessageCategory::GenAIUserMessage,
                ChatRole::User,
            ),
            make_block_with_source(
                "text",
                Some("generation"),
                None, // attribute-based, no event
                "attribute",
                MessageCategory::GenAIAssistantMessage,
                ChatRole::Assistant,
            ),
        ];

        // Verify setup: should have agent spans but no event-based messages
        let info = detect_session_history(&blocks);
        assert!(info.has_agent_spans, "should have agent spans");
        assert!(
            !info.has_event_based_messages,
            "should NOT have event-based messages for LangGraph"
        );

        let span_timestamps = HashMap::new();
        mark_history(&mut blocks, &span_timestamps);

        // User message should not be history
        assert!(!blocks[0].is_history, "user message should not be history");
        // Assistant text from generation span should NOT be history in LangGraph
        assert!(
            !blocks[1].is_history,
            "LangGraph assistant text should not be marked as history"
        );
    }

    #[test]
    fn test_strands_assistant_text_marked_history() {
        // Strands: assistant text from non-root generation span IS marked as history
        // because it's intermediate output that bubbles up via events
        let mut blocks = vec![
            make_block_with_source(
                "text",
                Some("agent"),
                None,
                "attribute",
                MessageCategory::GenAIUserMessage,
                ChatRole::User,
            ),
            make_block_with_source(
                "text",
                Some("generation"),
                Some("gen_ai.user.message"), // event-based input
                "event",
                MessageCategory::GenAIUserMessage,
                ChatRole::User,
            ),
            make_block_with_source(
                "text",
                Some("generation"),
                None, // assistant text without protection
                "attribute",
                MessageCategory::GenAIAssistantMessage,
                ChatRole::Assistant,
            ),
        ];

        let span_timestamps = HashMap::new();
        mark_history(&mut blocks, &span_timestamps);

        // Assistant text from generation span IS history in Strands (events bubble up)
        assert!(
            blocks[2].is_history,
            "Strands intermediate assistant text should be marked as history"
        );
    }

    #[test]
    fn test_langgraph_chain_span_json_filtered() {
        // LangGraph "tools" chain node output (JSON with tool results) should be filtered
        // because actual semantic tool_results come from generation spans
        let mut blocks = vec![
            make_block_with_source(
                "text",
                Some("chain"), // root chain span
                None,
                "attribute",
                MessageCategory::GenAIUserMessage,
                ChatRole::User,
            ),
            {
                // Non-root chain span JSON output (LangGraph "tools" node)
                let mut b = make_block_with_source(
                    "json", // raw state output
                    Some("chain"),
                    None,
                    "attribute",
                    MessageCategory::GenAIAssistantMessage,
                    ChatRole::Assistant,
                );
                b.parent_span_id = Some("parent".to_string()); // non-root
                b
            },
            make_block_with_source(
                "tool_result",
                Some("generation"),
                None,
                "attribute",
                MessageCategory::GenAIToolMessage,
                ChatRole::Tool,
            ),
        ];

        // Make first block root span (no parent)
        blocks[0].parent_span_id = None;

        let span_timestamps = HashMap::new();
        mark_history(&mut blocks, &span_timestamps);

        // User message from root should not be history
        assert!(
            !blocks[0].is_history,
            "root user message should not be history"
        );
        // JSON from non-root chain span should be history
        assert!(
            blocks[1].is_history,
            "non-root chain span JSON should be marked as history"
        );
        // Tool result from generation span should not be history
        assert!(!blocks[2].is_history, "tool result should not be history");
    }

    #[test]
    fn test_chain_span_json_filtered_even_root() {
        // JSON output from chain spans should be filtered (framework state)
        // This includes ROOT chain spans because LangGraph root span output.value
        // contains raw graph state, not semantic messages
        let mut blocks = vec![{
            let mut b = make_block_with_source(
                "json",
                Some("chain"),
                None,
                "attribute",
                MessageCategory::GenAIAssistantMessage,
                ChatRole::Assistant,
            );
            b.parent_span_id = None; // root span
            b
        }];

        let span_timestamps = HashMap::new();
        mark_history(&mut blocks, &span_timestamps);

        // Chain span JSON should be history even if root
        assert!(
            blocks[0].is_history,
            "chain span JSON should be marked as history even when root"
        );
    }

    #[test]
    fn test_root_agent_span_text_preserved() {
        // Text output from ROOT agent span should be preserved (actual response)
        let mut blocks = vec![{
            let mut b = make_block_with_source(
                "text",
                Some("agent"),
                None,
                "attribute",
                MessageCategory::GenAIAssistantMessage,
                ChatRole::Assistant,
            );
            b.parent_span_id = None; // root span
            b
        }];

        let span_timestamps = HashMap::new();
        mark_history(&mut blocks, &span_timestamps);

        // Root agent span text should not be history
        assert!(
            !blocks[0].is_history,
            "root agent span text should not be marked as history"
        );
    }

    // ========================================================================
    // PHASE 4b: INPUT-SOURCE ASSISTANT HISTORY TESTS
    // ========================================================================

    #[test]
    fn test_phase4b_marks_input_source_assistant() {
        // ADK pattern: assistant text from llm_request (input) in non-root gen span
        let mut blocks = vec![{
            let mut b = make_block_with_source(
                "text",
                Some("generation"),
                None,
                "attribute",
                MessageCategory::GenAIAssistantMessage,
                ChatRole::Assistant,
            );
            b.source_attribute = Some("gcp.vertex.agent.llm_request".to_string());
            b.parent_span_id = Some("agent_root".to_string());
            b
        }];

        let span_timestamps = HashMap::new();
        let stats = mark_history(&mut blocks, &span_timestamps);

        assert!(
            blocks[0].is_history,
            "input-source assistant should be history"
        );
        assert_eq!(stats.input_source_history, 1);
    }

    #[test]
    fn test_phase4b_skips_output_source_assistant() {
        // Assistant text from llm_response (output) should NOT be marked
        let mut blocks = vec![{
            let mut b = make_block_with_source(
                "text",
                Some("generation"),
                None,
                "attribute",
                MessageCategory::GenAIAssistantMessage,
                ChatRole::Assistant,
            );
            b.source_attribute = Some("gcp.vertex.agent.llm_response".to_string());
            b.parent_span_id = Some("agent_root".to_string());
            b
        }];

        let span_timestamps = HashMap::new();
        let stats = mark_history(&mut blocks, &span_timestamps);

        assert!(
            !blocks[0].is_history,
            "output-source assistant should NOT be history"
        );
        assert_eq!(stats.input_source_history, 0);
    }

    #[test]
    fn test_phase4b_skips_protected_blocks() {
        // Protected blocks (finish_reason) should never be marked by Phase 4b
        let mut blocks = vec![{
            let mut b = make_block_with_source(
                "text",
                Some("generation"),
                None,
                "attribute",
                MessageCategory::GenAIAssistantMessage,
                ChatRole::Assistant,
            );
            b.source_attribute = Some("gcp.vertex.agent.llm_request".to_string());
            b.parent_span_id = Some("agent_root".to_string());
            b.finish_reason = Some(FinishReason::Stop);
            b
        }];

        let span_timestamps = HashMap::new();
        mark_history(&mut blocks, &span_timestamps);

        assert!(
            !blocks[0].is_history,
            "protected block should NOT be marked by Phase 4b"
        );
    }

    #[test]
    fn test_phase4b_skips_user_role() {
        // User messages from input source should NOT be marked (they're current turn prompts)
        let mut blocks = vec![{
            let mut b = make_block_with_source(
                "text",
                Some("generation"),
                None,
                "attribute",
                MessageCategory::GenAIUserMessage,
                ChatRole::User,
            );
            b.source_attribute = Some("gcp.vertex.agent.llm_request".to_string());
            b.parent_span_id = Some("agent_root".to_string());
            b
        }];

        let span_timestamps = HashMap::new();
        mark_history(&mut blocks, &span_timestamps);

        assert!(
            !blocks[0].is_history,
            "user role should NOT be marked by Phase 4b"
        );
    }

    #[test]
    fn test_phase4b_skips_root_span() {
        // Root span input-source assistant should NOT be marked (root is authoritative)
        let mut blocks = vec![{
            let mut b = make_block_with_source(
                "text",
                Some("generation"),
                None,
                "attribute",
                MessageCategory::GenAIAssistantMessage,
                ChatRole::Assistant,
            );
            b.source_attribute = Some("gcp.vertex.agent.llm_request".to_string());
            b.parent_span_id = None; // root span
            b
        }];

        let span_timestamps = HashMap::new();
        mark_history(&mut blocks, &span_timestamps);

        assert!(
            !blocks[0].is_history,
            "root span should NOT be marked by Phase 4b"
        );
    }

    #[test]
    fn test_phase4b_skips_event_source() {
        // Event-sourced blocks are handled by Phase 4, not 4b
        let mut blocks = vec![{
            let mut b = make_block_with_source(
                "text",
                Some("generation"),
                Some("gen_ai.assistant.message"),
                "event",
                MessageCategory::GenAIAssistantMessage,
                ChatRole::Assistant,
            );
            b.parent_span_id = Some("agent_root".to_string());
            b
        }];

        let span_timestamps = HashMap::new();
        let stats = mark_history(&mut blocks, &span_timestamps);

        assert_eq!(
            stats.input_source_history, 0,
            "event source should not be counted in Phase 4b"
        );
    }

    // ========================================================================
    // PHASE 6: ORPHAN TOOL RESULT TESTS (Strands JS multi-turn history)
    // ========================================================================

    /// Reproduces the Strands JS bug where historical tool_use events bubble up
    /// to the root agent span, causing their tool_use_ids to be collected as
    /// "current" and preventing Phase 6 from marking their tool_results as orphans.
    ///
    /// Scenario: trace has NYC turn (history) + London turn (current).
    /// The root agent span has both NYC and London tool_use events (due to bubbling).
    /// Only the London tool_use appears in gen_ai.choice → only London is "current".
    /// NYC tool_result should be orphan (its tool_use_id not in gen_ai.choice).
    #[test]
    fn test_phase6_historical_tool_results_are_orphans_not_bubbled_agent_span() {
        // Simulate: root agent span has NYC tool_use (historical, bubbled) +
        //           London gen_ai.choice with London tool_use (current/protected)
        // execute_agent_loop_cycle has NYC tool_result + London tool_result

        let nyc_tool_use_id = "tooluse_NYC_historical";
        let london_tool_use_id = "tooluse_London_current";

        // Protected gen_ai.choice on root agent span — contains London tool_use
        let mut choice_block = make_block_with_source(
            "tool_use",
            Some("agent"),
            Some("gen_ai.choice"),
            "event",
            MessageCategory::GenAIChoice,
            ChatRole::Assistant,
        );
        if let ContentBlock::ToolUse { id, name, .. } = &mut choice_block.content {
            *id = Some(london_tool_use_id.to_string());
            *name = "weather_forecast".to_string();
        }
        choice_block.parent_span_id = None; // root span
        choice_block.finish_reason = Some(FinishReason::ToolUse);

        // Historical NYC tool_use on root agent span (bubbled, NOT protected)
        let mut nyc_tool_use = make_block_with_source(
            "tool_use",
            Some("agent"),
            Some("gen_ai.assistant.message"),
            "event",
            MessageCategory::GenAIAssistantMessage,
            ChatRole::Assistant,
        );
        if let ContentBlock::ToolUse { id, name, .. } = &mut nyc_tool_use.content {
            *id = Some(nyc_tool_use_id.to_string());
            *name = "weather_forecast".to_string();
        }
        nyc_tool_use.parent_span_id = None; // root span

        // NYC tool_result on execute_agent_loop_cycle (non-root agent span, multi-turn history)
        let mut nyc_tool_result = make_block_with_source(
            "tool_result",
            Some("agent"),
            Some("gen_ai.tool.message"),
            "event",
            MessageCategory::GenAIToolMessage,
            ChatRole::Tool,
        );
        if let ContentBlock::ToolResult { tool_use_id, .. } = &mut nyc_tool_result.content {
            *tool_use_id = Some(nyc_tool_use_id.to_string());
        }
        nyc_tool_result.parent_span_id = Some("root_agent".to_string()); // non-root

        // London tool_result on execute_agent_loop_cycle (non-root agent span, current turn)
        let mut london_tool_result = make_block_with_source(
            "tool_result",
            Some("agent"),
            Some("gen_ai.tool.message"),
            "event",
            MessageCategory::GenAIToolMessage,
            ChatRole::Tool,
        );
        if let ContentBlock::ToolResult { tool_use_id, .. } = &mut london_tool_result.content {
            *tool_use_id = Some(london_tool_use_id.to_string());
        }
        london_tool_result.parent_span_id = Some("root_agent".to_string()); // non-root

        // Tool_result in generation span (makes traces_with_multi_turn_history fire)
        let mut gen_tool_result = make_block_with_source(
            "tool_result",
            Some("generation"),
            Some("gen_ai.tool.message"),
            "event",
            MessageCategory::GenAIToolMessage,
            ChatRole::Tool,
        );
        if let ContentBlock::ToolResult { tool_use_id, .. } = &mut gen_tool_result.content {
            *tool_use_id = Some(nyc_tool_use_id.to_string());
        }
        gen_tool_result.parent_span_id = Some("exec_loop".to_string());

        // Gen_ai.choice event on generation span (bubbled) — enables has_event_based_messages
        let mut gen_choice = make_block_with_source(
            "text",
            Some("generation"),
            Some("gen_ai.choice"),
            "event",
            MessageCategory::GenAIChoice,
            ChatRole::Assistant,
        );
        gen_choice.parent_span_id = Some("exec_loop".to_string());
        gen_choice.finish_reason = Some(FinishReason::Stop);

        let mut blocks = vec![
            choice_block,
            nyc_tool_use,
            nyc_tool_result,
            london_tool_result,
            gen_tool_result,
            gen_choice,
        ];

        let span_timestamps = HashMap::new();
        let stats = mark_history(&mut blocks, &span_timestamps);

        // NYC tool_result (index 2) should be orphan — its tool_use_id not in gen_ai.choice
        assert!(
            blocks[2].is_history,
            "NYC tool_result should be marked as orphan (historical tool_use_id not in gen_ai.choice)"
        );

        // London tool_result (index 3) should NOT be orphan — London ID is in gen_ai.choice
        assert!(
            !blocks[3].is_history,
            "London tool_result should NOT be orphan (tool_use_id IS in gen_ai.choice)"
        );

        assert!(stats.orphan_tool_results >= 1, "at least 1 orphan expected");
    }
}

#[cfg(test)]
mod duplicate_key_tests {
    use super::*;
    use crate::domain::sideml::types::ContentBlock;

    /// A re-sent result that drops the tool name must still collapse into the original.
    ///
    /// Once the answered call is known, the tool name and the error flag are redundant: they exist
    /// to tell apart results that name no call. Keying on the full block identity here meant a
    /// framework that includes the name on the original and omits it on the re-send produced two
    /// keys for one message, so the feed showed it twice - and dedup could not collapse them
    /// either, because their regenerated ids differ.
    #[test]
    fn a_resend_that_drops_the_tool_name_still_collapses() {
        let text = serde_json::json!([{"type": "text", "text": "ok"}]);
        let named = ContentBlock::ToolResult {
            tool_use_id: Some("call-1".to_string()),
            name: Some("lookup".to_string()),
            content: text.clone(),
            is_error: false,
        };
        // The re-send: same answer to the same call, with a regenerated id and no tool name.
        let resent = ContentBlock::ToolResult {
            tool_use_id: Some("call-1-regenerated".to_string()),
            name: None,
            content: text.clone(),
            is_error: false,
        };

        // Their block identities differ, which is correct - that is what tells uncorrelated
        // results apart - but the key this phase uses must not.
        assert_ne!(
            crate::domain::sideml::feed::compute_block_hash(&named),
            crate::domain::sideml::feed::compute_block_hash(&resent),
            "the block identity is expected to include the tool name"
        );
        let key_of = |block: &ContentBlock| match block {
            ContentBlock::ToolResult { content, .. } => hash_tool_result_text(content),
            _ => unreachable!(),
        };
        assert_eq!(
            key_of(&named),
            key_of(&resent),
            "a re-send of one call's result must collapse whatever metadata it drops"
        );

        // The text still separates two different answers to the same call.
        let different = ContentBlock::ToolResult {
            tool_use_id: Some("call-1".to_string()),
            name: Some("lookup".to_string()),
            content: serde_json::json!([{"type": "text", "text": "failed"}]),
            is_error: false,
        };
        assert_ne!(key_of(&named), key_of(&different));
    }

    /// Two results that name no call keep the name and error flag in their identity, so a success
    /// and a failure with matching text stay distinct.
    #[test]
    fn uncorrelated_results_are_separated_by_name_and_error() {
        let content = serde_json::json!([{"type": "text", "text": "ok"}]);
        let hash = |name: Option<&str>, is_error: bool| {
            crate::domain::sideml::feed::compute_block_hash(&ContentBlock::ToolResult {
                tool_use_id: None,
                name: name.map(str::to_owned),
                content: content.clone(),
                is_error,
            })
        };
        assert_ne!(hash(Some("lookup"), false), hash(Some("write"), false));
        assert_ne!(hash(Some("lookup"), false), hash(Some("lookup"), true));
        assert_eq!(hash(Some("lookup"), false), hash(Some("lookup"), false));
    }
}
