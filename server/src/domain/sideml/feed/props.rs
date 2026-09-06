//! Property tests for the feed pipeline.
//!
//! The example-based tests in `tests.rs` pin behaviour on message shapes taken from real
//! traces. These check the laws that must hold for *every* input, which is where the
//! pipeline is most exposed: it parses JSON shaped by arbitrary third-party frameworks,
//! deduplicates by content hash, and sorts with a hand-written comparator.
//!
//! Each property fails for a distinct bug class:
//!   - `determinism`          a HashMap iteration order leaking into the response
//!   - `idempotent_dedup`     re-delivered spans inflating the feed
//!   - `no_duplicate_blocks`  one run emitting the same block twice
//!   - `total_order`          an inconsistent comparator, which `sort_by` may panic on
//!   - `never_panics`         malformed framework JSON taking down the endpoint
//!   - `role_filter`          the filter returning rows it should have excluded
//!
//! `feed_generator_is_not_vacuous` guards the generators themselves. An earlier version
//! of this file emitted bare `{"role":..}` objects instead of the
//! `{"source":..,"content":..}` envelope the parser expects, so every case produced an
//! empty feed and every property passed without exercising anything.

use chrono::{DateTime, TimeZone, Utc};
use proptest::prelude::*;
use serde_json::json;

use super::{FeedOptions, process_spans};
use crate::data::types::MessageSpanRow;
use crate::domain::sideml::types::ContentBlock;

const BASE_SECS: i64 = 1_700_000_000;

fn at(offset: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(BASE_SECS + offset, 0)
        .single()
        .expect("fixed timestamp is valid")
}

/// One SideML message in the wire envelope the pipeline consumes, timestamped at the
/// span's own time so it counts as current-turn rather than history.
fn message(role: &str, event: &str, body: serde_json::Value, time: &str) -> serde_json::Value {
    json!({
        "source": {"event": {"name": event, "time": time}},
        "content": {"role": role, "content": body}
    })
}

/// Well-formed payloads covering the shapes that drive the interesting paths: plain text,
/// an assistant tool call, and its matching tool result.
fn well_formed(time: String) -> impl Strategy<Value = String> {
    let (t1, t2, t3, t4) = (time.clone(), time.clone(), time.clone(), time);
    prop_oneof![
        "[a-z ]{1,20}".prop_map(move |text| json!([message(
            "user",
            "gen_ai.user.message",
            json!(text),
            &t1
        )])
        .to_string()),
        "[a-z ]{1,20}".prop_map(move |text| json!([message(
            "assistant",
            "gen_ai.choice",
            json!(text),
            &t2
        )])
        .to_string()),
        "[a-z]{1,8}".prop_map(move |name| json!([message(
            "assistant",
            "gen_ai.choice",
            json!([{"type": "tool_use", "id": "toolu_1", "name": name, "input": {}}]),
            &t3
        )])
        .to_string()),
        "[a-z ]{1,12}".prop_map(move |out| json!([message(
            "tool",
            "gen_ai.tool.message",
            json!([{"type": "tool_result", "tool_use_id": "toolu_1", "content": out}]),
            &t4
        )])
        .to_string()),
    ]
}

/// Payloads the pipeline must tolerate without panicking. Kept separate from
/// `well_formed` so the non-vacuity check can require real messages.
fn malformed() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("[]".to_string()),
        Just("null".to_string()),
        Just("{}".to_string()),
        Just("[".to_string()),
        Just("[{\"source\":".to_string()),
        Just("\"just a string\"".to_string()),
        Just("[{\"source\":{},\"content\":{\"role\":123}}]".to_string()),
        Just("[{\"content\":{\"role\":\"user\"}}]".to_string()),
    ]
}

fn row(trace_seq: u32, span_seq: u32, offset: i64, messages_json: String) -> MessageSpanRow {
    let start = at(offset);
    MessageSpanRow {
        trace_id: format!("trace{trace_seq}"),
        span_id: format!("span{span_seq}"),
        parent_span_id: None,
        span_timestamp: start,
        span_end_timestamp: Some(start),
        messages_json,
        tool_definitions_json: "[]".to_string(),
        tool_names_json: "[]".to_string(),
        model: Some("claude-haiku-4-5".to_string()),
        provider: Some("bedrock".to_string()),
        status_code: None,
        exception_type: None,
        exception_message: None,
        exception_stacktrace: None,
        input_tokens: 10,
        output_tokens: 5,
        total_tokens: 15,
        cost_total: 0.001,
        observation_type: None,
        session_id: None,
        ingested_at: start,
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

prop_compose! {
    fn good_row()(offset in 0i64..300)(
        offset in Just(offset),
        span_seq in 0u32..4,
        trace_seq in 0u32..2,
        msgs in well_formed(at(offset).to_rfc3339()),
    ) -> MessageSpanRow {
        row(trace_seq, span_seq, offset, msgs)
    }
}

prop_compose! {
    fn bad_row()(
        span_seq in 0u32..4,
        trace_seq in 0u32..2,
        offset in 0i64..300,
        msgs in malformed(),
    ) -> MessageSpanRow {
        row(trace_seq, span_seq, offset, msgs)
    }
}

fn good_rows() -> impl Strategy<Value = Vec<MessageSpanRow>> {
    prop::collection::vec(good_row(), 1..6)
}

/// A mix of well-formed and hostile rows, for the robustness properties.
fn any_rows() -> impl Strategy<Value = Vec<MessageSpanRow>> {
    prop::collection::vec(prop_oneof![good_row(), bad_row()], 0..8)
}

proptest! {
    /// Guards every other property here: if the generators stop producing real messages,
    /// the rest pass vacuously.
    #[test]
    fn feed_generator_is_not_vacuous(rows in good_rows()) {
        let out = process_spans(rows, &FeedOptions::new());
        prop_assert!(
            !out.messages.is_empty(),
            "generator produced no messages - the other properties would be vacuous"
        );
    }

    /// Same input, same output. A dedup keyed on a HashMap whose iteration order leaked
    /// into the result would make the messages endpoint flap between requests.
    #[test]
    fn determinism(rows in any_rows()) {
        let opts = FeedOptions::new();
        let a = process_spans(rows.clone(), &opts);
        let b = process_spans(rows, &opts);
        prop_assert_eq!(a.messages.len(), b.messages.len());
        for (x, y) in a.messages.iter().zip(b.messages.iter()) {
            prop_assert_eq!(&x.trace_id, &y.trace_id);
            prop_assert_eq!(&x.span_id, &y.span_id);
            prop_assert_eq!(x.message_index, y.message_index);
            prop_assert_eq!(x.entry_index, y.entry_index);
            prop_assert_eq!(x.role, y.role);
        }
    }

    /// Re-delivering the identical span set must not grow the feed: identity is content
    /// based, so the same span arriving twice is the same message.
    #[test]
    fn idempotent_dedup(rows in good_rows()) {
        let opts = FeedOptions::new();
        let once = process_spans(rows.clone(), &opts);
        let mut doubled = rows.clone();
        doubled.extend(rows);
        let twice = process_spans(doubled, &opts);
        prop_assert_eq!(
            once.messages.len(), twice.messages.len(),
            "duplicating the input changed the feed size"
        );
    }

    /// No two blocks in one feed may be the same block. `idempotent_dedup` compares two
    /// runs and is therefore blind to a bug that inflates both equally; this inspects a
    /// single run, which is what reaches the UI.
    #[test]
    fn no_duplicate_blocks(rows in good_rows()) {
        use std::collections::HashSet;
        let out = process_spans(rows, &FeedOptions::new());
        let mut seen = HashSet::new();
        for m in &out.messages {
            // Role belongs in the identity: a user and an assistant block can share span,
            // indices, entry_type and even content (both " ") and still be two distinct
            // messages. Leaving it out made this property report false duplicates.
            let identity = (
                m.trace_id.clone(),
                m.span_id.clone(),
                m.message_index,
                m.entry_index,
                m.entry_type.clone(),
                m.role.as_str(),
                format!("{:?}", m.content),
            );
            prop_assert!(
                seen.insert(identity),
                "block appears twice in one feed: {} {} idx {}/{}",
                m.trace_id, m.span_id, m.message_index, m.entry_index
            );
        }
    }

    /// Blocks from one span, at one timestamp, must stay in their natural
    /// (message_index, entry_index) order: that is the batch ordering the UI relies on to
    /// render assistant text before the tool call it precedes.
    ///
    /// Scoped to a single span on purpose. `process_spans` does not promise a global
    /// timestamp order across traces - traces are kept contiguous and ordered by group in
    /// `process_feed` - so asserting a global order here would test a contract the
    /// function does not have.
    #[test]
    fn total_order(rows in any_rows()) {
        let out = process_spans(rows, &FeedOptions::new());
        for pair in out.messages.windows(2) {
            let (a, b) = (&pair[0], &pair[1]);
            if a.span_id == b.span_id && a.trace_id == b.trace_id && a.timestamp == b.timestamp {
                prop_assert!(
                    (a.message_index, a.entry_index) <= (b.message_index, b.entry_index),
                    "same-batch blocks out of ascending order"
                );
            }
        }
    }

    /// Malformed or hostile framework JSON must degrade to "no messages", never panic:
    /// this runs inside an HTTP handler serving historical data.
    #[test]
    fn never_panics(rows in any_rows(), role in prop::option::of(
        prop::sample::select(vec!["user", "assistant", "system", "tool", "nonsense", ""])
    )) {
        let opts = FeedOptions::new().with_role(role.map(str::to_string));
        let _ = process_spans(rows, &opts);
    }

    /// Filtering never invents rows and never returns a role it was not asked for.
    #[test]
    fn role_filter_is_a_subset(rows in good_rows(), role in prop::sample::select(
        vec!["user", "assistant", "system", "tool"]
    )) {
        let all = process_spans(rows.clone(), &FeedOptions::new());
        let filtered = process_spans(rows, &FeedOptions::new().with_role(Some(role.to_string())));
        prop_assert!(
            filtered.messages.len() <= all.messages.len(),
            "filtering produced more messages than no filter"
        );
        for m in &filtered.messages {
            prop_assert_eq!(m.role.as_str(), role, "role filter returned a different role");
        }
    }
}

/// A response's blocks stay in source order and the answer is stable across runs.
///
/// `process_dedup` used to compare two blocks of one response by source order and everything else
/// by birth time. Those rules contradict each other whenever one response's blocks have different
/// birth times - text is timestamped at span end, tool_use at event time - and a third block
/// timestamped between them then makes `text < tool < third < text`, which `sort_by` may panic on.
/// The ordering is now derived from a key, so no such cycle can exist by construction.
///
/// The fixture reaches that arrangement: the span ends after the tool result that sits between the
/// text and its sibling call, so the text's birth time falls after both. Reinstating the old rule
/// makes this test fail on the ordering assertion, which is the check that matters - not the
/// absence of a panic, since an inconsistent comparator is free to return a wrong answer quietly.
#[test]
fn a_response_keeps_its_source_order_and_the_result_is_stable() {
    let msg = json!([
        // One response carrying text (span_end) and a tool call (event time).
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:01Z"}},
            "content": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "calling the tool"},
                    {"type": "tool_use", "id": "call-1", "name": "lookup", "input": {"q": "a"}}
                ]
            }
        },
        // A block timestamped between the two above once birth times are computed.
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": "2025-01-01T00:00:02Z"}},
            "content": {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "call-1", "name": "lookup",
                             "content": "answer"}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:03Z"}},
            "content": {"role": "assistant", "content": "done"}
        }
    ]);

    // The row must be dated where the events are: this helper's base is 2023 while the events
    // above are 2025, and `effective_timestamp` takes the later of span end and event time - so a
    // 2023 span end never lands after a 2025 event and the text tied with its sibling tool call,
    // which is why an earlier version of this test proved nothing. Dated in 2025 with the span
    // ending after the intervening tool result, the text's birth time falls after it and the
    // arrangement the old comparator could not order consistently is reached.
    let mut row = row(1, 1, 0, msg.to_string());
    row.span_timestamp = "2025-01-01T00:00:00Z"
        .parse::<DateTime<Utc>>()
        .expect("valid timestamp");
    row.span_end_timestamp = Some(row.span_timestamp + chrono::Duration::seconds(4));
    row.ingested_at = row.span_timestamp;

    // The assertion is that this returns at all - an intransitive comparator panics - and that it
    // returns the same answer every time.
    let first = process_spans(vec![row.clone()], &FeedOptions::new());
    for _ in 0..20 {
        let again = process_spans(vec![row.clone()], &FeedOptions::new());
        assert_eq!(
            again
                .messages
                .iter()
                .map(|b| format!("{:?}", b.content))
                .collect::<Vec<_>>(),
            first
                .messages
                .iter()
                .map(|b| format!("{:?}", b.content))
                .collect::<Vec<_>>(),
            "the same input produced two different orders"
        );
    }

    // And the response's own blocks stay in the order the model produced them.
    let text_at = first
        .messages
        .iter()
        .position(|b| matches!(&b.content, ContentBlock::Text { .. }))
        .expect("the text block");
    let call_at = first
        .messages
        .iter()
        .position(|b| matches!(&b.content, ContentBlock::ToolUse { .. }))
        .expect("the tool call");
    assert!(
        text_at < call_at,
        "the text preceded the call in the response, so it must precede it in the feed"
    );
}
