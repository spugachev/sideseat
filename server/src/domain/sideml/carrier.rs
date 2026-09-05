//! What a carrier's structure is evidence *of*.
//!
//! A carrier is the event or attribute an observation was read from. Reconstruction keeps asking the
//! same question of it - are these two identical-looking things one message seen twice, or two
//! messages? - and the answer depends on what kind of carrier it is, not on the content:
//!
//! - A `gen_ai.choice` event is one emission. Two tool calls in it are two calls, whether or not the
//!   provider sent ids, because a model asking twice is exactly what that looks like.
//! - LangChain's `output.value` is accumulated framework state. It re-lists its own messages, so the
//!   same call appears at two positions while describing one call.
//!
//! Both are ordered and both may contain history. They differ only in whether *position* proves
//! multiplicity - which is why this is four independent facts rather than one enum. The distinction
//! was previously an unstated global rule ("trust the id, fall back to position"), which happened to
//! give the right answer for both cases and said nothing about why.
//!
//! These are claims about the carrier, and a structural test cannot prove them: identical JSON can
//! represent accumulated state or distinct occurrences. What it can do is require every carrier the
//! corpus produces to be classified deliberately, which `carrier_semantics_are_declared` does.

/// What one carrier's shape tells reconstruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CarrierSemantics {
    /// Two observations at different positions are two occurrences, not one seen twice.
    ///
    /// True for a single emission; false for accumulated state, which re-lists what it already said.
    pub position_proves_distinct_occurrence: bool,
    /// Positions state the order the observations belong in.
    ///
    /// Almost always true - it is what `assert_carrier_subsequence` checks - and false only where a
    /// carrier is a bag rather than a sequence.
    pub position_provides_sequence_order: bool,
    /// The carrier is one emission, so its observations belong together and stay contiguous.
    pub carrier_is_atomic_emission: bool,
    /// The carrier may re-state earlier turns, so its observations can be history rather than news.
    pub carrier_may_contain_history_or_state: bool,
    /// The span *produced* what this carrier holds, rather than receiving it.
    ///
    /// Declared per carrier because it cannot be inferred from the others, and because inferring it
    /// from a prefix list is what let it drift: the Vercel SDK moved from `ai.result.*` to
    /// `ai.response.*` and the extractor followed while the list did not, so every Vercel response read
    /// as something the span *received*. It also cannot be derived from
    /// `carrier_is_atomic_emission` - `output.value` is accumulated state and still the span's output -
    /// nor from the event name alone: `gen_ai.tool.result` on the span that made the call is that
    /// span's own record of what came back, while `gen_ai.tool.message` is the framework handing a past
    /// result back to a model.
    ///
    /// The ordering resolver reads it to decide what a generation *received*, which is the input side
    /// of "input precedes output". Deriving that side by negating an inferred flag made it read a tool
    /// answer as a precondition of the call that produced it.
    pub carrier_holds_span_output: bool,
    /// The carrier is a *detached request frame*: the system instruction a generation was given,
    /// reported beside the conversation rather than inside it.
    ///
    /// This is a fact about the carrier, deliberately not about the role. `developer` normalises to
    /// `System`, and a system message *inside* an ordered array is a turn in that array, framed by its
    /// position - `adk/image_gen`'s second instruction legitimately sits mid-trace at index 9. Only the
    /// detached carriers assert "this precedes every other input of the request that carried me", which
    /// is the edge the ordering resolver builds from it: a frame is not a turn that happened after the
    /// question, it is the frame the request was made in.
    pub carrier_is_detached_request_frame: bool,
}

impl CarrierSemantics {
    /// One emission: everything in it happened now, and two of anything are two.
    const EMISSION: Self = Self {
        position_proves_distinct_occurrence: true,
        position_provides_sequence_order: true,
        carrier_is_atomic_emission: true,
        carrier_may_contain_history_or_state: false,
        carrier_holds_span_output: true,
        carrier_is_detached_request_frame: false,
    };

    /// A conversation as one span saw it: ordered, may repeat earlier turns, and a repeat inside it
    /// is a re-statement rather than a second occurrence.
    const SNAPSHOT: Self = Self {
        position_proves_distinct_occurrence: false,
        position_provides_sequence_order: true,
        carrier_is_atomic_emission: false,
        carrier_may_contain_history_or_state: true,
        carrier_holds_span_output: false,
        carrier_is_detached_request_frame: false,
    };

    /// Framework state that happens to contain messages - LangChain's `output.value`, an agent's
    /// accumulated scratchpad. Ordered, re-lists itself, and says nothing about multiplicity.
    /// Accumulated state is the span's *output* - `output.value` is what the chain produced - while
    /// still being a re-listing rather than one emission. That combination is why direction cannot be
    /// derived from `carrier_is_atomic_emission`.
    const ACCUMULATED_STATE: Self = Self {
        position_proves_distinct_occurrence: false,
        position_provides_sequence_order: true,
        carrier_is_atomic_emission: false,
        carrier_may_contain_history_or_state: true,
        carrier_holds_span_output: true,
        carrier_is_detached_request_frame: false,
    };
}

/// The semantics of the carrier an observation came from.
///
/// `event` and `attribute` are the carrier's name as recorded on the block; exactly one is set.
///
/// The default for an unrecognised carrier is [`CarrierSemantics::SNAPSHOT`], the cautious reading:
/// it declines to treat position as proof of a second occurrence, so a carrier nobody has classified
/// cannot invent messages. It can only under-report, which the answer invariant would catch.
pub fn semantics_for(event: Option<&str>, attribute: Option<&str>) -> CarrierSemantics {
    declared_semantics(event, attribute).unwrap_or(CarrierSemantics::SNAPSHOT)
}

/// The table's entry for a carrier, or `None` where nothing names it.
///
/// Separate from [`semantics_for`] so that "nobody has classified this" is distinguishable from
/// "classified, and it reads as a snapshot". The two are the same *value* and completely different
/// facts, and `carrier_semantics_are_declared` needs to tell them apart - a test that compared the
/// value could not, and reported every declared snapshot carrier as unclassified.
pub fn declared_semantics(
    event: Option<&str>,
    attribute: Option<&str>,
) -> Option<CarrierSemantics> {
    if let Some(event) = event {
        return Some(match event {
            // The model's own output, and a span's record of a tool it ran: each is one emission the
            // span produced.
            "gen_ai.choice"
            | "gen_ai.content.completion"
            | "gen_ai.output.messages"
            | "gen_ai.tool.result" => CarrierSemantics::EMISSION,
            // One emission too, but *received*: this is the framework handing a past result back to a
            // model, so its time is the hand-back and it is input to whatever the span then produces.
            // Reading it as output made a re-sent result look like a generation's own answer.
            "gen_ai.tool.message" => CarrierSemantics {
                carrier_holds_span_output: false,
                ..CarrierSemantics::EMISSION
            },
            // A re-sent turn, by definition history. `gen_ai.assistant.message` is the awkward one:
            // it is a replay for most frameworks and the actual output for a choiceless Logfire
            // generation span, so it is read as a snapshot and direction is decided elsewhere.
            // A re-sent turn, by definition history. `gen_ai.assistant.message` is the awkward one:
            // it is a replay for most frameworks and the actual output for a choiceless Logfire
            // generation span, so it is read as a snapshot and direction is decided elsewhere.
            "gen_ai.user.message"
            | "gen_ai.system.message"
            | "gen_ai.assistant.message"
            | "gen_ai.content.prompt"
            | "gen_ai.input.messages" => CarrierSemantics::SNAPSHOT,
            _ => return None,
        });
    }

    Some(match attribute {
        // The generic IO pair, and the framework-state attributes that behave like it. LangChain's
        // `output.value` re-lists its own tool calls, which is the case that forced this distinction.
        Some("output.value") => CarrierSemantics::ACCUMULATED_STATE,
        // The same shape on the receiving side: state handed *to* the span. Same reading of position
        // and history, opposite direction - which is why direction is its own fact.
        Some("input.value") | Some("message") | Some("messages") => CarrierSemantics {
            carrier_holds_span_output: false,
            ..CarrierSemantics::ACCUMULATED_STATE
        },
        // What this span *produced*: one response, in one payload. Ordered, its own, and two of
        // anything in it are two - the same reading as `gen_ai.choice`, which is the event form of the
        // same thing. Being an emission is what keeps a response's parts together: Vercel puts a
        // turn's intro text and the tool calls it introduces in one `ai.response`, and reading that as
        // a snapshot let the two be ordered independently, so the text sorted after its own calls.
        Some(key)
            if key.starts_with("gen_ai.output.messages")
                || key.starts_with("llm.output_messages")
                || key.starts_with("ai.response") =>
        {
            CarrierSemantics::EMISSION
        }
        // What this span *received*: a conversation as it saw it, which may re-state earlier turns.
        Some(key)
            if key.starts_with("gen_ai.input.messages")
                || key.starts_with("llm.input_messages")
                || key.starts_with("ai.prompt")
                // Logfire's request payload, and the Claude Code CLI's turns and tool results.
                || key == "request_data"
                || key == "new_context" =>
        {
            CarrierSemantics::SNAPSHOT
        }
        // The system prompt a model was given, under each framework's name for it: semconv's,
        // the Claude Code CLI's, and Strands'. Received like a snapshot, and additionally a
        // *detached request frame* - see the field's own comment. `gen_ai.system.message` is
        // deliberately not here: an event in a conversation stream is in-band, and may be a turn.
        Some("gen_ai.system_instructions") | Some("user_system_prompt") | Some("system_prompt") => {
            CarrierSemantics {
                carrier_is_detached_request_frame: true,
                ..CarrierSemantics::SNAPSHOT
            }
        }
        // A tool span's own pair: it was handed the arguments and it produced the result. One emission
        // each - a tool is called once - differing only in direction, which is the clearest case for
        // direction being its own fact rather than something inferred from the shape.
        //
        // `gen_ai.tool.call.*` is the same pair under the *current* conventions, which is how the Vercel AI
        // SDK's present integration reports a tool call: pure `gen_ai.*`, no `ai.*` at all. Same reading,
        // because it is the same fact written to a newer name.
        Some("ai.toolCall.result") | Some("gen_ai.tool.call.result") => CarrierSemantics::EMISSION,
        Some("ai.toolCall.args") | Some("gen_ai.tool.call.arguments") | Some("tool_name") => {
            CarrierSemantics {
                carrier_holds_span_output: false,
                ..CarrierSemantics::EMISSION
            }
        }
        // The model's reply, under the Claude Code CLI's name for it.
        Some("response.model_output") => CarrierSemantics::EMISSION,
        // The error built from a span's exception fields. The span produced it, it is not a re-send,
        // and it has no position in any payload - it is composed rather than read.
        Some("exception") => CarrierSemantics {
            position_provides_sequence_order: false,
            ..CarrierSemantics::EMISSION
        },
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_emission_proves_multiplicity_and_state_does_not() {
        // The case this distinction exists for, from both sides.
        assert!(
            semantics_for(Some("gen_ai.choice"), None).position_proves_distinct_occurrence,
            "two calls in one choice event are two calls - a model asking twice looks exactly like \
             this, and ids may be absent"
        );
        assert!(
            !semantics_for(None, Some("output.value")).position_proves_distinct_occurrence,
            "LangChain's output.value re-lists its own tool calls, so two positions there describe \
             one call"
        );
    }

    #[test]
    fn state_and_snapshots_are_still_ordered_and_may_hold_history() {
        for carrier in [
            semantics_for(None, Some("output.value")),
            semantics_for(None, Some("gen_ai.input.messages")),
        ] {
            assert!(
                carrier.position_provides_sequence_order,
                "both state a sequence, which is what the carrier-subsequence invariant reads"
            );
            assert!(
                carrier.carrier_may_contain_history_or_state,
                "both can re-state earlier turns"
            );
        }
    }

    #[test]
    fn an_unknown_carrier_takes_the_cautious_reading() {
        let unknown = semantics_for(None, Some("some.framework.newAttribute"));
        assert!(
            !unknown.position_proves_distinct_occurrence,
            "an unclassified carrier must not invent occurrences: it can under-report, which the \
             answer invariant catches, but over-reporting shows as duplicates a user sees"
        );
        assert_eq!(unknown, CarrierSemantics::SNAPSHOT);
    }
}
