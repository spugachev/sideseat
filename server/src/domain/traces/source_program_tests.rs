//! The pipeline against the program's truth: the discrimination no golden can make.
//!
//! `emit + emit` and `emit + restate` can produce byte-identical content, and only the program knows
//! which one it was - so these tests are the mutation controls for the identity rules, driven from
//! generated truth rather than from recorded output. Each corresponds to a case of the contraction
//! test specified in the design record's verification apparatus, restricted to the cases buildable
//! against the current pipeline (stable-identity contraction needs the rejected occurrence model and
//! is deliberately absent).

use serde_json::json;

use super::source_program::SourceProgram;
use crate::domain::pricing::PricingService;
use crate::domain::sideml::feed::{FeedOptions, process_spans};

fn survivors(program: &SourceProgram) -> (usize, usize) {
    let request = program.encode();
    let pricing = PricingService::init_for_test().expect("offline pricing service");
    let rows: Vec<_> = super::normalize_for_test(&request, &pricing)
        .into_iter()
        .map(|(_, row)| row)
        .collect();
    let result = process_spans(rows, &FeedOptions::new());
    let calls = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_use")
        .count();
    let results = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_result")
        .count();
    (calls, results)
}

/// Case 1: exact re-delivery of the same request changes nothing.
///
/// The same bytes twice are one export delivered twice - ingestion is idempotent by span id, and the
/// reconstruction must be too.
#[test]
fn exact_redelivery_changes_nothing() {
    let mut program = SourceProgram::new();
    program.request().emit_question("what is the weather?");
    let call = program.emit_call("forecast", json!({"city": "Paris"}));
    program
        .emit_result(&call, "sunny")
        .emit_answer("It is sunny.");

    let once = program.encode_requests(0..1);
    let pricing = PricingService::init_for_test().expect("offline pricing service");
    let rows_once: Vec<_> = super::normalize_for_test(&once, &pricing)
        .into_iter()
        .map(|(_, row)| row)
        .collect();
    let mut rows_twice = rows_once.clone();
    rows_twice.extend(rows_once.iter().cloned());

    let one = process_spans(rows_once, &FeedOptions::new());
    let two = process_spans(rows_twice, &FeedOptions::new());
    assert_eq!(
        one.messages.len(),
        two.messages.len(),
        "a re-delivered request must not add occurrences"
    );
}

/// Cases 2 and 4: two executions of one shape - the model genuinely ran the same call twice, each
/// with the provider id the encoder minted - are two calls with their own results.
///
/// This is the truth-driven form of the six-fixture repair: the program *knows* it executed twice,
/// where a golden could only record whatever the pipeline happened to keep.
#[test]
fn two_executions_of_one_shape_are_two() {
    let mut program = SourceProgram::new();
    program.request().emit_question("check Paris twice");
    let first = program.emit_call("forecast", json!({"city": "Paris"}));
    program.emit_result(&first, "sunny");
    program.request();
    let second = program.emit_call("forecast", json!({"city": "Paris"}));
    program
        .emit_result(&second, "still sunny")
        .emit_answer("Twice checked.");

    assert_eq!(program.expected_calls(), 2, "the program executed twice");
    let (calls, results) = survivors(&program);
    assert_eq!(
        (calls, results),
        (program.expected_calls(), program.expected_results()),
        "two executions with minted ids must both survive, each with its result"
    );
}

/// The other side, which is what makes case 2 falsifiable rather than vacuous: a later request
/// **re-stating** the call with a regenerated id adds nothing, because a re-send is not an execution
/// and its id is not evidence of one.
#[test]
fn a_restated_call_is_not_a_second_execution() {
    let mut program = SourceProgram::new();
    program.request().emit_question("check Paris");
    let call = program.emit_call("forecast", json!({"city": "Paris"}));
    program.emit_result(&call, "sunny");
    program.request();
    program
        .restate_call("forecast", json!({"city": "Paris"}))
        .emit_answer("Sunny, as replayed.");

    assert_eq!(program.expected_calls(), 1, "the program executed once");
    let (calls, _) = survivors(&program);
    assert_eq!(
        calls,
        program.expected_calls(),
        "a replayed call with a regenerated id must collapse onto the execution it restates"
    );
}

/// The documented plain-text limit, owned by the oracle instead of hidden by it: the same question
/// emitted twice with no id collapses to one, by design - identical content with nothing to tell the
/// copies apart is indistinguishable from a history re-send.
#[test]
fn an_identical_plain_question_collapses_by_design() {
    let mut program = SourceProgram::new();
    program
        .request()
        .emit_question("retry")
        .emit_answer("first");
    program
        .request()
        .emit_question("retry")
        .emit_answer("second");

    let request = program.encode();
    let pricing = PricingService::init_for_test().expect("offline pricing service");
    let rows: Vec<_> = super::normalize_for_test(&request, &pricing)
        .into_iter()
        .map(|(_, row)| row)
        .collect();
    let result = process_spans(rows, &FeedOptions::new());
    let questions = result
        .messages
        .iter()
        .filter(|b| b.role == crate::domain::sideml::ChatRole::User && b.entry_type == "text")
        .count();
    assert_eq!(
        questions, 1,
        "the plain-text collapse is the specified behaviour - if this fails, the identity rules \
         learned to keep id-less repeats, and the oracle must learn it in the same change"
    );
}
