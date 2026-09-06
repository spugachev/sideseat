//! A source of truth that is not a reading of the output.
//!
//! Every golden in this suite records what the pipeline *produced*, so a golden can bless a defect -
//! three of the false equivalences fixed in the review series were blessed exactly that way. The way
//! out, specified in the design record's verification apparatus, is to derive **both** the oracle and
//! the telemetry from one program:
//!
//! ```text
//! SourceProgram ──→ TruthGraph                    (the oracle; never given to the pipeline)
//!        └────────→ semconv encoder ──→ ExportTraceServiceRequest   (the real input)
//! ```
//!
//! The truth is the *action taken* - `emit`, `redeliver`, `restate`, `retry` - not any interpretation
//! of the bytes it produced. A test then runs the encoded request through the real ingestion path and
//! compares what survives against what the program says happened. That is the discrimination no golden
//! can make: `emit + emit` and `emit + restate` can produce byte-identical *content*, and only the
//! program knows which one it was.
//!
//! Deliberately small: one producer encoder (current GenAI semconv events on generation spans), and
//! the truth model encodes the pipeline's **specified** behaviour including its documented limits - a
//! repeated plain message with no id collapses by design (the identity notes in `feed/dedup.rs`), so
//! the oracle says one occurrence there, and says so as a stated limit rather than as an accident.

use serde_json::{Value as JsonValue, json};

/// What the program said happened, per emitted occurrence.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TruthOccurrence {
    /// A user turn. Plain text: identical content collapses by design, and the oracle accounts for it.
    Question { text: String },
    /// An assistant answer. Same plain-text collapsing rule.
    Answer { text: String },
    /// A tool call the model actually made, with the provider id the encoder minted for it.
    /// Distinct ids from emission carriers are execution evidence, so two of these with one shape are
    /// two occurrences.
    Call {
        id: String,
        name: String,
        input: JsonValue,
    },
    /// The result answering one call, tied by that call's id.
    ResultFor { call_id: String, text: String },
}

/// One generation request: a span carrying its own turn as semconv events.
struct Request {
    span_index: usize,
    events: Vec<JsonValue>,
}

/// The program: a sequence of source actions over one trace, and the truth they establish.
pub(crate) struct SourceProgram {
    trace_id: String,
    requests: Vec<Request>,
    truth: Vec<TruthOccurrence>,
    next_call: u32,
    base_nanos: i64,
}

impl SourceProgram {
    pub(crate) fn new() -> Self {
        Self {
            trace_id: "aa00000000000000000000000000feed".to_string(),
            requests: Vec::new(),
            truth: Vec::new(),
            next_call: 0,
            base_nanos: 1_787_000_000_000_000_000,
        }
    }

    /// Start a new generation request - a new span, one second after the previous one.
    pub(crate) fn request(&mut self) -> &mut Self {
        let span_index = self.requests.len();
        self.requests.push(Request {
            span_index,
            events: Vec::new(),
        });
        self
    }

    fn current(&mut self) -> &mut Request {
        if self.requests.is_empty() {
            self.request();
        }
        self.requests.last_mut().expect("a request exists")
    }

    fn event_time(&self, request: usize, ordinal: usize) -> String {
        // Strictly inside the span: at the span start an input is indistinguishable from history.
        let nanos =
            self.base_nanos + (request as i64) * 1_000_000_000 + (ordinal as i64 + 1) * 1_000_000;
        chrono::DateTime::from_timestamp_nanos(nanos).to_rfc3339()
    }

    /// The model was asked something: a new occurrence, unless identical text was already asked -
    /// the documented plain-text collapse, which the oracle owns rather than hides.
    pub(crate) fn emit_question(&mut self, text: &str) -> &mut Self {
        let truth = TruthOccurrence::Question {
            text: text.to_string(),
        };
        if !self.truth.contains(&truth) {
            self.truth.push(truth);
        }
        let (idx, ordinal) = {
            let request = self.current();
            (request.span_index, request.events.len())
        };
        let time = self.event_time(idx, ordinal);
        self.current().events.push(json!({
            "name": "gen_ai.user.message",
            "timeUnixNano": rfc3339_to_nanos(&time),
            "attributes": [
                {"key": "gen_ai.system", "value": {"stringValue": "synthetic"}},
                {"key": "content", "value": {"stringValue": text}},
            ]
        }));
        self
    }

    /// The model answered: `gen_ai.choice`, the emission carrier.
    pub(crate) fn emit_answer(&mut self, text: &str) -> &mut Self {
        let truth = TruthOccurrence::Answer {
            text: text.to_string(),
        };
        if !self.truth.contains(&truth) {
            self.truth.push(truth);
        }
        let (idx, ordinal) = {
            let request = self.current();
            (request.span_index, request.events.len())
        };
        let time = self.event_time(idx, ordinal);
        self.current().events.push(json!({
            "name": "gen_ai.choice",
            "timeUnixNano": rfc3339_to_nanos(&time),
            "attributes": [
                {"key": "finish_reason", "value": {"stringValue": "stop"}},
                {"key": "message", "value": {"stringValue": text}},
            ]
        }));
        self
    }

    /// The model made a tool call: a fresh provider id, minted here, which is what makes it an
    /// *execution* rather than an echo. Returns the id so results and restatements can name it.
    pub(crate) fn emit_call(&mut self, name: &str, input: JsonValue) -> String {
        self.next_call += 1;
        let id = format!("call_{:04}", self.next_call);
        self.truth.push(TruthOccurrence::Call {
            id: id.clone(),
            name: name.to_string(),
            input: input.clone(),
        });
        let (idx, ordinal) = {
            let request = self.current();
            (request.span_index, request.events.len())
        };
        let time = self.event_time(idx, ordinal);
        let message = json!([{"toolUse": {"toolUseId": id, "name": name, "input": input}}]);
        self.current().events.push(json!({
            "name": "gen_ai.choice",
            "timeUnixNano": rfc3339_to_nanos(&time),
            "attributes": [
                {"key": "finish_reason", "value": {"stringValue": "tool_use"}},
                {"key": "message", "value": {"stringValue": message.to_string()}},
            ]
        }));
        id
    }

    /// The tool answered one call.
    pub(crate) fn emit_result(&mut self, call_id: &str, text: &str) -> &mut Self {
        self.truth.push(TruthOccurrence::ResultFor {
            call_id: call_id.to_string(),
            text: text.to_string(),
        });
        let (idx, ordinal) = {
            let request = self.current();
            (request.span_index, request.events.len())
        };
        let time = self.event_time(idx, ordinal);
        let message = json!([{"toolResult": {"toolUseId": call_id, "status": "success", "content": [{"text": text}]}}]);
        self.current().events.push(json!({
            "name": "gen_ai.tool.message",
            "timeUnixNano": rfc3339_to_nanos(&time),
            "attributes": [
                {"key": "message", "value": {"stringValue": message.to_string()}},
            ]
        }));
        self
    }

    /// A later request re-sends a call it did not make, with a **regenerated** id - the shape a
    /// history replay takes. No truth is added: a re-send is not an occurrence, and the regenerated id
    /// is exactly the id that must not be trusted as execution evidence.
    pub(crate) fn restate_call(&mut self, name: &str, input: JsonValue) -> &mut Self {
        self.next_call += 1;
        let regenerated = format!("regen_{:04}", self.next_call);
        let (idx, ordinal) = {
            let request = self.current();
            (request.span_index, request.events.len())
        };
        let time = self.event_time(idx, ordinal);
        let message =
            json!([{"toolUse": {"toolUseId": regenerated, "name": name, "input": input}}]);
        self.current().events.push(json!({
            "name": "gen_ai.assistant.message",
            "timeUnixNano": rfc3339_to_nanos(&time),
            "attributes": [
                {"key": "message", "value": {"stringValue": message.to_string()}},
            ]
        }));
        self
    }

    /// The truth: how many distinct tool-call executions the program performed.
    pub(crate) fn expected_calls(&self) -> usize {
        self.truth
            .iter()
            .filter(|t| matches!(t, TruthOccurrence::Call { .. }))
            .count()
    }

    /// The truth: how many results the program's tools produced.
    pub(crate) fn expected_results(&self) -> usize {
        self.truth
            .iter()
            .filter(|t| matches!(t, TruthOccurrence::ResultFor { .. }))
            .count()
    }

    /// Encode the whole program as one OTLP export - the real input the pipeline receives.
    pub(crate) fn encode(
        &self,
    ) -> opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
        self.encode_requests(0..self.requests.len())
    }

    /// Encode a subset, so a test can deliver the same request twice (`redeliver`) or split the
    /// program across exports.
    pub(crate) fn encode_requests(
        &self,
        range: std::ops::Range<usize>,
    ) -> opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
        let spans: Vec<JsonValue> = self.requests[range]
            .iter()
            .map(|request| {
                let start = self.base_nanos + (request.span_index as i64) * 1_000_000_000;
                let end = start + 900_000_000;
                json!({
                    "traceId": self.trace_id,
                    "spanId": format!("{:016x}", 0xbeef_0000_0000_0000u64 + request.span_index as u64),
                    "name": format!("chat request-{}", request.span_index),
                    "kind": 1,
                    "startTimeUnixNano": start.to_string(),
                    "endTimeUnixNano": end.to_string(),
                    "attributes": [
                        {"key": "gen_ai.operation.name", "value": {"stringValue": "chat"}},
                        {"key": "gen_ai.provider.name", "value": {"stringValue": "synthetic"}},
                        {"key": "gen_ai.request.model", "value": {"stringValue": "model-x"}},
                    ],
                    "events": request.events,
                })
            })
            .collect();
        let request = json!({
            "resourceSpans": [{
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": "source-program"}}
                ]},
                "scopeSpans": [{
                    "scope": {"name": "source.program.encoder", "version": "1"},
                    "spans": spans,
                }]
            }]
        });
        serde_json::from_value(request).expect("the encoder emits valid OTLP")
    }
}

fn rfc3339_to_nanos(time: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(time)
        .expect("encoder time")
        .timestamp_nanos_opt()
        .expect("in range")
        .to_string()
}
