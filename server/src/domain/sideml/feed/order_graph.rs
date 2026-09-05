//! Order resolver: the timeline as a partial order, not a scalar key.
//!
//! The previous timeline was one sort key whose anchor is a mutable per-response minimum. Because
//! that anchor is computed *after* dedup, the order depends on which copy of a message survived, and
//! two copies tie on quality routinely — so reading a carrier that was previously ignored silently
//! reorders unrelated messages. Six scalar-anchor candidates were tried and rejected (see the plan);
//! the conclusion, reviewed with Codex, is that ordering is a **partial order** and time is a
//! *priority*, not a constraint.
//!
//! This module builds that partial order and resolves it. Production runs
//! [`Constraints::PRODUCTION`], which lists exactly which classes are enforced and what each one was
//! measured to change; [`Constraints::NEUTRAL`] enforces nothing and is provably unable to move a
//! block, which keeps the machinery itself verifiable
//! (`the_neutral_resolver_reproduces_the_legacy_order`) as classes are promoted one at a time. Under
//! [`Constraints::FULL`] it produces the redesign's intended answer, which tests compare against.
//!
//! # Model (Codex's framing)
//!
//! Three levels, deliberately distinct:
//!
//! - **Evidence occurrences**: every pre-dedup observation, with its exact carrier instance. This is
//!   why the resolver reads the classified blocks *before* dedup — the emission that binds a turn's
//!   intro text to its tool call is on one span, but dedup may keep a re-listed copy of the text from
//!   another span, which would lose the binding.
//! - **Logical identities**: the dedup equivalence classes (the survivors).
//! - **Ordering units**: one identity, or several identities contracted because they were one atomic
//!   emission. Contiguity cannot be a pairwise edge — a DAG says `A < B`, never "nothing between A
//!   and B" — so an emission becomes a single node and external edges attach to its boundary.
//!
//! # Constraint classes
//!
//! See [`Constraints`] for the full list and what each was measured to change: atomic-emission
//! contraction, exact call → result, carrier sequence, generation dataflow, request framing, and the
//! fragmented ordered-input family. Credible time is a **priority** for the topological pop, never an
//! edge.

use std::collections::{BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};

use super::dedup::{SpanTimestamps, effective_timestamp};
use super::types::BlockEntry;

/// A survivor's contribution to an emission instance: `(message_index, entry_index, survivor)`.
/// The pair fixes the block's source order within the emission; the index points back at the
/// survivor being contracted.
type EmissionMember = (i32, i32, usize);

/// What orders one unit against another when the constraints leave them free: the anchor its evidence
/// gives it, where it was first observed, and its own id as the final discriminator.
type PopKey = (Option<DateTime<Utc>>, usize, usize);

/// One payload instance: its span, the event or attribute it arrived on, and which instance of that
/// carrier it was - the root of the position path. A span can emit `gen_ai.choice` more than once, and
/// those are different payloads.
type PayloadKey<'a> = (&'a str, Option<&'a str>, Option<&'a str>, String);

// How many cycles the resolver broke, visible to tests: the `warn!` below reaches production
// telemetry, and tests install no subscriber, so without this a contradiction in the evidence is
// invisible exactly where the corpus could catch it.
#[cfg(test)]
thread_local! {
    pub(crate) static CYCLES_BROKEN_IN_TESTS: std::cell::RefCell<usize> =
        const { std::cell::RefCell::new(0) };
}

/// The synthetic node standing for "everything this span received precedes everything it produced".
///
/// Numbered past the real units so it cannot collide with one, and it emits no blocks - it exists only
/// to keep the edge count linear in a span's messages rather than quadratic.
fn barrier_unit(span: usize, survivor_count: usize) -> usize {
    survivor_count + span
}

/// What the resolver needs to know about one pre-dedup observation.
///
/// Deliberately not the observation itself. The resolver reads the evidence set *before* dedup, and
/// holding onto whole blocks for that meant cloning every message's content on every request - on a
/// fixture whose tool results carry base64 images that is the dominant cost, and none of it is ever
/// read. This is the same set of facts in a handful of words per observation.
pub(super) struct OrderEvidence {
    /// Which emission instance this observation belongs to, when it is a credible emission of one.
    emission: Option<usize>,
    /// Where the observation sat in its payload, for the emission's own order.
    message_index: i32,
    entry_index: i32,
    /// The observation's own effective time. The *survivor's* time is no use here: dedup overwrites it
    /// with the old batch anchor, so reading it would make the new order a function of the old one.
    effective: DateTime<Utc>,
    /// Usable as evidence of when the message happened: a credible emission, not a history re-send.
    credible: bool,
    /// Which span carried this observation, interned.
    span: usize,
    /// Which carrier of that span, interned - the event or attribute it was read from.
    carrier: usize,
    /// That carrier's positions state the order its observations belong in.
    carrier_ordered: bool,
    /// The span produced this observation, rather than receiving it.
    is_output: bool,
    /// The span is a generation - a model call, so its input caused its output.
    from_generation: bool,
    /// The observation came from a *detached request frame* carrier - the system instruction a
    /// generation was given, reported beside the conversation. A carrier fact, never a role fact:
    /// see `CarrierSemantics::carrier_is_detached_request_frame`.
    detached_frame: bool,
    /// The ordered-input carrier *family* this observation belongs to, interned per span, when its
    /// carrier is an ordered input array of a generation span. `llm.input_messages.0.message` and
    /// `.1.message` are one array, and the family is what groups them - the exact key interns them
    /// apart and a one-member sequence orders nothing. History is deliberately **not** folded in here
    /// (unlike `carrier_ordered`): the framing block reads this and neutralises replays with its
    /// first-seen rule instead, which is what lets the array order the request's own new turns even
    /// when their surviving copies live on other spans.
    input_family: Option<usize>,
}

/// Reduce the classified, pre-dedup blocks to what the resolver reads.
///
/// An emission instance is `(span, position-path root)`: two `gen_ai.choice` events on one span have
/// different roots, so this separates them while the blocks of one choice share it. Instances are
/// interned to indices, so the only allocation is one key per distinct emission.
pub(super) fn collect_order_evidence(
    blocks: &[BlockEntry],
    span_timestamps: &HashMap<String, SpanTimestamps>,
) -> Vec<OrderEvidence> {
    let mut instances: HashMap<(String, String), usize> = HashMap::new();
    let mut spans: HashMap<&str, usize> = HashMap::new();
    // Keyed by payload *instance*, not carrier name: a span can emit `gen_ai.choice` several times,
    // and interning by name merged those into one carrier - so a sequence edge could be drawn between
    // two different emissions as though one payload had listed them. Contraction already keys on the
    // instance; this is the same key, which is what the TLA+ model assumes throughout.
    let mut carriers: HashMap<PayloadKey<'_>, usize> = HashMap::new();
    let mut input_families: HashMap<(String, String), usize> = HashMap::new();
    blocks
        .iter()
        .map(|block| {
            let credible = is_credible_emission(block);
            let next_span = spans.len();
            let span = *spans.entry(block.span_id.as_str()).or_insert(next_span);
            let semantics = crate::domain::sideml::carrier::semantics_for(
                block.event_name.as_deref(),
                block.source_attribute.as_deref(),
            );
            let payload_root = block
                .position
                .to_string()
                .split('.')
                .next()
                .unwrap_or("")
                .to_string();
            let next_carrier = carriers.len();
            let carrier = *carriers
                .entry((
                    block.span_id.as_str(),
                    block.event_name.as_deref(),
                    block.source_attribute.as_deref(),
                    payload_root,
                ))
                .or_insert(next_carrier);
            let emission = credible.then(|| {
                let next = instances.len();
                *instances
                    .entry((block.span_id.clone(), emission_scope(block)))
                    .or_insert(next)
            });
            // Two fragmented input families, each measured on its own, deliberately not every ordered
            // input carrier. Broadening to Vercel's `ai.prompt` regressed a sequential two-step
            // trace, measured: request 2's array lists `call1, result1` and its first-seen
            // sequencing pulled the second step's call ahead of the first step's result -
            // `call1, result1, call2, result2` became `call1, call2, result1, result2`, which
            // misrepresents the causality the old order showed. Each further family needs its own
            // measured pass, exactly like every other promotion in this module.
            //
            // The event-stream form of the same fragmentation (`gen_ai.system.message`,
            // `gen_ai.user.message` interning apart on one generation span) was implemented and
            // reverted: it fired thousands of forward no-op edges, created no new cycles, and fixed
            // nothing - for `strands/swarm`, the one case it was aimed at, the target does not exist
            // as an orderable unit. Strands rewrites the question before the model sees it
            // (`Context: User Request: ...`), so no identity links the surviving question to any
            // request, and the wrapped copy is correctly filtered as a context echo. A constraint
            // class with no measured repair is surface without benefit.
            let ordered_input = block.is_generation_span()
                && semantics.position_provides_sequence_order
                && !semantics.carrier_holds_span_output
                && !semantics.carrier_is_detached_request_frame
                && block
                    .source_attribute
                    .as_deref()
                    .is_some_and(|k| k.starts_with("llm.input_messages"));
            let input_family = ordered_input.then(|| {
                let family = canonical_input_family(
                    block.event_name.as_deref(),
                    block.source_attribute.as_deref(),
                );
                let next = input_families.len();
                *input_families
                    .entry((block.span_id.clone(), family))
                    .or_insert(next)
            });
            OrderEvidence {
                emission,
                message_index: block.message_index,
                entry_index: block.entry_index,
                effective: effective_timestamp(block, span_timestamps),
                credible: credible && !block.is_history,
                span,
                carrier,
                carrier_ordered: semantics.position_provides_sequence_order && !block.is_history,
                is_output: block.is_output_source(),
                from_generation: block.is_generation_span(),
                detached_frame: semantics.carrier_is_detached_request_frame,
                input_family,
            }
        })
        .collect()
}

/// The family of an ordered-input carrier: the name with any trailing array index stripped, so the
/// members of one array share it. `llm.input_messages.0.message` and `.1.message` are one array;
/// `new_context` or `ai.prompt` are already whole.
fn canonical_input_family(event: Option<&str>, attribute: Option<&str>) -> String {
    let name = attribute.or(event).unwrap_or_default();
    match name.find(|c: char| c.is_ascii_digit()) {
        Some(i) if i > 0 && name.as_bytes().get(i - 1) == Some(&b'.') => name[..i - 1].to_string(),
        _ => name.to_string(),
    }
}

/// Which emission of its span an observation belongs to.
///
/// Two different scopes, because frameworks split one response two different ways:
///
/// - An **event** carrier, or an array under one attribute, is one payload per instance, and a span
///   can emit `gen_ai.choice` more than once - so the scope is the root of the position path, which
///   distinguishes those instances.
/// - A **split** attribute carrier spreads one response across sibling keys: Vercel writes
///   `ai.response.text` beside `ai.response.toolCalls`, and those have different position roots while
///   being one emission. So the scope is the attribute's *family* - the key with its last segment
///   dropped - which binds the siblings without merging every output the span produced.
///
/// `(span, direction)` was the tempting generalisation and it overreaches: one span can hold several
/// emissions, and duplicate output forms of the same one.
fn emission_scope(block: &BlockEntry) -> String {
    let payload_root = || {
        block
            .position
            .to_string()
            .split('.')
            .next()
            .unwrap_or("")
            .to_string()
    };
    match block.source_attribute.as_deref() {
        // A family only where the key actually has one: `ai.response.text` -> `ai.response`. A
        // single-segment key is its own family.
        Some(attribute) => match attribute.rsplit_once('.') {
            Some((family, _)) if !family.is_empty() => format!("attr:{family}"),
            _ => format!("attr:{attribute}"),
        },
        None => format!("event:{}", payload_root()),
    }
}

/// Whether an observation is evidence of *when* a message happened and part of *one emission*.
///
/// An atomic emission the span itself produced: `gen_ai.choice`, and only when the span is the
/// emitter (`is_output_source`), never a received copy or a re-listed snapshot. A framework handing
/// a past result back to the model carries an emission-shaped carrier but its time is the hand-back,
/// not the occurrence — reading it as evidence moved a result ahead of its call.
fn is_credible_emission(block: &BlockEntry) -> bool {
    block.is_output_source()
        && crate::domain::sideml::carrier::semantics_for(
            block.event_name.as_deref(),
            block.source_attribute.as_deref(),
        )
        .carrier_is_atomic_emission
}

/// A disjoint-set over survivor indices, used to contract co-emitted identities into one unit.
struct UnionFind {
    parent: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
        }
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra != rb {
            // Deterministic: the smaller index becomes the representative.
            let (root, child) = if ra < rb { (ra, rb) } else { (rb, ra) };
            self.parent[child] = root;
        }
    }
}
/// What the evidence says must come before what, over *blocks*, as a relation rather than a sequence.
///
/// The resolver's answer is one linearisation of its own graph: a topological order chosen by time and
/// legacy index among the many the constraints permit. That choice is presentation. Anything reasoning
/// about causality has to ask the relation instead - and not the resolver's graph, for two reasons that
/// both come from what that graph is *for*.
///
/// **It is over units, not blocks.** An emission is contracted to one node, so an edge into or out of it
/// speaks for every block inside it: `call_a -> result_a` becomes `emission -> result_a`, which also
/// asserts `call_b -> result_a`. Right for presentation, because an emission is atomic and stays
/// contiguous - and wrong here, because a provider's conversation history is exactly what splits an
/// emission apart, writing each call beside its own result.
///
/// **It includes generation dataflow**, whose input side deliberately keeps replayed history. "The tool
/// result this answer cites came first" is the right reading there and a false statement about global
/// order, as that class documents. Dedup then makes it self-contradictory: a span whose input replays
/// the calls it is about to re-emit has them collapsed onto the same survivors, so the graph acquires
/// `call_b -> barrier -> call_a` for two calls of one emission.
///
/// So this is built from the three classes that describe one emission and one payload, at block
/// granularity: an emission's own sequence, exact call to result, and a carrier's stated order. Indices
/// are into the survivor slice - the same indexing the pre-resolve transcript uses.
#[derive(Debug, Clone, Default)]
pub(super) struct Precedence {
    /// Reverse adjacency over survivor indices.
    predecessors: Vec<Vec<u32>>,
    /// Forward adjacency, for looking one step ahead when choosing between interchangeable candidates.
    successors: Vec<Vec<u32>>,
}

impl Precedence {
    /// A relation stated directly as `(before, after)` pairs over block indices, for tests that need to
    /// exercise the matcher against a shape no captured fixture contains.
    #[cfg(test)]
    pub(super) fn from_edges(blocks: usize, edges: &[(usize, usize)]) -> Self {
        let mut predecessors = vec![Vec::new(); blocks];
        let mut successors = vec![Vec::new(); blocks];
        for &(before, after) in edges {
            if before < blocks && after < blocks {
                predecessors[after].push(before as u32);
                successors[before].push(after as u32);
            }
        }
        Self {
            predecessors,
            successors,
        }
    }

    /// What this block immediately precedes. Used to look one step ahead, not to reason about order.
    pub(super) fn successors_of(&self, block: usize) -> &[u32] {
        self.successors.get(block).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Everything that must precede `b`, accumulated into `seen` and skipping what it already holds.
    ///
    /// Amortised: a matcher checking many candidates against a growing set of matched occurrences pays
    /// for each block and edge once in total, not once per query.
    pub(super) fn collect_ancestors(&self, b: usize, seen: &mut HashSet<u32>) {
        if b >= self.predecessors.len() {
            return;
        }
        let mut stack = vec![b as u32];
        while let Some(node) = stack.pop() {
            for &p in self.predecessors.get(node as usize).into_iter().flatten() {
                if seen.insert(p) {
                    stack.push(p);
                }
            }
        }
    }
}

/// Which ordered thing a sequence edge came from. Both state an order over their own members, and
/// neither says anything about the other's, so they are collected separately and keyed apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum SequenceSource {
    /// One emission instance: the blocks a single response produced, in the order it produced them.
    Emission(usize),
    /// One carrier: the order a payload states between its own observations.
    Carrier(usize),
}

/// Build the block-level causal relation over a trace's survivors - see [`Precedence`] for why it is
/// not the resolver's graph.
pub(super) fn causal_precedence(
    evidence: &[OrderEvidence],
    survivors: &[BlockEntry],
    lineage: &[Option<usize>],
) -> Precedence {
    let n = survivors.len();
    let mut predecessors: Vec<Vec<u32>> = vec![Vec::new(); n];
    let mut successors: Vec<Vec<u32>> = vec![Vec::new(); n];
    let mut edges: HashSet<(u32, u32)> = HashSet::new();
    let mut add = |from: usize,
                   to: usize,
                   predecessors: &mut Vec<Vec<u32>>,
                   successors: &mut Vec<Vec<u32>>| {
        if from != to && from < n && to < n && edges.insert((from as u32, to as u32)) {
            predecessors[to].push(from as u32);
            successors[from].push(to as u32);
        }
    };
    let survivor_of = |observation: usize| lineage.get(observation).copied().flatten();

    // An emission's own sequence, and a carrier's stated order: the same shape, so collected together.
    // Consecutive members only - the transitive closure is what the walk computes.
    let mut sequences: HashMap<SequenceSource, Vec<EmissionMember>> = HashMap::new();
    for (observation, seen) in evidence.iter().enumerate() {
        let Some(survivor) = survivor_of(observation) else {
            continue;
        };
        if let Some(instance) = seen.emission {
            sequences
                .entry(SequenceSource::Emission(instance))
                .or_default()
                .push((seen.message_index, seen.entry_index, survivor));
        }
        if seen.carrier_ordered {
            sequences
                .entry(SequenceSource::Carrier(seen.carrier))
                .or_default()
                .push((seen.message_index, seen.entry_index, survivor));
        }
    }
    let mut keys: Vec<SequenceSource> = sequences.keys().copied().collect();
    keys.sort_unstable();
    for key in keys {
        let mut members = sequences.remove(&key).unwrap_or_default();
        members.sort_unstable();
        let mut sequence: Vec<usize> = Vec::with_capacity(members.len());
        for (_, _, survivor) in members {
            if sequence.last() != Some(&survivor) {
                sequence.push(survivor);
            }
        }
        for pair in sequence.windows(2) {
            add(pair[0], pair[1], &mut predecessors, &mut successors);
        }
    }

    // Exact call to result, at block granularity and only where the id is unambiguous - a reused or
    // regenerated id says nothing.
    let mut calls: HashMap<&str, Vec<usize>> = HashMap::new();
    for (i, block) in survivors.iter().enumerate() {
        if block.entry_type == "tool_use"
            && let Some(id) = block.tool_use_id.as_deref().filter(|s| !s.is_empty())
        {
            calls.entry(id).or_default().push(i);
        }
    }
    for (i, block) in survivors.iter().enumerate() {
        if block.entry_type != "tool_result" {
            continue;
        }
        let Some(id) = block.tool_use_id.as_deref().filter(|s| !s.is_empty()) else {
            continue;
        };
        if let Some(callers) = calls.get(id)
            && callers.len() == 1
        {
            add(callers[0], i, &mut predecessors, &mut successors);
        }
    }

    Precedence {
        predecessors,
        successors,
    }
}

/// Which constraints the resolver is allowed to *change the answer* with.
///
/// This is the promotion dial. `NEUTRAL` builds the whole graph and runs the whole resolve while
/// enforcing nothing, so its output is provably the legacy order — the proof that the machinery
/// cannot move anything on its own, kept checkable as classes are promoted.
///
/// One field per behaviour, deliberately: promoting a class means flipping *one* of them, so the
/// resulting golden delta is attributable to that class alone. A single bundled flag turned all four
/// on at once, which would have made the first promotion's diff uninterpretable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Constraints {
    /// Contract an emission whose survivors are not already adjacent in the legacy order.
    ///
    /// This is the half of neutrality that filtering edges does not cover: moving an emission's
    /// scattered members together is a reorder, and it is precisely the reorder the redesign exists
    /// to make (the `strands-js/swarm` intro text).
    pub contract_non_contiguous_emissions: bool,
    /// Enforce an edge of *any* class that the legacy order has backwards.
    ///
    /// This is the dial that lets the graph actually change an order rather than merely agree with
    /// one, so it is the second half of every promotion: a class whose edges are all forward already
    /// changes nothing.
    pub enforce_backward_edges: bool,
    /// Order units by time first, rather than by their legacy index.
    pub time_priority: bool,
    /// Order a unit's members by their source position rather than by legacy index.
    pub source_position_member_order: bool,
    /// Enforce the order a carrier's own payload states between its surviving observations.
    ///
    /// A message array is a sequence, and two blocks of one message are ordered by their position in
    /// it. Without this the time priority can swap them - two blocks of one ADK `llm_response` came
    /// back reversed - because a per-unit anchor says nothing about order *inside* a payload. This is
    /// the constraint form of the `assert_carrier_subsequence` invariant.
    pub carrier_sequence_edges: bool,
    /// Express generation dataflow as the product of a span's inputs and outputs, rather than through
    /// a barrier node.
    ///
    /// The two must produce the same order - the barrier exists only to make the edge count linear in a
    /// span's messages instead of quadratic - and `a_barrier_orders_exactly_as_pairwise_edges_do`
    /// compares them across the corpus. Production uses the barrier.
    pub pairwise_dataflow_edges: bool,
    /// Enforce that what a generation span *received* precedes what it *produced*.
    ///
    /// The minimal turn structure, and deliberately local dataflow rather than a rule about roles: a
    /// model call's input caused its output, so the system prompt and the tool results a call was given
    /// precede the answer it produced, and transitivity carries that across spans. A global rule like
    /// "the terminal assistant message follows the last user message and every intervening tool" says
    /// something similar and is false for parallel branches, subagents, retries and abandoned calls.
    pub generation_dataflow_edges: bool,
    /// Enforce that a *detached request frame* precedes every other input of the request that carried
    /// it.
    ///
    /// The one confirmed ordering defect: a framework reports the instruction on the span that sent it -
    /// the generation span - while the question arrived on an orchestration span that started earlier,
    /// so ordering by evidence time put the frame after the question. Two scalar repairs failed,
    /// measured (one tripped `assert_carrier_subsequence` on ADK, the other moved 15 feed views and
    /// repaired nothing), because "before" here is a constraint, not a position.
    ///
    /// The request is the generation span that carried the frame, and the edge is drawn to the other
    /// *input* units of that same span, projected through lineage - so the surviving copy of the
    /// question, wherever it sits, inherits the edge. Scoped to one request, deliberately: a trace
    /// legitimately holds several instructions (`adk/reasoning` repeats one at three request
    /// boundaries; `adk/image_gen` changes it mid-trace), and any wider scope is falsified by a
    /// committed fixture.
    pub request_framing_edges: bool,
}

impl Constraints {
    /// Provably output-neutral: every constraint is built and resolved, none can move a block.
    ///
    /// Kept as its own configuration after promotions begin, because it is what
    /// `the_neutral_resolver_reproduces_the_legacy_order` tests: the proof that the machinery cannot
    /// move anything on its own has to stay checkable, or a promotion could hide a resolver bug.
    #[cfg(test)]
    pub(super) const NEUTRAL: Self = Self {
        pairwise_dataflow_edges: false,
        contract_non_contiguous_emissions: false,
        enforce_backward_edges: false,
        time_priority: false,
        source_position_member_order: false,
        carrier_sequence_edges: false,
        generation_dataflow_edges: false,
        request_framing_edges: false,
    };

    /// What production enforces today.
    ///
    /// One class is promoted at a time, and each promotion's golden delta is read fixture by fixture
    /// before it lands. Promoted so far:
    ///
    /// - **Atomic-emission contraction** with source-position member order: a turn's intro text and
    ///   the call it introduces are one `gen_ai.choice`, so they stay together in the order that event
    ///   listed them. `strands-js/swarm` was the case - the intro text trailed the tool result it was
    ///   meant to introduce, because the text took its span's end time and the call took its event
    ///   time, so they grouped separately.
    /// - **Carrier-sequence edges**: the order a payload states between its own surviving blocks. On
    ///   its own this changes nothing (it enforces what the previous sort already produced); it is
    ///   promoted with contraction because it is what keeps two blocks of one message from being
    ///   separated once anything else can move them.
    ///
    /// - **Generation dataflow**, with direction read from the carrier declaration: what a model call
    ///   received precedes what it produced. This is the class that pins a tool result before the
    ///   answer citing it - no other class states that, because the consuming span is the only place
    ///   the two meet - and it is what cleared all ten `PerCarrier` reorders, so
    ///   `REORDERS_UNDER_PER_CARRIER` is now empty.
    ///
    ///   History is kept on its input side, unlike carrier-sequence edges, because the two ask
    ///   different questions of a re-send: "these precede what this generation produced" is true of a
    ///   replay; "these are in this relative order globally" is not, and that reading dragged ADK's
    ///   second system prompt to the front of a session.
    ///
    /// - **Occurrence anchors** (`time_priority`): a unit sorts at the earliest time its *evidence*
    ///   gives it, and ties break on where it was **first observed** across every copy - not on the
    ///   surviving block's index in the previous sort, which is what survivor choice could move.
    ///
    ///   This needed the emission scope generalised first. Vercel spreads one response across sibling
    ///   attributes (`ai.response.text` beside `ai.response.toolCalls`), which have different position
    ///   roots, so contraction could not hold that response together and promoting time alone displaced
    ///   its intro text behind its own calls. `emission_scope` now uses the attribute *family* for split
    ///   carriers and the payload root for event carriers - `(span, direction)` was the tempting
    ///   generalisation and overreaches, since one span can hold several emissions.
    ///
    ///   Effect, measured: two fixtures, both repairs. An `adk/tool_use` span view showed both tool
    ///   *results before* both calls and now reads `call, result, call, result`; the whole-view
    ///   causality invariant had missed it because span views are exempt (a span usually holds only one
    ///   half of a pair) and this span holds both. `agent-framework/swarm` stops batching all three
    ///   specialists' system prompts ahead of all three answers and interleaves each prompt with its own
    ///   agent's reply.
    ///
    /// Every class is now promoted. What remains is not a dial:
    ///
    /// - **The dataflow class still emits the product** of a span's inputs and outputs where the two
    ///   sets overlap. Elsewhere a barrier node bounds it to `in + out`; the pairwise form is kept only
    ///   for the overlap case, which no corpus fixture reaches. In practice outputs number one to three,
    ///   so the product is near-linear; adversarially it is not bounded.
    ///
    /// Replay matching used to be on this list. It is now injective matching against
    /// [`causal_precedence`], which is a *different* relation from this graph on purpose - see that
    /// type for why a presentation graph cannot answer a causality question.
    pub(super) const PRODUCTION: Self = Self {
        pairwise_dataflow_edges: false,
        contract_non_contiguous_emissions: true,
        enforce_backward_edges: true,
        time_priority: true,
        source_position_member_order: true,
        carrier_sequence_edges: true,
        generation_dataflow_edges: true,
        request_framing_edges: true,
    };

    /// Every constraint enforced - the redesign's intended answer.
    #[cfg(test)]
    pub(super) const FULL: Self = Self {
        pairwise_dataflow_edges: false,
        contract_non_contiguous_emissions: true,
        enforce_backward_edges: true,
        time_priority: true,
        source_position_member_order: true,
        carrier_sequence_edges: true,
        generation_dataflow_edges: true,
        request_framing_edges: true,
    };
}

/// Resolve the order over the surviving blocks.
///
/// `pre_dedup` is the classified evidence set (every observation); `survivors` is the deduplicated
/// result in the current pipeline order — that order is the deterministic tie-break and the
/// neutrality seed. Returns the survivors permuted into the partial order's resolution.
///
/// # Neutrality
///
/// Under [`Constraints::NEUTRAL`] the result is exactly `survivors`. Every enforced edge is already
/// forward in the legacy order and every contracted unit is already contiguous in it, so the legacy
/// order is itself a topological order of the graph; popping the ready unit with the smallest legacy
/// index therefore yields the legacy order, because any predecessor of the smallest-index unfinished
/// unit would have a smaller index and would already be done.
pub(super) fn resolve(
    evidence: &[OrderEvidence],
    survivors: &[BlockEntry],
    lineage: &[Option<usize>],
    span_timestamps: &HashMap<String, SpanTimestamps>,
    constraints: Constraints,
) -> Vec<BlockEntry> {
    let n = survivors.len();
    if n <= 1 {
        return survivors.to_vec();
    }

    // Which survivor each observation became, as the pipeline recorded it - not recomputed here.
    //
    // Recomputing was wrong twice over. Survivors are not unique by `MessageIdentity`, because dedup
    // keys on `(identity, repeat ordinal)`: a response holding two identical tool calls with distinct
    // ids keeps both, and one map entry then took the other's evidence (`crewai/mcp_tools` is the
    // corpus trace that does this). And `withdraw_unbacked_ids` runs in between, clearing a
    // correlated result's id - which changes its identity outright, so its evidence stopped matching
    // anything at all.
    let survivor_of =
        |observation: usize| -> Option<usize> { lineage.get(observation).copied().flatten() };

    // Co-emission sets from the evidence: group credible-emission observations by instance, collect
    // the surviving identities in each, in source order. A block whose identity did not survive is
    // ignored - the unit is over survivors.
    let mut by_instance: HashMap<usize, Vec<EmissionMember>> = HashMap::new();
    for (observation, seen) in evidence.iter().enumerate() {
        let Some(instance) = seen.emission else {
            continue;
        };
        let Some(survivor) = survivor_of(observation) else {
            continue;
        };
        by_instance.entry(instance).or_default().push((
            seen.message_index,
            seen.entry_index,
            survivor,
        ));
    }

    // Contract each instance's survivors into one unit, and remember the source order within it.
    //
    // Iterated in a deterministic order: a HashMap's iteration order varies per run, and while
    // union-find's result does not depend on the order the unions arrive in, the intra-unit keys and
    // any later diagnostics do.
    let mut instances: Vec<(&usize, &Vec<EmissionMember>)> = by_instance.iter().collect();
    instances.sort_by_key(|(instance, _)| **instance);

    // A survivor is routinely claimed by *two* emission instances: an inner generation span emits a
    // message and its parent agent span re-emits the same one as its own output. That is the common
    // shape, not an anomaly, so instances are merged rather than rejected - and the consequence is
    // that a unit's members can carry position paths rooted in different payloads, whose coordinates
    // are not comparable. Taking a global minimum position across them can violate the source order
    // of both emissions.
    //
    // So each instance contributes *adjacency* rather than coordinates: consecutive members of one
    // emission become an edge, and a unit's members are ordered by resolving those edges. Two
    // emissions that agree are both honoured; if they disagree the unit falls back to legacy order,
    // which is the only answer that cannot claim to satisfy evidence it contradicts.
    let mut uf = UnionFind::new(n);
    let mut intra_edges: Vec<(usize, usize)> = Vec::new();
    for (_, members) in instances {
        let mut legacy: Vec<usize> = members.iter().map(|&(_, _, s)| s).collect();
        legacy.sort_unstable();
        legacy.dedup();

        if !constraints.contract_non_contiguous_emissions
            && !legacy.windows(2).all(|w| w[1] == w[0] + 1)
        {
            continue;
        }

        // This emission's own order, by source position, deduplicated: two blocks of one message can
        // map to one survivor.
        let mut ordered: Vec<EmissionMember> = members.clone();
        ordered.sort_by_key(|&(msg_idx, entry_idx, survivor)| (msg_idx, entry_idx, survivor));
        let mut sequence: Vec<usize> = Vec::with_capacity(ordered.len());
        for (_, _, survivor) in ordered {
            if sequence.last() != Some(&survivor) {
                sequence.push(survivor);
            }
        }
        for pair in sequence.windows(2) {
            if pair[0] != pair[1] {
                intra_edges.push((pair[0], pair[1]));
            }
        }
        // Union all survivors of this instance together.
        let first = sequence[0];
        for &survivor in &sequence[1..] {
            uf.union(first, survivor);
        }
    }

    let unit_of: Vec<usize> = (0..n).map(|i| uf.find(i)).collect();

    // Priority per unit: the earliest time the *evidence* gives it. Time seeds the topological pop;
    // it never forces an order an edge does not.
    //
    // Taken from the pre-dedup occurrences, not from the survivors. A survivor's `timestamp` has
    // already been overwritten by `process_dedup` with its old batch anchor, so reading it here would
    // make the new order a function of the order it is replacing - and would carry the very
    // copy-survival dependence this redesign exists to remove: whichever copy won would decide the
    // anchor. Only a credible emission counts as evidence of a time (a re-listed snapshot's time is
    // when it was assembled), with the survivor's own effective time as the fallback where an
    // identity has no emission at all - a user message read from an attribute array, say.
    //
    // The fallback is the minimum effective time over **every observation** projected to the unit,
    // never the chosen survivor's alone. The survivor's copy is picked by quality and rescue rules
    // that legitimately change - the history rescue keeps the *earliest* candidate today, and a rule
    // change there must not move an unrelated block. With two identical user copies at t=10 and t=30
    // and an unrelated credible unit at t=20, reading the survivor's time put the user before or
    // after it depending on which copy was rescued; the minimum over both is the same fact whichever
    // copy wins.
    let mut unit_priority: HashMap<usize, DateTime<Utc>> = HashMap::new();
    let record = |unit: usize, time: DateTime<Utc>, map: &mut HashMap<usize, DateTime<Utc>>| {
        map.entry(unit)
            .and_modify(|t| {
                if time < *t {
                    *t = time;
                }
            })
            .or_insert(time);
    };
    let mut from_emission: HashMap<usize, DateTime<Utc>> = HashMap::new();
    let mut from_any_observation: HashMap<usize, DateTime<Utc>> = HashMap::new();
    for (observation, seen) in evidence.iter().enumerate() {
        let Some(survivor) = survivor_of(observation) else {
            continue;
        };
        record(unit_of[survivor], seen.effective, &mut from_any_observation);
        if !seen.credible {
            continue;
        }
        record(unit_of[survivor], seen.effective, &mut from_emission);
    }
    for (i, block) in survivors.iter().enumerate() {
        let unit = unit_of[i];
        let time = from_emission
            .get(&unit)
            .copied()
            .or_else(|| from_any_observation.get(&unit).copied())
            .unwrap_or_else(|| effective_timestamp(block, span_timestamps));
        unit_priority
            .entry(unit)
            .and_modify(|t| {
                if time < *t {
                    *t = time;
                }
            })
            .or_insert(time);
    }
    // The smallest legacy index in each unit: the neutrality seed, and the tie-break of last resort.
    let mut unit_min_legacy: HashMap<usize, usize> = HashMap::new();
    for (i, &unit) in unit_of.iter().enumerate() {
        unit_min_legacy
            .entry(unit)
            .and_modify(|m| *m = (*m).min(i))
            .or_insert(i);
    }

    // Where each unit was *first observed*, over every observation that projects to it.
    //
    // This is the tie-break that survivor choice cannot move. The legacy index is the position of the
    // *surviving* block in the previous sort, so two units with nothing ordering them were separated by
    // which copy happened to win dedup - which is the dependence this redesign exists to remove, and
    // what makes `adk/tool_use` group its tool calls in two traces and interleave them in a third.
    // First observation is a property of the evidence: it considers every copy, so it does not change
    // when a different one survives.
    let mut unit_first_seen: HashMap<usize, usize> = HashMap::new();
    for (observation, _) in evidence.iter().enumerate() {
        let Some(survivor) = survivor_of(observation) else {
            continue;
        };
        unit_first_seen
            .entry(unit_of[survivor])
            .and_modify(|m| *m = (*m).min(observation))
            .or_insert(observation);
    }

    // Exact call -> result edges over units. A result's unit follows its call's unit, but only when
    // exactly one surviving call carries the id: a reused or regenerated id is ambiguous and adds no
    // edge (Codex: do not treat "first call with this id" as a hard constraint).
    let mut call_units: HashMap<&str, Vec<usize>> = HashMap::new();
    for (i, block) in survivors.iter().enumerate() {
        if block.entry_type == "tool_use"
            && let Some(id) = block.tool_use_id.as_deref().filter(|s| !s.is_empty())
        {
            call_units.entry(id).or_default().push(unit_of[i]);
        }
    }
    let units: Vec<usize> = {
        let mut u: Vec<usize> = unit_priority.keys().copied().collect();
        u.sort_unstable();
        u
    };
    // Under the scaffold the seed is the legacy index alone (`None` sorts before any `Some`, so the
    // time term drops out entirely): that is what makes the resolve reproduce the legacy order rather
    // than merely agree with it on this corpus. Promoting time to the primary key is its own delta.
    let key = |u: usize,
               unit_priority: &HashMap<usize, DateTime<Utc>>,
               unit_min_legacy: &HashMap<usize, usize>| {
        let primary = if constraints.time_priority {
            Some(unit_priority[&u])
        } else {
            None
        };
        // Under `NEUTRAL` the legacy index alone decides, which is what makes the neutrality proof
        // hold. Otherwise first-observation breaks the tie and the legacy index is the last resort,
        // for a unit no observation projects to.
        let secondary = if constraints.time_priority {
            unit_first_seen
                .get(&u)
                .copied()
                .unwrap_or(unit_min_legacy[&u])
        } else {
            unit_min_legacy[&u]
        };
        (primary, secondary, u)
    };

    // Keys first, because the dataflow class needs one for the barrier node it introduces. Computed
    // once per unit rather than per edge: rebuilding a key on every indegree decrement made the resolve
    // cost scale with the edge count, which is what the barrier exists to bound.
    let mut keys_of_units: HashMap<usize, PopKey> = units
        .iter()
        .map(|&u| (u, key(u, &unit_priority, &unit_min_legacy)))
        .collect();
    let mut barriers: Vec<usize> = Vec::new();

    let mut successors: HashMap<usize, Vec<usize>> =
        units.iter().map(|&u| (u, Vec::new())).collect();
    let mut indegree: HashMap<usize, usize> = units.iter().map(|&u| (u, 0)).collect();
    let mut edges: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();
    for (i, block) in survivors.iter().enumerate() {
        if block.entry_type != "tool_result" {
            continue;
        }
        let Some(id) = block.tool_use_id.as_deref().filter(|s| !s.is_empty()) else {
            continue;
        };
        let Some(callers) = call_units.get(id) else {
            continue;
        };
        if callers.len() != 1 {
            continue; // ambiguous id
        }
        let call_unit = callers[0];
        let result_unit = unit_of[i];
        if call_unit == result_unit {
            continue; // one emission already orders them
        }
        add_edge(
            call_unit,
            result_unit,
            constraints,
            &unit_min_legacy,
            &mut successors,
            &mut indegree,
            &mut edges,
        );
    }

    // Carrier sequence: the order a payload states between its own surviving observations.
    //
    // Adjacent pairs only; the transitive closure adds nothing to a topological order. Ordered by the
    // payload position, which is what `message_index`/`entry_index` carry for a carrier's blocks.
    if constraints.carrier_sequence_edges {
        let mut by_carrier: HashMap<usize, Vec<(i32, i32, usize)>> = HashMap::new();
        for (observation, seen) in evidence.iter().enumerate() {
            if !seen.carrier_ordered {
                continue;
            }
            let Some(survivor) = survivor_of(observation) else {
                continue;
            };
            by_carrier.entry(seen.carrier).or_default().push((
                seen.message_index,
                seen.entry_index,
                survivor,
            ));
        }
        let mut carriers: Vec<&usize> = by_carrier.keys().collect();
        carriers.sort_unstable();
        let carriers: Vec<usize> = carriers.into_iter().copied().collect();
        for carrier in carriers {
            let mut members = by_carrier.remove(&carrier).unwrap_or_default();
            members.sort_unstable();
            let mut sequence: Vec<usize> = Vec::with_capacity(members.len());
            for (_, _, survivor) in members {
                let unit = unit_of[survivor];
                if sequence.last() != Some(&unit) {
                    sequence.push(unit);
                }
            }
            for pair in sequence.windows(2) {
                add_edge(
                    pair[0],
                    pair[1],
                    constraints,
                    &unit_min_legacy,
                    &mut successors,
                    &mut indegree,
                    &mut edges,
                );
            }
        }
    }

    // Generation dataflow: what a model call received precedes what it produced.
    //
    // Read from the evidence rather than from the survivors, because the copy on display often comes
    // from a different span - the answer a chain span re-lists is still the generation's output, and
    // the system prompt a generation received is still its input even when the surviving copy of it
    // was read somewhere else.
    //
    // Only edges between *different* units are added, and an edge already implied by contraction is
    // skipped. Under the scaffold an edge the legacy order has backwards is dropped, as everywhere.
    if constraints.generation_dataflow_edges {
        // Ordered sets, not vectors with a membership scan: a generation span re-sending a long
        // history has hundreds of input observations mapping to a handful of units, and checking each
        // against a growing `Vec` made collecting them quadratic in the observation count. `BTreeSet`
        // also fixes the iteration order, which a `HashSet` would leave to chance.
        let mut inputs_by_span: HashMap<usize, BTreeSet<usize>> = HashMap::new();
        let mut outputs_by_span: HashMap<usize, BTreeSet<usize>> = HashMap::new();
        for (observation, seen) in evidence.iter().enumerate() {
            if !seen.from_generation {
                continue;
            }
            // History is *kept* on the input side here, unlike carrier-sequence edges. The two ask
            // different questions of a re-send. "These messages precede what this generation produced"
            // is true of a replayed input and is exactly the evidence that pins a tool result before
            // the answer that cites it - no other class states that, because the consuming span is the
            // only place the two meet. "These messages are in this relative order globally" is *not*
            // true of a replay, which is what dragged ADK's second system prompt to the front of a
            // session, so that reading stays gated there.
            let Some(survivor) = survivor_of(observation) else {
                continue;
            };
            let side = if seen.is_output {
                &mut outputs_by_span
            } else {
                &mut inputs_by_span
            };
            side.entry(seen.span).or_default().insert(unit_of[survivor]);
        }
        let mut spans: Vec<&usize> = inputs_by_span.keys().collect();
        spans.sort_unstable();
        let spans: Vec<usize> = spans.into_iter().copied().collect();
        for span in spans {
            let Some(outputs) = outputs_by_span.get(&span) else {
                continue;
            };
            let inputs = &inputs_by_span[&span];

            // Overlapping sets keep the pairwise form. For inputs `{u, a}` and outputs `{u, b}` the
            // product contains `u -> b`, and a barrier cannot express that without also asserting
            // `u -> barrier -> u`. Dropping `u` from the input side loses the real edge; keeping it
            // invents a cycle.
            //
            // Defensive rather than measured, and worth saying: no fixture in the corpus has a span
            // that both received and produced the same message, so removing this guard does not fail
            // `a_barrier_orders_exactly_as_pairwise_edges_do`. The equivalence that test *does* check is
            // the one that matters for the bound - and overlap being absent is why the bound holds
            // everywhere it is measured.
            let overlaps = inputs.iter().any(|u| outputs.contains(u));
            if overlaps || constraints.pairwise_dataflow_edges {
                for &input in inputs {
                    for &output in outputs {
                        add_edge(
                            input,
                            output,
                            constraints,
                            &unit_min_legacy,
                            &mut successors,
                            &mut indegree,
                            &mut edges,
                        );
                    }
                }
                continue;
            }

            // A barrier, rather than an edge from every input to every output.
            //
            // "Everything received precedes everything produced" is the *product* of the two sets as
            // pairwise edges, and a span re-sending a long history has hundreds of inputs - so the graph
            // grew quadratically in a span's message count. One barrier node expresses the same relation
            // in `inputs + outputs` edges: every input precedes the barrier, the barrier precedes every
            // output, and precedence is transitive.
            //
            // Its key is the smallest key among its outputs, so it is popped exactly when the earliest
            // output would have been - it emits nothing, and the resulting order is unchanged.
            let barrier = barrier_unit(span, survivors.len());
            let barrier_key = outputs
                .iter()
                .filter_map(|u| keys_of_units.get(u).copied())
                .min();
            let barrier_legacy = outputs
                .iter()
                .filter_map(|u| unit_min_legacy.get(u).copied())
                .min();
            let (Some(barrier_key), Some(barrier_legacy)) = (barrier_key, barrier_legacy) else {
                continue;
            };
            keys_of_units.insert(barrier, barrier_key);
            unit_min_legacy.insert(barrier, barrier_legacy);
            successors.entry(barrier).or_default();
            indegree.entry(barrier).or_insert(0);
            barriers.push(barrier);

            for &input in inputs {
                add_edge(
                    input,
                    barrier,
                    constraints,
                    &unit_min_legacy,
                    &mut successors,
                    &mut indegree,
                    &mut edges,
                );
            }
            for &output in outputs {
                add_edge(
                    barrier,
                    output,
                    constraints,
                    &unit_min_legacy,
                    &mut successors,
                    &mut indegree,
                    &mut edges,
                );
            }
        }
    }

    // Request framing: a detached frame precedes the inputs *first seen* in its own request.
    //
    // A framework reports the system instruction on the span that *sent* it - the generation span -
    // while the question arrived on an orchestration span that started earlier, so ordering by evidence
    // time put the frame after the question. 27 trace views across 22 fixtures, every one displaced by
    // exactly one position (`a_system_instruction_precedes_the_first_user_turn`).
    //
    // The request is the generation span that carried the frame; the fact is the carrier's
    // (`carrier_is_detached_request_frame`), never the role's, since `developer` normalises to System
    // and an in-band system message is a turn in its array. Both sides derive from *all* pre-dedup
    // observations projected through lineage, like the dataflow class above, so the surviving copy of
    // the question inherits the edge wherever it sits - reading the survivor's own span instead would
    // let a quality tie decide which request gets the edge, which
    // `which_copy_survives_does_not_change_the_order` forbids.
    //
    // Two exclusions on the target side, and the first was learned from a regression rather than
    // deduced. "The frame precedes this input" is true *within one request's payload* and false as a
    // global statement about a replay: `agent-framework/swarm` hands the conversation through four
    // agents, each re-sending it under a new instruction, and framing every input dragged all four
    // instructions to the front of the trace. So a frame precedes only what its request saw **first** -
    // an input already listed by an earlier request belongs to that turn, and an earlier generation's
    // *output* precedes this frame by conversation order however this request re-consumed it.
    if constraints.request_framing_edges {
        let mut frames_by_span: HashMap<usize, BTreeSet<usize>> = HashMap::new();
        let mut inputs_by_span: HashMap<usize, BTreeSet<usize>> = HashMap::new();
        let mut generation_outputs: BTreeSet<usize> = BTreeSet::new();
        let mut span_first_effective: HashMap<usize, DateTime<Utc>> = HashMap::new();
        // The ordered members of each input array, per (span, family): `(message, entry, unit)`.
        type ArrayMembers = HashMap<usize, Vec<EmissionMember>>;
        let mut arrays_by_span: HashMap<usize, ArrayMembers> = HashMap::new();
        for (observation, seen) in evidence.iter().enumerate() {
            if !seen.from_generation {
                continue;
            }
            let Some(survivor) = survivor_of(observation) else {
                continue;
            };
            let unit = unit_of[survivor];
            if seen.is_output {
                generation_outputs.insert(unit);
                continue;
            }
            span_first_effective
                .entry(seen.span)
                .and_modify(|t| {
                    if seen.effective < *t {
                        *t = seen.effective;
                    }
                })
                .or_insert(seen.effective);
            if let Some(family) = seen.input_family {
                arrays_by_span
                    .entry(seen.span)
                    .or_default()
                    .entry(family)
                    .or_default()
                    .push((seen.message_index, seen.entry_index, unit));
            }
            let side = if seen.detached_frame {
                &mut frames_by_span
            } else {
                &mut inputs_by_span
            };
            side.entry(seen.span).or_default().insert(unit);
        }
        // Requests in the order they happened, so "first seen" is well defined; the span id breaks a
        // timestamp tie deterministically.
        let mut requests: Vec<usize> = frames_by_span
            .keys()
            .chain(inputs_by_span.keys())
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        requests.sort_by_key(|span| (span_first_effective.get(span).copied(), *span));

        let mut already_seen: BTreeSet<usize> = BTreeSet::new();
        // Requests that only carry arrays still take part in first-seen accounting.
        for span in arrays_by_span.keys() {
            if !requests.contains(span) {
                requests.push(*span);
            }
        }
        requests.sort_by_key(|span| (span_first_effective.get(span).copied(), *span));
        for span in requests {
            let inputs = inputs_by_span.get(&span).cloned().unwrap_or_default();
            // An ordered input array orders the request's **first-seen** members. The array's own
            // positions are the evidence, history included - the framing block projects everything
            // through lineage - and first-seen is what neutralises a replay: a member already listed by
            // an earlier request belongs to that turn, and an earlier generation's output keeps its
            // conversation position, so a changed instruction at position 0 of a replaying array
            // (`adk/image_gen`'s critic) frames only its own request's new turns. This is what lets the
            // array order a frame that lives *inside* it - langgraph's `llm.input_messages` carries
            // `system@0, user@1`, and extraction fragments that array into per-index carriers, so the
            // ordinary carrier-sequence class never sees it whole.
            if let Some(families) = arrays_by_span.get(&span) {
                let mut family_ids: Vec<&usize> = families.keys().collect();
                family_ids.sort_unstable();
                for family in family_ids {
                    let mut members = families[family].clone();
                    members.sort_unstable();
                    let mut sequence: Vec<usize> = Vec::new();
                    // A set beside the vector, because `Vec::contains` made this quadratic in the
                    // array's unique members and a replaying array re-lists the whole conversation.
                    let mut in_sequence: BTreeSet<usize> = BTreeSet::new();
                    for (_, _, unit) in members {
                        if already_seen.contains(&unit)
                            || generation_outputs.contains(&unit)
                            || !in_sequence.insert(unit)
                        {
                            continue;
                        }
                        sequence.push(unit);
                    }
                    for pair in sequence.windows(2) {
                        add_edge(
                            pair[0],
                            pair[1],
                            constraints,
                            &unit_min_legacy,
                            &mut successors,
                            &mut indegree,
                            &mut edges,
                        );
                    }
                }
            }
            if let Some(frames) = frames_by_span.get(&span) {
                for &frame in frames {
                    for &input in &inputs {
                        if frames.contains(&input)
                            || generation_outputs.contains(&input)
                            || already_seen.contains(&input)
                        {
                            continue;
                        }
                        add_edge(
                            frame,
                            input,
                            constraints,
                            &unit_min_legacy,
                            &mut successors,
                            &mut indegree,
                            &mut edges,
                        );
                    }
                }
            }
            already_seen.extend(inputs);
        }
    }

    // Kahn's algorithm, popping the ready unit with the smallest (priority, min-legacy, unit-id).
    // On a stall - a cycle - break it deterministically by the same key over the remaining units,
    // so the resolver is total rather than panicking. (Full SCC condensation is a later increment.
    // Two corpus fixtures *do* cycle - `ordering_contradictions_are_pinned` holds the set - and
    // both land on the correct order under this release rule.)
    let mut order: Vec<usize> = Vec::with_capacity(units.len());
    // Two ordered sets rather than a rescan. The loop used to look at every unit on every iteration
    // to find the ready ones, which is quadratic in the number of units - and a session view can hold
    // thousands. `ready` yields the next unit in O(log n); `remaining` exists only for the cycle case,
    // where nothing is ready and the smallest key has to be released to keep the resolve total.
    // Barriers are nodes too: they carry no blocks, but they have to be popped for their outputs to
    // become ready.
    let nodes: Vec<usize> = units.iter().copied().chain(barriers).collect();
    let keys = keys_of_units;

    let mut ready: BTreeSet<(PopKey, usize)> = BTreeSet::new();
    let mut remaining: BTreeSet<(PopKey, usize)> = BTreeSet::new();
    for &u in &nodes {
        let entry = (keys[&u], u);
        remaining.insert(entry);
        if indegree[&u] == 0 {
            ready.insert(entry);
        }
    }

    // Cycles released to keep the resolve total. A cycle means the *evidence* contradicts itself, which
    // is a fact about the telemetry or about a constraint class, and it used to be resolved in silence -
    // so nothing distinguished "the order is derived" from "the order is a deterministic guess after a
    // contradiction". Counted here and reported once below, rather than per release, because one
    // contradiction commonly strands several units.
    let mut cycles_broken = 0usize;

    while order.len() < nodes.len() {
        let next_entry = match ready.iter().next().copied() {
            Some(entry) => entry,
            // A cycle: the evidence contradicts itself. Release the smallest-key remaining unit so the
            // result is still a total order.
            None => {
                cycles_broken += 1;
                #[cfg(test)]
                CYCLES_BROKEN_IN_TESTS.with(|c| *c.borrow_mut() += 1);
                remaining
                    .iter()
                    .next()
                    .copied()
                    .expect("a node remains while the order is incomplete")
            }
        };
        ready.remove(&next_entry);
        remaining.remove(&next_entry);
        let next = next_entry.1;
        order.push(next);
        for &s in &successors[&next] {
            if let Some(d) = indegree.get_mut(&s) {
                *d = d.saturating_sub(1);
                if *d == 0 {
                    let entry = (keys[&s], s);
                    if remaining.contains(&entry) {
                        ready.insert(entry);
                    }
                }
            }
        }
    }

    if cycles_broken > 0 {
        tracing::warn!(
            cycles_broken,
            units = units.len(),
            blocks = n,
            trace_id = survivors.first().map(|b| b.trace_id.as_str()),
            "ordering evidence contradicts itself; released units deterministically to keep the order \
             total - the result is a consistent guess rather than a derived order"
        );
    }

    // Emit each unit's members, ordered by the emissions' own adjacency.
    let mut members_of: HashMap<usize, Vec<usize>> =
        units.iter().map(|&u| (u, Vec::new())).collect();
    for (i, &unit) in unit_of.iter().enumerate() {
        members_of.get_mut(&unit).expect("unit present").push(i);
    }
    let mut out = Vec::with_capacity(n);
    for unit in order {
        let mut members = members_of.remove(&unit).unwrap_or_default();
        members.sort_unstable();
        if constraints.source_position_member_order {
            members = order_within_unit(&members, &intra_edges);
        }
        for i in members {
            out.push(survivors[i].clone());
        }
    }
    out
}

/// Add one precedence edge between units, unless the scaffold forbids it.
///
/// The scaffold enforces only an edge the legacy order already respects, so no edge of its graph can
/// move anything - which is what makes the resolver safe to run in production before any class is
/// promoted. Duplicate edges are skipped so an indegree cannot be counted twice.
fn add_edge(
    from: usize,
    to: usize,
    constraints: Constraints,
    unit_min_legacy: &HashMap<usize, usize>,
    successors: &mut HashMap<usize, Vec<usize>>,
    indegree: &mut HashMap<usize, usize>,
    edges: &mut std::collections::HashSet<(usize, usize)>,
) {
    if from == to {
        return;
    }
    let backward = unit_min_legacy[&from] > unit_min_legacy[&to];
    if backward && !constraints.enforce_backward_edges {
        return;
    }
    // Set membership, not a linear scan of the successor list: a generation span with many inputs and
    // many outputs produces their product in edges, and checking each against a growing `Vec` made
    // building the graph quadratic in that product.
    if edges.insert((from, to)) {
        successors.get_mut(&from).expect("unit present").push(to);
        *indegree.get_mut(&to).expect("unit present") += 1;
    }
}

/// Order one unit's members by the adjacency its emissions stated, smallest legacy index first among
/// the members nothing else has to precede.
///
/// Falls back to the given (legacy) order when the edges restricted to this unit contain a cycle: two
/// emissions disagreeing about the order of the same pair is a contradiction in the evidence, and
/// legacy order is the one answer that does not claim to satisfy either.
fn order_within_unit(members: &[usize], intra_edges: &[(usize, usize)]) -> Vec<usize> {
    if members.len() < 2 {
        return members.to_vec();
    }
    let inside: HashMap<usize, ()> = members.iter().map(|&m| (m, ())).collect();
    let mut successors: HashMap<usize, Vec<usize>> =
        members.iter().map(|&m| (m, Vec::new())).collect();
    let mut indegree: HashMap<usize, usize> = members.iter().map(|&m| (m, 0)).collect();
    for &(from, to) in intra_edges {
        if !inside.contains_key(&from) || !inside.contains_key(&to) {
            continue;
        }
        let succ = successors.get_mut(&from).expect("member present");
        if !succ.contains(&to) {
            succ.push(to);
            *indegree.get_mut(&to).expect("member present") += 1;
        }
    }

    let mut out: Vec<usize> = Vec::with_capacity(members.len());
    let mut remaining: Vec<usize> = members.to_vec();
    while !remaining.is_empty() {
        let Some(&next) = remaining
            .iter()
            .filter(|m| indegree[m] == 0)
            .min_by_key(|&&m| m)
        else {
            // Cycle: the emissions contradict each other about this unit.
            return members.to_vec();
        };
        out.push(next);
        remaining.retain(|&m| m != next);
        for &s in &successors[&next] {
            if let Some(d) = indegree.get_mut(&s) {
                *d = d.saturating_sub(1);
            }
        }
    }
    out
}

#[cfg(test)]
mod cycle_tests {
    use super::*;
    use crate::data::types::MessageCategory;
    use crate::domain::sideml::provenance::PositionPath;
    use crate::domain::sideml::types::{ChatRole, ContentBlock};
    use chrono::TimeZone;

    fn block(span: &str, text: &str) -> BlockEntry {
        BlockEntry {
            position: PositionPath::default(),
            entry_type: "text".to_string(),
            content: ContentBlock::Text {
                text: text.to_string(),
            },
            role: ChatRole::User,
            trace_id: "trace-1".to_string(),
            span_id: span.to_string(),
            session_id: None,
            message_index: 0,
            entry_index: 0,
            parent_span_id: None,
            span_path: vec![span.to_string()],
            timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            order_time: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            observation_type: None,
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
            source_type: "attribute".to_string(),
            event_name: None,
            source_attribute: Some("carrier".to_string()),
            category: MessageCategory::GenAIUserMessage,
            content_hash: text.to_string(),
            is_semantic: true,
            uses_span_end: false,
            is_history: false,
            tool_use_id_correlated: false,
            promoted_to_span_output: false,
        }
    }

    fn evidence(carrier: usize, position: i32) -> OrderEvidence {
        OrderEvidence {
            emission: None,
            message_index: position,
            entry_index: 0,
            effective: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            credible: false,
            span: 0,
            carrier,
            carrier_ordered: true,
            is_output: false,
            from_generation: false,
            detached_frame: false,
            input_family: None,
        }
    }

    /// Contradictory evidence still yields every block exactly once.
    ///
    /// Two carriers state opposite orders for the same pair - carrier 0 says `a` then `b`, carrier 1
    /// says `b` then `a` - which is a genuine contradiction in the telemetry and the only way the
    /// resolver's stall branch is reached. That branch had **never executed in the test suite**: no
    /// corpus fixture cycles at this constraint density, so the one path whose job is to keep a
    /// contradiction from becoming a lost message was unverified.
    ///
    /// What is asserted is the property that must survive a contradiction: the answer is still a total
    /// order over exactly the survivors. Which of the two wins is deliberately not asserted - it is a
    /// deterministic tie-break over a contradiction, not a fact about the conversation.
    #[test]
    fn contradictory_evidence_still_returns_every_block_exactly_once() {
        let survivors = vec![block("span-a", "a"), block("span-b", "b")];
        // Four observations: carrier 0 orders (a, b), carrier 1 orders (b, a).
        let evidence_set = vec![
            evidence(0, 0),
            evidence(0, 1),
            evidence(1, 0),
            evidence(1, 1),
        ];
        let lineage = vec![Some(0), Some(1), Some(1), Some(0)];

        let resolved = resolve(
            &evidence_set,
            &survivors,
            &lineage,
            &HashMap::new(),
            Constraints::PRODUCTION,
        );

        assert_eq!(
            resolved.len(),
            survivors.len(),
            "a contradiction must not drop or duplicate a block"
        );
        let mut seen: Vec<&str> = resolved.iter().map(|b| b.span_id.as_str()).collect();
        seen.sort_unstable();
        assert_eq!(
            seen,
            vec!["span-a", "span-b"],
            "every survivor appears exactly once"
        );
    }
}
