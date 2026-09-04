# Ingestion architecture

**Status**: design, under review. Revision 8. Revision log at the end.

What this describes: how OTLP spans from any GenAI framework become the message list the span,
trace, session and feed views return. Not how they are stored, queued or served.

## The problem this replaces

Five layers of heuristics decide the answer today — extractor selection, carrier semantics,
history classification, deduplication, ordering — and their interactions are not predictable.
The evidence is measured, not felt:

| Change attempted | Corpus ripple | Target case fixed |
| --- | --- | --- |
| Text from an atomic-emission carrier uses `span_end` | 29 fixtures reordered | no |
| Prefer a generation span's copy over an agent span's | 22 fixtures reordered | no |
| Let the generic reader fill a side a dialect left empty | langgraph expanded 12 → 28 messages | no |
| Six earlier ordering candidates (recorded in the plan file) | varies | no |

The root cause is a category error: **the same carrier name means different things on
different spans.** `gen_ai.output.messages` on a `chat` span is one emission the span produced;
the identical key on the root `invoke_agent` span restates the whole conversation that span
orchestrated. The carrier table is keyed by attribute *name*, so it cannot express the
difference — and every downstream layer inherits the ambiguity.

**The motivating example is weaker than it first looked, and that is worth stating precisely.** An
ordering failure of this shape was observed while capturing `vercel-ai-js/tool-use` under the Vercel
AI SDK's current OpenTelemetry integration:

```
observed: system, user, assistant(final text), assistant(tool_use), assistant(tool_use), tool, tool
correct:  system, user, assistant(tool_use), assistant(tool_use), tool, tool, assistant(final text)
```

That capture was reverted and is not in the repo, so it is not reproducible evidence. Two things
were then measured against what *is* committed:

- the committed `vercel-ai-js/tool-use` golden (legacy `ai.*` shape) is **correctly ordered** in all
  three of its traces;
- `_synthetic/carrier_collision_agent_and_generation` — written for this design, pure semconv, a root
  `invoke_agent` restating the whole conversation over two generation spans and a tool span, with no
  framework declared — is also **correctly ordered** (`system, user, tool_use, tool_result, text`).

So the bare carrier collision is *not* sufficient to produce the defect: today's heuristics happen to
resolve this shape, because the tool span's own result outranks the root's copy and each generation
span anchors its own response. The collision remains a category error the model cannot express, and
the fixture is kept as the canonical statement of it — but the argument for this redesign does not
rest on it.

**What the argument rests on is the ripple table above.** A change that reorders 29 fixtures while
leaving its target case untouched is a statement about the layers, not about the change: the output
order is one global tuple whose terms come from heuristics, so every term is coupled to every shape.
That is the fragility, and it is reproducible.

The third row was measured during review 6 and is the cleanest demonstration yet, because the
change is one a reviewer would call obviously right. `raw_io` — the reader for the framework-agnostic
`input.value`/`output.value` pair — runs only when *no* dialect extractor produced anything
(`messages.rs`, `!any_specific`). That is a gate on a different carrier: a span whose question arrives
as `llm.input_messages` and whose answer sits only in `output.value` returns the question alone.

Three attempts to open it, each measured:

| Attempt | Result |
| --- | --- |
| run the generic reader unconditionally | duplicated the answer on `answer_beside_the_conversation` and `nested_state_messages` — it bypassed carrier claiming |
| …respecting claims | `crewai/files` **lost its answer**: CrewAI serialises its whole agent configuration into `input.value`, so `{"agent":{"role":…,"goal":…}}` arrived as a user message *after* the reply. (This is the same attribute that makes one CrewAI fixture gitignored for holding credentials.) |
| …admit only the *answer* side, gated on the declared `carrier_holds_span_output` | langgraph 12 → 28 messages: `output.value` on a `Prompt` or `should_continue` node is node state, not an answer |

The third failure is the thesis restated at the smallest possible scale: **`output.value` means "the
answer" on a generation span and "node state" on a chain span**, and no gate over carrier *names* can
tell them apart.

**And that is where the redesign paid for itself the first time.** The separating fact is the span's
observation type — a carrier *instance*, which is this document's whole principle — and it turned out
to be free: `detect_observation_type` is a pure function of the span name and attributes, already
computed for every span before message extraction runs. Admitting the generic answer carrier only on a
generation span, and only when nothing already accounts for the span's output, is corpus-neutral across
all 119 fixtures and repairs the shape (`1e623a45`,
`_synthetic/dialect_question_generic_answer`). Mutation-verified in both directions: disabling the
block loses the answer, and dropping the observation-type guard reproduces the langgraph expansion.

So the third row of the ripple table is the one case where the diagnosis produced a fix rather than
another revert — and it did so *at ingest, with no schema change*, which is a fact the migration plan
had to absorb.

Two other real defects came out of the same session:

- `extract_json` returns `None` for anything `serde_json` rejects, so a **plain-text** `output.value`
  was dropped entirely — and `text/plain` is a documented OpenInference mime type. Now read as text
  (`_synthetic/plain_text_generic_io`). Its one corpus change is a repair: a `RunnableSequence` span
  view went from 0 messages to the prompt it actually carried.
- CrewAI's use of `input.value` for its whole agent configuration is why the *question* side keeps the
  stricter rule. Configuration is not a turn, and it is the same attribute that makes one CrewAI
  fixture gitignored for holding credentials.

## Cross-framework agreement is a falsifiability source, and it works

Review 7's conclusion was that the central distinction is only falsifiable against hand-annotated
ground truth. There is one source of ground truth the corpus already contains and nothing was using:
**the same sample program, run through different SDKs.** Eleven suites have a `tool_use` sample, and
the logical conversation is the same in each, so disagreement between them is evidence without an
annotation.

Measured, as a role/kind shape (`S`=system, `U`=user, `A`=assistant, `T`=tool, `*`=tool_use,
`r`=tool_result):

| Shape | Suites |
| --- | --- |
| `S U A* A* Tr Tr A` | adk, crewai, strands-js, vercel-ai-js — **four agreeing exactly** |
| `S U A* Tr A A U A* Tr A U A* Tr A` | agent-framework, openai-agents — two agreeing exactly |
| `U S A* Tr A …` (16 messages) | claude-agent-sdk and claude-agent-sdk-js — **Python/JS parity, exact** |
| `S U A A* A* Tr Tr A` | strands — the extra leading `A` is its genuine preamble text |
| `U S A* A* Tr Tr A` | langgraph |

The agreement is real and worth stating: four independent SDKs produce a byte-identical shape, and the
two Claude Agent SDKs agree across languages. That is a much stronger consistency claim than "the
goldens did not change".

**And the disagreement found a defect immediately.** Two suites place the **system instruction after
the user's question**. The cause is visible in the observation types:

| Suite | user message from | system instruction from |
| --- | --- | --- |
| langgraph | a `chain` span | the `generation` span |
| claude-agent-sdk | a plain `span` | the `generation` span |
| strands (correct) | its `agent` span | the same `agent` span |

So the system prompt is ordered *by when its evidence arrived*, and its evidence arrives on the
generation span — later than the question, which the orchestration span carried. Where both come from
one span, document order holds and the result is right.

That is a real presentation defect: a system instruction is not a turn that happened after the
question, it is the **frame the request was made in**.

**Attempting it as a scalar adjustment failed, and the failure is precise.** The obvious repair follows
the `after_call` precedent: give a lone system block the earliest response time in its trace, before
sorting, as a property of the block rather than of a compared pair. Measured:

| | Result |
| --- | --- |
| first form (move every system block) | `assert_carrier_subsequence` fired on `adk/image_gen` — ADK's `gcp.vertex.agent.llm_request` lists the whole request in *one* carrier, so the instruction is already first within it and moving that block out of its carrier group reversed the snapshot's own positions 0 and 2 |
| refined (skip a block sharing a carrier with non-system messages) | **15 feed views changed and not one trace view was repaired** |

The second row is the interesting one. Equalising `batch_time` only makes the instruction *tie* with
the question; the next term in the key is the span, and the orchestration span sorts before the
generation span — so `user, system` survives unchanged, while merging the block into the first response
run reorders the feed, which reverses runs as wholes. A scalar cannot express "before" without
inventing a time that is not a response's time, which would re-entangle the display timestamp that
`order_time` was split out to protect.

So this needs a **hard edge, not a key term**: `system → the other inputs of its request`, resolved in
`order_graph` where "before" is a constraint rather than a position. It is the first case in this
document where the constraint form is not merely tidier but *necessary*, and it is an input-framing
constraint rather than a causal one — a class the resolver does not yet have.

Reverted. Four measured attempts now, and this is the only one whose diagnosis names the mechanism that
would work.

Two things this establishes for the design. Cross-SDK agreement belongs in the verification apparatus
as a first-class check, not as an observation. And the ordering layer needs constraints for input
*framing*, not only for causality — which is an argument for the constraint graph that neither the
ripple table nor the invariant audit had produced.

## The principle

**Classify evidence, not messages.** The semantic unit is a *carrier instance* — a carrier
together with the span context it appears on — never a carrier name.

## The persistence boundary, which is not optional

Revisions 1-3 described one in-memory pipeline. The product does not have one: **extraction runs at
ingest and writes rows; normalisation runs per read, from those rows.** That split is deliberate — it
is what makes a parsing fix apply to history without re-ingestion — and it falls exactly between
stage 2 and stage 3.

What crosses that boundary today is `RawMessage { source, content }` plus a small span projection.
Measured, not assumed:

- **the instrumentation scope is never captured for spans at all.** Extraction iterates
  `scope_spans` and reads only `.spans` (`extract/mod.rs:335`); `scope_name`/`scope_version` columns
  exist in both analytics schemas for *metrics* only. `raw_span` is built from the span and resource
  attributes.
- the read path receives normalised message content and a span projection — not operation name,
  instrumentation scope, resource declarations or the physical carrier.

So a profile keyed on scope and version, which is what makes an undeclared producer decidable, **has
no input at read time.** For stage 3 to run where the design puts it, persistence has to retain:

| Fact | Available today |
| --- | --- |
| physical carrier locator: source kind, key, event ordinal, family membership | partly, via `MessageSource` |
| lossless raw carrier payload | no — content is normalised at ingest |
| span name, operation, observation type, hierarchy, both timestamps | in span rows, not in the message projection |
| instrumentation scope name and version | **no — never captured** |
| resource declarations | partly (`sideseat.framework` is read at ingest only) |
| source paths sufficient to regenerate stable observation positions | no |

That is a persisted-format change and a migration, and historical rows will be incomplete however it
is done — an unavoidable consequence of the facts not having been recorded. Adding fields to
`RawMessage` avoids a new column but still versions the stored JSON. This section exists because
revisions 1-3 said "not how they are stored" and then quietly assumed a single process with the whole
OTLP payload in hand.

**Consequence for the migration order**: capturing provenance and scope is step 1, and it is useful
on its own regardless of whether anything downstream is ever rewritten.

### Profiles must not be runtime-editable

The reconstruction cache is keyed on a BLAKE3 digest of the rows and nothing else (`feed/cache.rs`),
deliberately with no version constant: a changed row is a different key, so there is no invalidation
to get wrong. `every_field_of_a_row_reaches_the_digest` is a structural test that keeps
`MessageSpanRow` and the digest in agreement.

A **profile is configuration outside the rows**, and it changes the answer. So a runtime-editable
profile set breaks exactly the discipline that makes the cache safe: two reads of identical rows
would return different answers under one key. Two ways out, and only one of them is consistent with
the rest of the system:

- **profiles are build-time** — compiled in, changing only with the binary. A deploy replaces every
  process and every cache starts empty, so the invariant holds for free. This is the choice.
- profiles are runtime data — then every read must pin one immutable snapshot and the key becomes
  `(rows digest, session grouping, mode, canonical profile-snapshot digest)`, covering every selector,
  assignment and `refines` edge in canonical order, as a content digest rather than a replica-local
  generation counter. Any runtime-adjustable **budget or semantic flag that changes the answer**
  belongs in the same digest. That digest becomes something an operator can get wrong, and the same
  argument that rejected a persisted cache with a hand-maintained version constant rejects it.

The cost of the first is real and should be stated: onboarding a new private dialect needs a release,
not a config push; there are no per-tenant profiles; and during a rolling deploy two replicas may
interpret the same rows differently until it completes. Given that a dialect also needs a fixture and a
golden review, that is the honest cost of the guarantee rather than an extra restriction — and the
rolling-deploy window is the same one any behaviour change already has.

## Stages

```
raw carrier instances
   → syntax decoding            (payload shape → observations)
   → typed evidence claims      (what this instance is evidence of)
   → occurrences + causal relation
   → session replay reconciliation
   → presentation projection
```

Each stage may only know what is listed for it. A stage that needs a fact from a later stage is
a design error, not something to thread through.

### 1. Carrier capture

**A carrier is a bundle, not necessarily one OTLP key.** One emission is routinely spread across
sibling attributes, so a struct with one key and one payload cannot represent it without an implicit
span-wide lookup — which is the hidden coupling this stage exists to remove. Three in the corpus:

- `gen_ai.tool.call.arguments` is unusable without the sibling tool name and call id
  (`messages.rs:896`);
- OpenInference's dotted `llm.input_messages.*` attributes are one carrier, then enriched from a
  separate `input.value` (`messages.rs:1071`);
- Vercel spreads one emission over sibling `ai.response.*` attributes — the existing resolver already
  treats that family as one emission (`order_graph.rs:168`).

```rust
CarrierBundle {
    family_locator,    // trace / span / source kind / family / instance ordinal
    members,           // per member: key or event name, ordinal, raw payload
    span_context,      // operation name, span name, observation type, hierarchy, start and end
    instrumentation,   // scope name and version, resource declarations
}
```

Membership in a family is declared, not inferred by prefix matching at use time.

### 2. Syntax decoding

Decoders turn a bundle's syntax into normalised observations, each with its own claim.

The line, stated more precisely than revision 3's "may not decide identity or order", which was too
broad to be true — decoding an array *necessarily* fixes observation boundaries and their source
order:

| A decoder may decide | A decoder may not decide |
| --- | --- |
| **syntactic** identity: which bytes are one observation | **occurrence** identity: whether two observations are one event |
| **syntactic** order: position within its own payload | **global** order |
| explicit ids present in the payload, and their field names | what an id proves (`ReferenceAuthority`) |
| local grouping declared by the syntax | history, authority, production time, causality |

```rust
Observation { bundle_locator, position, normalized_content, explicit_ids, claim }
```

Two decoders in `messages.rs` **cannot** be written under this restriction and must be split:

- **OpenInference multimodal enrichment** (`messages.rs:1071`, `:1120`, `:1149`) decodes
  `llm.input_messages.*`, reads a *second* carrier `input.value`, matches the two by array index,
  declares one "strictly higher quality" and replaces the rendering. That is cross-carrier occurrence
  matching (stage 4) plus representative authority (stage 6) inside a decoder. Split three ways.
- **Claude Code's duplicate suppression** (`messages.rs:3767`) drops a name-tagged tool result because
  it judges it the same event as the id-tagged one. Under this design it emits both, and assembly
  reconciles them — with the id's `ReferenceAuthority` as the evidence, which is what makes the
  decision reviewable.

The rest of the per-framework parsers survive as decoders. Their *ownership* machinery — first match
wins, per-carrier claiming, registration order — does not: two decoders must not compete for a span
by the order they were registered.

### 3. Evidence claims

**A claim attaches to an observation, not to a carrier instance.** Revision 1 put it on the
instance and that was a cardinality error, refuted by a fixture already in the corpus.
`_synthetic/answer_beside_the_conversation` carries, on one `chat` span, one `output.value`:

```json
{"messages": [{"role": "user", "content": "ask"}], "raw": "the crewai answer"}
```

`messages` restates the question the span received; `raw` is the answer the span produced. The
extractor already emits both from that one source (`messages.rs`, the transcript members and the
sibling `raw` read). No instance-level claim can describe both, and no predicate over "payload
shape" repairs a cardinality error — it only hides it. So the decoder emits observations, and
each observation carries its own claim.

The claim is a **sum type over authority**, not a record of freely combinable fields, because the
combinations are not independent:

```rust
enum OccurrenceEvidence {
    /// This observation is the occurrence. Position proves multiplicity.
    Creates { temporal: EventTime | SpanCompletion, cohesion: Option<EmissionId> },
    /// This observation refers to an occurrence evidenced elsewhere. Position proves nothing,
    /// and it carries no production time — a restatement's timestamp is when it was copied.
    Restates { sequence: Ordered | Unordered },
    /// No producer contract covers this observation. See "Unknown", below.
    Unknown,
}

ObservationClaim {
    occurrence:  OccurrenceEvidence,
    direction:   Received | Produced | Neither,
    reference:   Option<ReferenceAuthority>,
}
```

`multiplicity_authority` is gone: it is exactly `matches!(occurrence, Creates { .. })`, given
invariant 5. A separate field for a derived fact is a second place to get it wrong. And the record
form admitted `Restates` with `SpanCompletion` authority, which is not a claim about anything.

**References carry authority too**, because "an unambiguous stable reference" in stage 4 is
otherwise the next hidden heuristic. `explicit_ids` records syntax; what stage 4 needs is what the
id *proves*:

```rust
ReferenceAuthority {
    kind:             OccurrenceIdentity | CorrelationOnly | MultiplicityWitness,
    scope:            Global | Trace | Span | Emission,
    replay_stability: Stable | Regenerated | Unknown,
}
```

The current code already depends on this distinction without naming it: `dedup.rs` deliberately
ignores a tool call's id for cross-copy identity (history re-sends regenerate it —
`Regenerated`), while two *distinct* ids within one response prove two calls
(`MultiplicityWitness`, scope `Emission`). Those are two different facts about the same syntax,
which is why one boolean could never carry them.

Applied to the failing Vercel case:

| Observation | occurrence | direction | temporal |
| --- | --- | --- | --- |
| `chat` span, `gen_ai.output.messages` | Creates | Produced | SpanCompletion |
| root `invoke_agent`, the same key | **Restates** | Produced | — (none by construction) |
| tool span, `gen_ai.tool.call.arguments` | Restates | Received | — |
| tool span, `gen_ai.tool.call.result` | Creates | Produced | SpanCompletion |

#### Profile selection

Not "predicates over the instance" — that phrasing in revision 1 was a hole big enough to hide
every heuristic being removed. A profile is a **partial, declarative rule**:

```rust
ProfileRule {
    id,
    selects: {                    // all fields optional; a rule matches on what it states
        source_kind, carrier_key_or_family, operation, observation_type,
        root_or_child, parent_operation,
        instrumentation_scope, instrumentation_version_range,
        decoded_shape_tags,       // closed vocabulary, emitted by the decoder
        observation_path_or_kind,
    },
    assigns: PartialClaim,        // any subset of the claim's fields
    refines: [profile_id],        // explicit precedence, the only kind there is
}
```

Matching, and why it is not first-match-wins by another name:

1. **Every** rule is evaluated. Registry iteration order has no semantic effect.
2. Assignments merge **field by field**. Identical assignments simply agree.
3. A conflicting assignment wins only if its rule names the other in `refines`.
4. Two unordered rules conflicting on a field produce a `ClaimConflict`: that field becomes
   `Unknown` and may not generate a hard identity, temporal or causal assertion.
5. Cross-field validity is checked after merging.

A property test permutes the registry and requires byte-identical claims — so order-sensitivity is
structurally impossible rather than merely discouraged. That is the difference from
`messages.rs`'s current `EXTRACTORS` list, whose semantics *are* its order.

Profiles must be partial: a semconv rule establishes direction and sequence for `gen_ai.*`, while
a scope-and-version rule adds `replay_stability` for one producer. Requiring every profile to fill
every field would force duplicated mega-profiles, which is how registries rot.

**Framework identity is not the correctness switch for standard carriers** — see "Framework
independence, stated so it is true" for the exact claim and its limits. Measured on the corpus:
`gen_ai.operation.name` is present on essentially every request file of adk, anthropic, bedrock,
langgraph, openai, openai-agents, strands and agent-framework; CrewAI is the outlier (5 of 33).
So selection keyed on operation, observation type and scope has a real basis, and a conforming
producer nobody has captured is covered by the semconv rules alone.

#### What a profile may read

The line matters, because "payload shape" is where the old heuristics would come back. Profiles
receive `DecodedShapeFacts` — a closed vocabulary from the decoder — and **never** `raw_payload`
or normalised content. Enforced by the API, not by review.

| Legitimate: structural protocol facts | Not legitimate: reconciliation facts |
| --- | --- |
| envelope or schema variant | text meaning, keywords |
| object member or array path (`messages` vs `raw`) | content equality or similarity to another observation |
| explicit role, content-block type | whether a prefix resembles earlier conversation |
| explicit ids and the field names carrying them | presence of tool results elsewhere in the trace |
| finish reason, response id, provider timestamp | message count as evidence of snapshot vs emission |
| producer-documented tags (Claude Code's `[TOOL RESULT: id]`) | arbitrary nested objects that look message-shaped |

`messages` versus `raw` is legitimate **only** inside a named dialect where `raw` is defined as the
answer — the code calls that "this file's own vocabulary", which is the right instinct and needs to
become a typed rule rather than a comment. Claude Code's bracket tags are legitimate for the same
reason: documented producer syntax, gated by that producer's scope.

The rule this excludes, by name: `history.rs`'s inference that a generation span containing tool
results means a full history replay. That is a fact about a trace, not about a carrier, and it must
not reappear inside a shape predicate.

#### `Unknown`, defined

Revision 1 listed `Unknown` and never said what stage 4 does with it, which made the enum
decorative. An `Unknown` observation:

- creates an **explicitly provisional** occurrence when nothing else evidences its content, so the
  content stays visible;
- contributes **no** production time, **no** cohesion, and **no** hard sequence edge;
- may be matched *onto* by a later restatement, and strengthened by later direct evidence;
- emits an unsupported-claim diagnostic naming the carrier and the span.

This is the cautious reading the current `carrier.rs` already takes for an unclassified carrier,
made explicit: it may under-report, which the answer invariant catches; it never over-reports,
which a user sees as duplicates.

#### Locally undecidable cases, and what to do instead of guessing

Some observations cannot be claimed from their own instance. Naming them is part of the design,
because the alternative is a guess that looks like a rule:

| Observation | Sometimes | Sometimes | Resolution |
| --- | --- | --- | --- |
| `gen_ai.assistant.message` on a generation span | a re-sent earlier answer (`_synthetic/resent_history`) | the span's actual output, for choiceless Logfire / OpenAI-Agents traces | Local only when a producer contract (scope + version) declares choiceless output, or the span carries an explicit completion marker. Otherwise `Unknown`. |
| `output.value` on a generation span | the new answer, as a scalar (`_synthetic/plain_output_value`) | a transcript *and* an answer (`answer_beside_the_conversation`) | Observation-level claims, per decoded path — which is why the claim moved off the instance |
| an unrecognised carrier holding message-shaped JSON | accumulated state | one emission | Genuinely undecidable. `Unknown`. |

The first row is a **known violation in today's code**: `feed/mod.rs:1953` resolves it by asking
whether *any* span in the trace emitted a `gen_ai.choice`. That is a trace-global question deciding
a carrier's meaning, so it breaks the locality this design requires, and migration step 3's
falsification test ("an observation's claim changes because of an unrelated span") is aimed squarely
at it.

The gate is trace-global for a stated reason, though, and the replacement has to cover it: its own
comment (`:1955`) records that in Strands the `gen_ai.choice` lives in a *parent or sibling* span, so
a span-local test would promote Strands' re-sent assistant messages to output. That is the shape a
producer contract must handle — Strands declares its scope, so the contract is available; what is not
available is the same answer for an *undeclared* producer, which is why that case stays `Unknown`
rather than being decided by a neighbour's carriers.

#### When a claim is wrong

Traced through, with the honest gaps marked, because "an invariant will catch it" was assumed
rather than checked in revision 1:

| Wrong claim | Consequence | Caught by | How loudly |
| --- | --- | --- | --- |
| `Creates` on a restatement | every copy becomes another occurrence, with false cohesion, edges and time priority | invariant 6, *only* if the metamorphic test adds and removes a semantically equivalent parent transcript — repeating an identical span is not enough | test failure; no runtime diagnostic |
| `Restates` on a creation, matching something | a genuine repeat disappears | **nothing today.** Multiplicity preservation is circular here: whether the carrier bears occurrences is the claim under test | silent |
| `Restates` on a creation, matching nothing | content survives as provisional, losing production time, cohesion and provenance | nothing; the answer invariant only catches a *wholly* missing answer | silent |
| wrong temporal authority | unconstrained ready nodes reorder; deterministically wrong | nothing; "deterministic total projection" guarantees deterministic wrongness | silent |

Two consequences for the migration. **Claims need their own conformance fixtures with
independently annotated expected claims** — message goldens cannot verify a claim, since a wrong
claim that happens to produce the right order is invisible in them, and a regenerated golden blesses
whatever it produced. And every claim needs a **decision ledger**: matching profile ids, predicates
that matched, assignments contributed, refinements applied, conflicts, final values, observations
affected. The ledger is what makes a wrong claim debuggable in six months instead of being
re-derived by bisecting the corpus.

### 4. Occurrence assembly

**An occurrence is a block, not a message.** The pipeline is already block-granular — one
`BlockEntry` per `ContentBlock`, and a message's thinking and text are deliberately two entries — and
a message-granular occurrence cannot express a copy that carries the thinking and one that does not.
So the unit is a block occurrence, and a message is a *cohesion group* over block occurrences.

**Identity is a witness, not content.** An occurrence's identity is an opaque token; its initial
value is its creation witness, `(observation locator, position within the observation)`. Content
hashes are *matching keys* — evidence that two witnesses may name one occurrence — and never
identities. Formally:

```text
Occurrence = equivalence class over { CreateWitness } ∪ { RestatementWitness }
```

Two **creation** witnesses may be united only by a reference with `kind: OccurrenceIdentity` and
`replay_stability: Stable`, or by a producer-declared same-emission relation. **Content equality is
not sufficient.** Absent such evidence they stay two occurrences, or one explicit ambiguity — never
silently merged.

That case is real and revision 2 could not express it: `order_graph.rs:622` already observes a
survivor claimed by both an inner generation emission and its parent's re-emission, and its comment
calls that the common shape. Revision 2 said only `Restates` observations are matched onto existing
nodes, so two direct witnesses *necessarily* produced two nodes. Where the producer is known, the
parent's re-emission is claimed `Restates` and the problem does not arise; where it is not, the
witnesses stay distinct and the ambiguity is reported. The one thing not permitted is uniting them
because their text matches.

**Matching is a global assignment, not a priority list walked greedily.** Revision 2's "by, in
order" conflated two different things — how much an evidence class proves, and the order a search
explores candidates in. They are separate, and the second must not affect the answer:

| Tier | Evidence | Force |
| --- | --- | --- |
| 1 | reference, `OccurrenceIdentity` + `Stable` | proof; dominates |
| 2 | producer-declared copy relation | proof |
| 3 | semantic fingerprint | candidate key only |
| 4 | position and partial-order compatibility | a *constraint* on the assignment, not an identity |

A reference that is `CorrelationOnly` or `Regenerated` may not be used as an identity — the rule
`dedup.rs` follows today by hand. **A call/result relationship is causality, not identity**, and
revision 2 was wrong to list it as a matching tier: it belongs in stage 7 as an edge. It may serve as
disambiguating *context* for a candidate, and it is identity evidence only when the id itself is
`OccurrenceIdentity`, which is tier 1 already.

The assignment must be **injective**, **order-independent** (invariant to the order observations are
iterated — stated set-wise, because reads are recomputed per request and nothing guarantees a stable
input order), and **backtracking**: greedy is demonstrably wrong here, which the existing replay
matcher already proves by needing to search when identical results could answer several calls.

**Injectivity is per restating carrier bundle, not global across the trace.** This is the one
under-specification that changes *membership* rather than order, so it is stated rather than left to
the implementer: one direct occurrence re-listed by two different parent snapshots must be matched by
**both**. Global target injectivity would let only the first snapshot claim it and turn the second
into a provisional occurrence — a duplicate in the user's view, and a violation of invariant 6, which
says adding a parent transcript changes nothing. Within *one* bundle the matching is injective, which
is what stops a snapshot's two identical entries collapsing onto one occurrence.

The objective is **maximal evidence authority first, then maximal cardinality** — never the reverse.
A larger set of tier-3 fingerprint matches must not outrank a smaller set of tier-1 reference matches,
because that would let content similarity override a provider's own identifiers. Where two assignments
are equally supported at the highest tier in play, the result is an **explicit ambiguity**, not a
choice, and an ambiguous match is preferred to a weak unmatched one only when the weaker option would
create an occurrence — visible duplication is worse than a flagged uncertainty.

**Provisional occurrences**, and the merge algebra revision 2 left as a phrase:

- a restatement matching nothing yields a provisional occurrence, so root-only telemetry stays
  useful, with **no** invented production time;
- when direct evidence later arrives, the provisional node's id and its whole observation lineage are
  **kept**; the creation witness joins its evidence set; edges from any separately built direct node
  are redirected onto the canonical node, self-edges dropped and duplicate edges coalesced; existing
  evidenced edges stay and newly justified cohesion and causal edges are appended; the production
  anchor is added *as evidence*, never overwriting evidence with a scalar;
- atomic groups, SCCs and the projection are then recomputed.

**The graph merge is monotonic; the returned list is not, and that is correct.** New direct evidence
can supply a production anchor, cohesion membership or a generation edge, and the topological
projection may legitimately change — the design says as much where it declines strong stable
insertion. What must be stable across a re-delivery is **membership and lineage**, not display
position. Stating it the other way round would forbid ever correcting an order with better evidence.

One case revision 2 could not answer, now explicit: if a provisional `"yes"` is followed by *two*
distinct `Creates("yes")` observations, neither strengthens it. An equally-supported match stays
ambiguous, because choosing one silently makes membership depend on arrival order.

This removes "history detection" as a concept: a parent transcript is evidence referring to earlier
occurrences, not a copy to be classified and filtered.

### 5. Session reconciliation

**One matching engine, two policies.** Stage 4 matches restatements within a trace and stage 5
matches replays across traces; both are constrained injective matching with backtracking and bounded
search, so building them twice guarantees they drift.

```text
left evidence  →  candidate edges tagged with authority
               →  injective assignment into a target relation
               →  subject to partial-order compatibility
               →  matches, unmatched, ambiguities, search-completeness
```

The policies differ, and only in these ways:

| | Stage 4 | Stage 5 |
| --- | --- | --- |
| target | same-trace creation and provisional nodes | prior-session occurrences |
| an unmatched item | creates a provisional node; matching continues | ends the replay prefix; everything after it is new content |
| result | an occurrence cluster | unions the local cluster with a prior-session one |
| extra constraint | — | inter-trace turn order |
| resource limit | — | must expose incompleteness (`replay_matching_complete`) |

**Stage 5 matches against `ReplayPrecedence`, not the presentation DAG.** Revision 2's "prior
occurrence DAG" was too broad. The two relations are already deliberately separate in the code
(`order_graph.rs:243`): the presentation graph contracts emissions and carries generation-dataflow
constraints, and neither is a valid statement about how a provider may serialise a history. Naming
one relation for both jobs is how that separation would be lost.

### 6. Representative selection

**Per block occurrence, never per copy.** Copies do not agree on which blocks exist — an enriched
copy carries thinking the raw one lacks, a snapshot may carry fewer blocks — so replacing a whole
message copy deletes content that only the non-chosen copy held.

```text
message cohesion group
  ├── text block occurrence      → { raw, enriched, execution, snapshot } → chosen rendering
  ├── thinking block occurrence  → { enriched }                          → chosen rendering
  └── tool-call block occurrence → { … }                                 → chosen rendering
```

So: an occurrence is neither the chosen copy nor a naïve union of payloads. It is a stable set of
block occurrences, each with its alternative renderings. Complementary blocks survive independently;
only *conflicting renderings of the same block* need a choice. **Absence is not deletion evidence** —
a snapshot that omits a thinking block is evidence about the blocks it contains and says nothing
about the ones it does not.

Invariant 8 forbids the choice moving anything; it did not forbid the choice *losing* something,
which is why this is spelled out rather than left to the invariant.

One scalar "quality" score must not answer both *what to display* and *where it happened*.

**The winner policy**, per block occurrence and in this order, because "choose the best rendering"
is not a specification: a rendering from a `Creates` witness outranks one from a `Restates` witness;
among those, one whose bundle carried more of the block's own detail (a media reference resolved, a
thinking block present) outranks a lossier encoding; a tie is broken by the witness locator, which is
total. The current scalar table's terms map onto this — but `NON_HISTORY` and `FROM_TOOL_SPAN` become
*witness* facts rather than score points, and nothing in the ordering path reads the result.

### 7. Presentation projection

Deterministic topological order over the occurrence DAG: contracted atomic groups, hard causal
edges, soft local sequence constraints, credible time as ready-node priority only, SCCs condensed and
reported.

**`order_graph` is reusable as algorithms, not as a module** — revision 3 overstated this. What is
reusable: sparse adjacency construction, union-find, the deterministic Kahn queue, the barrier
machinery. What must go: `collect_order_evidence` (`order_graph.rs:105`) derives its facts from
`BlockEntry` — carrier semantics, history classification, output direction, generation-span
classification, effective timestamps — and every one of those inputs is a thing this design removes.
And `resolve` *constructs* the graph as well as resolving it: contraction, anchors, call/result edges,
carrier sequence edges, generation dataflow edges. The target signature carries no `BlockEntry`, no
carrier table, no history flag and no lineage inference:

```rust
resolve(nodes, contracted_groups, hard_edges, soft_edges, ready_priorities) -> Order | Degraded
```

**And SCC condensation does not exist today.** Revision 1 listed it among what survives; in fact a
cycle releases the smallest remaining node, and the code's own comment (`order_graph.rs:999`) records
condensation as future work. It is new work, not preserved work — which matters, because a cycle is
real evidence of contradiction and the current behaviour reports nothing.

Span views are **observation** views — "what this span carried". Trace, session and feed views are
**occurrence** projections. Different questions; no shared hidden history filter.

## Invariants

Framework-independent, and each testable on its own:

1. **Evidence accounting** — every carrier instance is decoded, retained as opaque, or
   produces a diagnostic. Nothing disappears silently.
2. **Decoder locality** — decoding one carrier depends only on its payload and dialect.
3. **No unsupported assertion** — every occurrence, multiplicity decision, causal edge and
   anchor names the evidence justifying it.
4. **Representation independence** — re-encoding or enriching an occurrence does not create
   another one.
5. **Multiplicity preservation** — two positions in a `Creates` observation are two occurrences
   even with identical content; positions in a `Restates` observation prove nothing. Note this is
   a *consequence* of the claim, not an independent check on it: it cannot detect a wrong claim,
   only an assembler that ignores a right one.
6. **Restatement idempotence** — adding a parent transcript, a duplicate span or another replay
   changes neither membership nor causal order. The test must add a **semantically equivalent
   transcript**, not a byte-identical duplicate span: the existing duplicate-span property test
   passes trivially and does not exercise this.
6a. **Claim conformance** — a claim fixture states the expected claim per observation,
   independently of the message goldens, because a wrong claim that yields the right order is
   invisible in a golden and a regenerated golden blesses it.
6b. **Profile permutation** — permuting the profile registry yields byte-identical claims.
7. **Replay invariance** — any linear extension of a prior partial order may be re-sent as a
   snapshot without creating occurrences; matching is injective.
8. **Authority separation** — choosing a better display representation cannot change identity,
   placement, anchors or order.
8a. **Representation completeness** — the block-occurrence set is independent of which rendering
   wins. Forcing every alternative to win in turn must not remove a block. Invariant 8 forbids
   *movement* and is silent about *loss*, which is a different failure.
9. **Witnessed causality** — only local, evidenced relations become hard edges: atomic-emission
   member order and cohesion; an unambiguous result after its call; matched generation inputs
   before that generation's output.
10. **Presentation independence** — changing presentation constraints cannot change the
    occurrence multiset or the session reconciliation result.
11. **Deterministic total projection** — acyclic evidence yields a deterministic linear
    extension; contradictory evidence is localised to an SCC and reported.
12. **Honest ambiguity** — indistinguishable identical plain messages stay explicitly ambiguous.
    A rendering policy may collapse them; the model must not call that proven identity.

Deliberately *not* an invariant: "stable insertion" in its strong form. New evidence may
legitimately add a causal edge and correct an earlier order. Stability applies to redundant and
restating evidence only.

### The invariant set audited: what it actually checks

The list above claims each invariant is "framework-independent and testable on its own". Audited,
that is not true of all of them, and the honest accounting matters more than the count.

**They are not independent.** Six pairs overlap, and a derived invariant is a second place to encode
one rule — the exact objection that removed `multiplicity_authority` from the claim:

| Pair | The dependency |
| --- | --- |
| 3 ↔ 9 | an unevidenced hard edge violates both; 9's genuine content is its *locality whitelist*, not "must be evidenced" |
| 3 ↔ 12 | calling two indistinguishable messages one proven occurrence is already an unsupported multiplicity decision |
| 3 ↔ 5 | merging two `Creates` positions with no identity evidence violates both; 5 is the assembler-level test of 3 |
| 5 ↔ 12 | same collapse, stated twice |
| 4 ↔ 6 | when the added representation is a transcript, duplicate span or replay, 6 is the strictly stronger form |
| 6 ↔ 7 | 7's *membership* clause is a special case of 6; its independent content is injectivity |

And two entries are not invariants at all: **6a** (claim conformance) is an oracle requirement — it
says the output matches hand-authored annotations, not that the annotations are true — and **6b**
(profile permutation) is a determinism property of the registry. Both belong in the verification
section, not the invariant list.

The genuinely independent dimensions are six: input accounting and locality (1-2); the claim oracle
and registry determinism (6a-6b); occurrence non-contraction and idempotence (4-7, 12, which should be
merged); representation versus placement (8, 8a); presentation versus reconciliation (10); graph
validity and projection (9, 11).

**Most have no test today, and the generator cannot produce the shapes.** `feed/props.rs` generates
single-message rows with one fixed tool id — no bundles, no claims, no multiple positions, no replays,
branches, ambiguities or representation alternatives. So:

| Test exists | Invariants |
| --- | --- |
| none — statements of intent | 1, 2, the semantic half of 3, 4, 6a, 6b, 7, 8a, 12 |
| a weaker approximation only | 5, 6, 8, 9, 11 |
| genuinely covered | 10 (single-trace and session ordering-independence), though it compares rendered content rather than occurrence identity |

Two specific gaps worth naming because they read as covered and are not:
`carrier_semantics_are_declared` only sees carriers that already produced classified blocks, so a
**wholly dropped** carrier is invisible to it — invariant 1's test does not exist. And
`reading_more_carriers_only_adds_messages` observes rendered output, not decoder output, so it is not a
test of decoder locality.

### Several kept invariants would falsely accuse

The document has a table of rejected invariants; the kept ones need the same audit.

| Invariant | Would fire on | Correction |
| --- | --- | --- |
| 6, "another replay changes neither membership nor causal order" | a genuinely retried identical call, a branch-local or subagent occurrence — if equivalence is decided from *content* | restrict to a replay **proven** to restate the same occurrence set |
| 6, again | `Restates { sequence: Ordered }` — adding previously absent ordered evidence may legitimately refine order, which 6 forbids | membership is invariant; order may be refined by new *ordered* evidence |
| 7, "any linear extension" | a topological extension interleaving parallel branches in a sequence no producer can serialise | quantify over producer-valid `ReplayPrecedence` serialisations within one branch scope |
| 9, "an unambiguous result after its call" | parallel calls, cancelled and error turns with unanswered calls, subagent results whose call was never exported | "unambiguous" must require a *matched call occurrence*, not a reused id |
| 11, deterministic total projection | the project feed, which is deliberately newest-first across responses, so a result legitimately precedes its earlier call | applies to the stage-7 chronological projection, **not** to the feed endpoint |
| 12, identical plain messages "stay ambiguous" | two identical positions in one `Creates` observation, parallel branches, retried calls — all provably distinct | the ambiguity is in *matching a restatement* to them, never in whether two creations exist |

One thing the list also **dropped** and should not have: a completed turn retains an answer. The
existing `assert_has_an_answer` is what caught three of the defects in this document. It was omitted
because `strands/error` legitimately has none — but the right condition is *"a turn with explicit
successful completion evidence has an answer"*, not "every fixture except the error one".

### False equivalence is still not detectable, and this is the load-bearing admission

The failure the system most fears is two genuine occurrences collapsed into one. It is invisible in
goldens (the golden records the collapse as correct) and `assert_no_duplicates` may actively *enforce*
it. Checked against the list: invariant 5 only works after an observation was correctly labelled
`Creates` (the circularity the invariant itself concedes); invariant 3 passes if the union cites a
*wrong* `Restates` claim as its evidence; invariant 6 positively requires restatements to collapse;
invariant 12 passes if the wrong claim is taken to make the messages distinguishable.

The missing invariant, which should be added:

> **Distinct creation witnesses are never contracted** unless independently verifiable
> occurrence-identity evidence, or an independently annotated producer copy relation, connects them.

Its property test discriminates five cases: the same witness locator redelivered (count unchanged); a
fresh witness locator with identical content (count +1); parallel branches with identical content (two
occurrences); a retried identical call with fresh creation evidence (two occurrences); a stable
reference or declared same-emission relation (contraction permitted).

That catches an **assembler** that ignores correct claims. It cannot catch a **profile** that labels a
creation `Restates`, unless the test has ground truth from outside the telemetry. For arbitrary
production telemetry carrying no stable occurrence marker, that ground truth **is not constructible** —
inferring it from identical content is the heuristic this design exists to remove. Synthetic fixtures
and out-of-band capture annotations can supply it; the normaliser cannot. That is an information limit,
not an implementation gap.

### Is this design falsifiable? Partly — and the honest answer matters

- **Mechanical errors: yes.** Presentation-reconciliation coupling fails
  `ordering_constraints_do_not_change_a_session_s_messages`; exact redelivery fails
  `redundant_evidence_does_not_change_the_answer`; collapsing CrewAI's genuine repeat fails
  `repeated_identical_calls_keep_both_and_stay_resolvable`.
- **The central semantic claim: no.** A subtly wrong `Restates` that collapses a genuine id-less repeat
  has no test that must fail. A claim-conformance fixture fails only if that exact observation was
  independently annotated.

So for a corpus case whose count changes, `message_goldens` produces a diff and a human decides. For a
collapse already blessed by a golden, or a shape absent from the corpus, **nothing fails** — and "no
duplicates" may certify the error. The design is therefore an improvement in *structure and
attribution*, and it does not, on its own, replace "the goldens change and a human reviews the diff"
as the guarantee for the one distinction everything rests on. Anything stronger requires per-occurrence
ground truth that is added deliberately, fixture by fixture.

## The verification apparatus, specified

Review 7 said what was missing; this is what to write. The governing decision: **do not extend the
current row fuzzer.** `feed/props.rs` generates isolated persisted rows holding one message, with no
hierarchy, bundles, copies or branch structure — it cannot reach a single shape this model needs.

### A scenario generator that carries its own ground truth

The direction of derivation is what makes it non-circular:

```text
SourceProgram ──→ TruthGraph          (the oracle; never given to the implementation)
       └───────→ producer encoder ──→ ExportTraceServiceRequest   (the real input)
```

Never generate OTLP and infer truth from it. The program's own actions are the truth:

```rust
program.emit_new(...)                              // a new TruthOccurrenceId
program.redeliver(witness)                         // same witness, same id
program.restate(targets)                           // no new id
program.retry(...)                                 // a new id
program.alias(a, b, StableOccurrenceIdentity)      // one id, two witnesses
```

Shapes it must reach: carrier bundles with several physical members; several observations at distinct
positions including byte-identical content; exact re-delivery of one locator; fresh locators with
identical content; parent snapshots with changed encoding and regenerated ids; cross-trace replay;
parallel branches with identical outputs; retries as distinct attempts; an ambiguous restatement
compatible with two identical creations; complementary and conflicting renderings; cancelled and error
turns; and the mixed-authority carrier.

What it must **not** generate, because each would manufacture a false failure: bundles inferred from a
shared name prefix (membership comes from the producer encoder); private carriers on span types that
producer never emits; claim combinations the sum type forbids; one witness locator with two payloads
(that is replacement, not re-delivery); stable occurrence authority on a regenerated id; *global*
replay injectivity; arbitrary topological interleavings of parallel branches (only producer-valid
serialisations); a successful completion with no answer; a cancelled turn that also carries a success
marker.

**Shrink the `SourceProgram` and re-render it — never shrink raw OTLP.** Each case carries a contract
naming the shape to preserve, the witness pairs that must stay distinct, and the identity edges that
must survive, so a shrink cannot turn "fresh witness" into "exact re-delivery" and call the property
satisfied.

### The contraction test, and what is buildable today

The assertion is over **occurrence equivalence classes**, not rendered message counts:
`creation_partition(&assembly) -> BTreeSet<BTreeSet<CreateWitness>>`.

| Case | Expected partition | Buildable now? |
| --- | --- | --- |
| exact re-delivery of one locator | one class, one witness | **yes** — the corpus test already doubles rows |
| two locators, identical content, no identity evidence | two singletons | **yes** for atomic-emission carriers |
| parallel branches, identical content | two singletons | input constructible, but rendered count cannot distinguish "contracted" from "a witness was dropped" — weak until evidence accounting exists |
| retry as two attempts | two occurrences, each result attached to its own call | **yes** as a rendered-multiplicity gate (`crewai/mcp_tools` already serves this) |
| same stable `OccurrenceIdentity`, or a declared same-emission edge | one class, two witnesses | needs the new model — there is no `ReferenceAuthority` today, so a collapse cannot be attributed to the intended evidence |

One correction to revision 7: in the last case contraction must be **required**, not merely permitted,
or an assembler that ignores stable identity passes.

### Metamorphic tests, as transformations

| Invariant | Transformation | Exact | May change |
| --- | --- | --- | --- |
| 2 decoder locality | decode the target bundle alone, then add/remove/reorder unrelated bundles and spans | the target bundle's observations: positions, content, ids, shape tags | observations and diagnostics for the added bundles |
| 4 representation independence | substitute a producer-declared equivalent encoding that adds no temporal or order evidence | the creation-witness partition and occurrence count | the chosen rendering and its provenance |
| 6 restatement idempotence | add a proven parent transcript, duplicate span or replay, with changed encoding and regenerated ids | creation membership; **no** production anchor or hard edge may appear from a restatement | lineage gains witnesses; an `Ordered` restatement may refine order, an `Unordered` one may not; exact re-delivery stays byte-identical |
| 7 replay invariance | emit a **producer-valid** serialisation of a prior `ReplayPrecedence` as a later snapshot, over several serialisations | no occurrence created for the matched prefix; matching injective *within* the bundle; several bundles may each match one prior occurrence | ordered replay may refine soft sequence; a suffix after the first unmatched item may create occurrences |
| 8 authority separation | hold the graph fixed, force each rendering to win via a test-only override | partition, placement, anchors, edges, matches, final order | displayed content and provenance only |
| 8a representation completeness | force every per-block alternative to win in turn | the block-occurrence id set and cohesion membership | the rendering of the selected block |
| 10 presentation independence | reconcile once, project under neutral / production / pairwise / perturbed constraints | occurrence multiset, partition, matches, unmatched set, ambiguities, representative choice | linear order and presentation-only diagnostics |

Note what this corrects: `reading_more_carriers_only_adds_messages` is **not** a test of decoder
locality — it compares rendered output after the whole pipeline. Locality has to compare decoder output
before claims, dedup or rendering.

### Mutation controls, named per seam

A property that only passes the current implementation is not a gate. Each of these must fail:

| Mutation | Must fail |
| --- | --- |
| contract equal content | contraction cases 2-4 |
| treat every creation as `Restates` | the claim oracle |
| make replay injectivity global | two independent parent snapshots |
| choose a representative per copy | invariant 8a |
| reconcile from the presented order | invariant 10 |

### Ordering: what must exist before what

The dependency is blunt — **a test that cannot fail today is worthless as a gate on tomorrow's
change.**

1. the scenario generator, truth graph, producer encoders and model-aware shrinker;
2. stage-level observables: carrier accounting, claims plus ledger, the assembly witness partition,
   reconciliation matches and ambiguities, the projection graph;
3. exemptions replaced by source-proven limitations;
4. current-pipeline gates for re-delivery, identical positions and retries, each with its mutation
   control.

Then, per mechanism: profiles need decoder locality, evidence accounting, the claim oracle and
permutation invariance **first**, or profile work recreates the unfalsifiable switch; the assembler
needs the five-case suite plus 4 and 6 against a shadow implementation, because promoting one whose only
observable is rendered count cannot be judged; reconciliation needs producer-valid replay generation and
per-bundle injectivity; representative selection needs 8 and 8a with the winner override; the resolver
comes last, with membership already frozen. Goldens are regenerated last of all.

## Can the central distinction be falsified without hand annotation?

For arbitrary existing id-less telemetry, **no** — and four tempting substitutes each fail for a
different reason:

| Candidate | Why it is not sufficient |
| --- | --- |
| a second independent implementation | agreement can mean both share the same wrong profile rule |
| self-consistency across the four views | a consistent false contraction appears consistently in every view |
| cross-framework agreement | identifies an outlier without establishing which framework is right — as the system-instruction case shows, the *minority* was correct there, and only reasoning about spans settled it |
| mutation testing | proves the apparatus detects specified mistakes, not that an unannotated production claim is true |

**But there is a construction that works, and it needs no hand-authored annotations**: controlled
capture with a machine-generated manifest. Drive each SDK from a generated `SourceProgram` against
deterministic fake models and tools, and record an out-of-band manifest mapping each application action
and its live trace/span context to a `TruthOccurrenceId`. The harness knows whether it emitted, retried,
re-delivered, or merely let the SDK re-send history — so the ground truth is independent of any
interpretation of the telemetry, while the telemetry itself is real output from a real framework
encoder.

That is the strongest available answer, and it is bounded: it says nothing about historical or
third-party telemetry. For that open-world case review 7's conclusion stands — a wrong `Restates` can be
internally consistent and observationally indistinguishable from the truth.

### Exemptions, and the rule that keeps one honest

Exemptions are how an invariant rots into a formality, and three existing ones are too broad:
`PAIRING_EXEMPT` skips a whole fixture on its label; `NO_ANSWER_EXPECTED` skips every view of
`strands/error`; `KNOWN_DEFAULTED` lists carriers with no machine-checkable reason.

The rule:

> A legitimate exemption **proves that the invariant's antecedent is false in the source telemetry**. A
> suppressed failure merely records that the implementation cannot currently satisfy it.

The machinery, so the rule is enforced rather than aspired to: a typed `ExemptionSpec` carrying the
invariant, a producer selector *including scope and version range*, the fixture, a locator pattern built
from stable labels and canonical ordinals (never generated ids), the exact expected finding with its
witnesses and count, a `SourcePredicate` and the diagnostic the user must see. The predicate evaluator
reads the raw `ExportTraceServiceRequest` **before** normalisation and may not call the decoder or look
at rendered output — otherwise a broken decoder can "prove" that the evidence it failed to read was
absent.

Two consequences worth taking: `strands/error` should stop being an exemption at all, because the answer
invariant's antecedent is genuinely false there (no successful completion) and that predicate can be
asserted *positively*; and `KNOWN_DEFAULTED` carriers should either be declared or retained opaquely
with a diagnostic, rather than living in a silent allowlist.

So an exemption is an *exact expected limitation*, not a skipped assertion: it names the invariant, the
producer and version, the exact fixture/view/span/carrier locator, the expected violating witnesses and
their count, a source-level predicate showing the required evidence is absent, and the diagnostic the
user must see. And it fails if an extra violation appears, if the expected violation **disappears**, if
the raw source starts carrying the missing evidence, or if the diagnostic is gone.
`REORDERS_UNDER_PER_CARRIER` already does the bidirectional half correctly — every listed entry must
still occur, and a fixed one must be removed. The pairing and answer exemptions need the same.

## One fact, one job

Revision 1's whole diagnosis was that `batch_time` did three jobs and one quality score did two. The
same shape is easy to reintroduce in a *design*, so the separations are listed rather than assumed.
Each row was a conflation present in an earlier revision of this document:

| Fact | Its one job | What it must not also decide |
| --- | --- | --- |
| a call/result reference | a causal edge (stage 7) | occurrence identity (stage 4) — unless the id is itself `OccurrenceIdentity` |
| `ReplayPrecedence` | what a provider may legally have serialised | presentation order |
| `PresentationConstraints` | the order shown | replay admissibility |
| a provisional occurrence | keeping unwitnessed content visible | being a silent identity target for the first arriving witness |
| a chosen rendering | how a block is displayed | which blocks exist |
| evidence authority (the tier table) | how much a class of evidence proves | the order a search explores candidates |
| credible time | ready-node priority | a causal edge |

## The operational contract

Revisions 1-4 were semantics with no operational content, and one of them is a real hazard: **stage 4
as specified is factorial in the worst case.** A bundle with `m` restatements each having `n`
compatible targets admits `n!/(n-m)!` injective assignments, and provisional branches enlarge the tree
further. Partial-order compatibility couples the choices, so this is not edge filtering that prunes
itself.

It bites on exactly one shape: many observations sharing a fingerprint, no stable occurrence ids, and
order constraints that disambiguate them only late — repeated `"yes"`, several tools answering `"ok"`,
parallel or retried identical calls. That shape is not hypothetical; it is why the existing
cross-trace matcher needed a budget at all (`feed/mod.rs:404`: nine interchangeable calls have `9!`
orderings of one set).

The pipeline is linear in its input today (~27 MB/s, measured). **The obligation is not to add a
second, algorithmic source of superlinearity**, since the existing quadratic growth is in the
telemetry a replaying framework emits and no amount of normaliser work touches it.

### The matching budget

Revision 4 said stages 4 and 5 share one bounded engine and then gave stage 4 no bound, which is a
contradiction. The contract, mirroring the precedent already in the code
(`MATCH_BUDGET = 20_000`, `feed/mod.rs:353`):

1. forced tier-1 and tier-2 unions, and deterministic singleton matches, happen **without search**;
2. at most 20 000 speculative candidate-assignment expansions per reconstruction;
3. ambiguity components are processed in canonical **witness-locator** order — never hash-map or row
   order, or two replicas disagree;
4. on exhaustion: **keep proven matches, discard unproven weak assignments** in the unresolved
   components, and emit those observations as provisional occurrences and explicit ambiguities. This
   deliberately **under-strips** — the standing rule that duplicates beat deletion;
5. deterministic non-search work continues; the answer is never refused wholesale;
6. the caller is told: `reconciliation_complete: false`, with stage, budget, attempts and the count of
   unresolved observations.

Returning a best-so-far weak assignment is **not** an option: without proof of optimality it can merge
a genuine repeat, which deletes content.

The graph merge must be **batched** — unions collected, then the graph built once (`O((V+E) log V)`).
Recomputing groups, SCCs and the projection per merge is `O(M(V+E))`, and the existing resolver
already needed barriers and ordered ready sets to remove quadratic behaviour.

Whether the model preserves input-linearity on the normal path is **not knowable from this document**,
and that is a gap rather than an omission: there is no candidate-count measurement and no stage-4
benchmark. One is required before any promotion, alongside `bench_session_scaling`.

### Statelessness, stated properly

Reads are **stateless recomputations from rows**, so revision 4's "the provisional node's id is kept"
was written as an incremental mutation of something that does not persist. Two replicas would pick
different ids. The rule has to be **set-wise**: an occurrence retains every witness id as an alias,
and its primary id is chosen by stable witness-locator order. Then any replica, warm or cold, derives
the same identity from the same rows.

And **evidence is not monotonic across reads.** Revision 4 called the graph merge monotonic; that is
true within one reconstruction and false between two. ClickHouse's `ReplacingMergeTree` can have
merged away the earlier version of a re-delivered span, so a later read may see *less* evidence, and
it cannot offer DuckDB's as-of-watermark cut. So the merge algebra must be a **pure function of the
rows now present**, never an assumption that evidence only accumulates.

What the caller sees, because none of this may be silent:

```text
provisional_occurrences     count
ambiguous_occurrences       count
reconciliation_complete     bool
input_snapshot              exact_as_of | best_effort   // per analytics backend
```

### "Self-scaling" is the wrong goal for this layer

Autoscaling belongs to the serving layer: it raises aggregate throughput and cannot reduce one 68 MB
reconstruction's latency, nor rescue a factorial search. The properties actually wanted here, each
checkable:

- stateless and horizontally deployable;
- **deterministic across replicas**, including under budget exhaustion;
- linear in input on the normal path;
- hard-bounded speculative search;
- memory bounded by observations plus sparse candidate and graph edges;
- conservative, caller-visible degradation.

One consequence worth naming: scaling out multiplies *cold* caches, so horizontal scaling increases
duplicate reconstruction work. The cache is performance state, not semantic state, so this costs
latency and never correctness — and a warm cache and a cold one must produce byte-identical answers,
which `a_cached_reconstruction_equals_a_fresh_one` already checks.

## Framework independence, stated so it is true

Earlier revisions claimed "framework identity is optional metadata, not the correctness switch" and
"a conforming producer nobody has captured works automatically". The second is false and the first is
half true. The claim that survives review:

> **Framework identity is not required to decode and conservatively render supported OpenTelemetry
> GenAI semantic-convention carriers. Producer scope and version remain correctness inputs for
> private dialects, and for standard carriers whose occurrence semantics are locally ambiguous.**

Why the weaker form is the honest one: when a rule decides that `gen_ai.assistant.message` is output
rather than replay *because of the scope*, that scope is functioning as producer detection. The design
requires exactly such a contract for Strands and for choiceless Logfire/OpenAI-Agents telemetry. The
dependency is **narrowed, versioned and made explicit** — not removed.

Selecting on instrumentation scope is still materially better than detecting a framework:

- it is per instrumentation library and **per span**, where `sideseat.framework` describes a whole
  process and mislabels spans emitted by a nested library;
- it is **versioned**, so a contract can be bounded to the versions it was observed on;
- generic semconv rules match on carrier family, operation and observation type and never consult it,
  so scope refines rather than routes.

### What the redesign can and cannot shrink

Counted over today's code, so the payoff is a number rather than a feeling:

| | Count | Can the redesign remove it? |
| --- | --- | --- |
| extractor families in `messages.rs` | 16 | — |
| …of those, producer-independent (indexed `gen_ai.*`, current semconv messages, generic `input`/`output`) | 3 | already |
| …genuine dialect decoders (OpenInference, Logfire, Vercel, ADK, LiveKit, MLflow, Traceloop, PydanticAI, LangSmith, LangChain/LangGraph, AutoGen, CrewAI, Claude Code) | 13 | **no** — the producer chose a private encoding |
| private attribute-recovery families in `attributes.rs` (token names, usage JSON, request/response JSON, invocation parameters) | ~10 | **no** |
| framework-recognition rules | 28 | they become metadata, not routing |

So roughly **23 irreducible dialect adapters** survive as decoders. What the redesign removes is
framework-specific *policy*, and there are six such couplings, each currently a place a fix must be
made twice:

1. extractor ownership and registration order;
2. suppressing the generic reader when any dialect extractor matched (measured above — the third
   ripple);
3. the semconv-only tool-span gate plus a separate Vercel bypass;
4. OpenInference cross-carrier enrichment and its representation choice;
5. Logfire suppressing `request_data` on the strength of another carrier;
6. Claude Code duplicate suppression inside decoding.

**That list is the deliverable.** It is what "less fragile" means concretely: six decisions that today
depend on a framework's identity or on another carrier's presence, replaced by per-observation claims.

### The honest limit

No system can interpret every future private dialect. If two producers emit identical OTLP meaning
different things, the information is absent. Coverage says less than it looks: the support matrix
recognises 32 frameworks and 11 have captures, so "correct for all frameworks" is an open-world claim
under either architecture.

Two cases make the boundary concrete:

- a **new producer emitting correct semconv** under an unknown scope gets the generic claims and is
  correct wherever the carrier's meaning is locally determined; it stays `Unknown` — conservative,
  not correct — for an ambiguous carrier like `gen_ai.assistant.message`;
- the same producer that *also* puts its real final answer in a private member (the CrewAI `raw`
  shape, which is real) does **not** work automatically: profiles may not inspect arbitrary payload,
  and `raw` means "answer" only inside a named dialect. The answer stays opaque.

The achievable contract:

- a conforming producer nobody has captured is **decoded and conservatively rendered** without a
  framework label;
- an unknown private carrier degrades conservatively **and visibly**;
- onboarding a dialect adds local claims without changing established semantics.

### Absorbing the next semconv revision

Semconv is unstable and has already changed. The design is **better at semantic change and no better
at alias churn**, which is worth knowing before calling it forward-compatible:

| Change | Places to touch |
| --- | --- |
| a rename or alias | carrier-family declaration + a decoder alias — two, and a family-keyed profile is untouched. Today a `get_first` chain absorbs it in one place, so this is a small regression |
| a new shape or transport (events becoming attributes, as already happened) | bundling + decoder + profile assignment — three, but each is local and named |
| a change of *meaning* | profile rules and their conformance fixtures — which is the point: today a meaning change is spread across event recognition, attribute extraction, carrier semantics, normalisation and the input/output source lists |

The missing piece is **one canonical versioned semconv compatibility module**, so alias churn stays a
single-place edit. Not yet specified, and it should be.

## What survives, and what goes

**Survives**: `PositionPath` and carrier-instance provenance; the content normalisers; content
and binary fingerprints (as matching tools); operation type, span hierarchy, raw timestamps;
exact call/result references; observation→occurrence lineage; injective replay matching;
atomic-emission contraction; generation input→output dataflow; deterministic topological
projection; separate display and ordering timestamps; every diagnostic.

Not on that list, though revision 1 put it there: **SCC condensation**, which does not exist yet.

**Goes**: the name-keyed `semantics_for`; extractor first-match/claiming and priority order;
`is_input_source`/`is_output_source` prefix lists; `promoted_to_span_output`; the whole
multi-phase `history.rs`; content-based `MessageIdentity` as occurrence identity;
`call_repeat_ordinals`; scalar quality as a membership or placement mechanism; birth-time maps,
`batch_key`, `batch_times`, `feed_positions` and the legacy tuple sort; id-less correlation as a
hard identity mutation.

`order_graph`'s *algorithms* stay — contraction, sparse edges, deterministic Kahn, causal
precedence. Its evidence collection and its API go, and SCC condensation is new work rather than
preserved work (see stage 7). The resolver is already live in production with nearly every constraint
class enabled, so it can and does linearise the wrong graph perfectly.

## Should this be built? — the case against, and the recommendation

Stated here because a design document that only argues for itself is not evidence. The case
**against** the rewrite is strong, and most of it comes from this document's own measurements:

- the motivating ordering defect **does not reproduce** — both the committed Vercel golden and the
  purpose-built pure-semconv collision fixture are correctly ordered;
- the ripple evidence proves the layers are *coupled*; it does not prove this replacement model is
  right;
- the resolver already runs in production with every constraint class promoted;
- the rewrite adds a profile language, an authority lattice, a global matcher, a graph merge algebra,
  a persisted-format change and a nine-step migration;
- three of the four wrong-claim failure modes are, by this document's own table, caught by nothing —
  so the rewrite would ship with less detection than the thing it replaces until the annotated claim
  fixtures exist;
- the corpus is narrower than 119 fixtures suggests: 11 of 32 recognised frameworks have captured
  coverage, so "correct for all frameworks" is an open-world claim either way.

**The recommendation is therefore not to rewrite, but to adopt this model incrementally**, in an order
where each step is worth doing even if the next never happens:

| # | Step | Worth doing alone because |
| --- | --- | --- |
| 1 | Capture and persist carrier provenance, event ordinals and **instrumentation scope** | scope is not recorded at all today; nothing downstream can be scope-aware until it is, and it is a pure addition |
| 2 | Refactor `order_graph` into a pure resolver (`nodes, groups, edges, priorities`), with real SCC condensation and a degradation signal | a cycle currently releases the smallest node and reports nothing |
| 3 | Claim ledger and annotated claim fixtures, in shadow mode | makes the three silent failure modes detectable, whatever decides the claims |
| 4 | Move cross-carrier authority out of decoders (OpenInference enrichment, Claude Code suppression) | those are stage-4 and stage-6 decisions sitting in stage 2, and each is a place a fix has to be made twice |
| 5 | Only then replace dedup and reconciliation — and only against a **reproducible** membership or ordering failure | the one case that motivated the rewrite was not reproducible; the next should be pinned by a fixture before any mechanism changes |

**Landed already, and it changes the estimate.** The first carrier-*instance* claim is in
(`1e623a45`): the generic answer carrier is admitted only on a **generation** span, decided by
`detect_observation_type`, which is a pure function of the span name and attributes and is already
computed before message extraction runs. Corpus-neutral, mutation-verified both ways — disabling it
loses the answer on `_synthetic/dialect_question_generic_answer`, and dropping the observation-type
guard reproduces the langgraph 12 → 28 expansion.

The lesson for step 1: **span context is available at ingest for free.** So the persistence work is
needed for *scope* and *raw carrier payloads* — the facts nothing derives — and not for span context,
which narrows step 1 considerably and means claims keyed on operation and observation type can be built
before any schema change.

The target model below keeps its value as vocabulary and as the destination. What it does not have yet
is a defect that only it can fix.

## Migration, if the full model is built

Each step verifiable against the 119-fixture corpus on its own.

| # | Step | Verification | Evidence it is wrong |
| --- | --- | --- | --- |
| 1 | Canonical fixtures: same key on generation vs agent, root-only snapshot, undeclared conforming producer, unknown carrier, **mixed-authority carrier** (`answer_beside_the_conversation` already is one) | New invariants fail on the old path for the intended reasons | A fixture depends on a framework label, or does not reproduce the collision |
| 2 | `CarrierInstance`, `Observation`, provenance ledger beside the current blocks | Byte-identical goldens; every block traces to observations; every carrier decoded, opaque or diagnosed | Any output delta, or an unaccounted carrier |
| 3 | Profile registry and observation claims, shadow mode, with the decision ledger and annotated claim fixtures | Corpus-wide claim report; registry permutation is byte-identical; Vercel root and `chat` get different claims without framework identity; the mixed carrier gets two different claims | An observation's claim changes because of an unrelated span, semconv needs `sideseat.framework`, or a claim's provenance cannot be printed |
| 4 | Shadow occurrence assembler | Occurrence multiset vs current output; duplicate spans and root restatements idempotent; direct-emission repeats survive | A snapshot creates duplicates, direct repeats collapse, or membership changes are unexplained |
| 5 | Membership from occurrences, legacy presentation retained | Count and content invariants, answer and pairing checks, reviewed delta list | Missing answers, new unmatched results, unexplained ambiguity growth |
| 6 | Cross-trace reconciliation against the occurrence DAG | Every valid linear extension strips fully; presentation settings preserve membership | Ordering options change membership, or matching over-strips a genuine repeat |
| 7 | Representative selection separated from placement | Force every alternative representative to win; multiset and order unchanged | Any tie flip moves or deletes an occurrence |
| 8 | Occurrences into the resolver; promote edge classes one at a time | Every movement names its edge; the Vercel case becomes calls → results → final | The target stays wrong, unrelated movement has no edge, or time acts as causality |
| 9 | Switch each view, dual-run, then delete the old heuristics | Zero unreviewed deltas; unknown-carrier tests stay conservative | A removed heuristic changes membership — the new model did not replace it |

**The rule for goldens**: do not regenerate until a delta is attributable to a named claim or
causal edge. "The corpus changed by 22 fixtures" is not evidence of correctness; "these three
occurrences moved because these generation-dataflow edges became available" is.

## Revision log

Each revision is a design review that found something the previous one had wrong. Kept so the
reasoning is auditable rather than re-derived.

| Rev | Focus | What it changed | Refuted by |
| --- | --- | --- | --- |
| 1 | overall decomposition | first written form: seven stages, instance-level claims, 12 invariants | — |
| 2 | the claim model (stage 3) | **claims moved from the carrier instance to the observation** — one `output.value` holds a restated transcript and a new answer, so an instance-level claim is a cardinality error; authority became a sum type (dropping `multiplicity_authority` as derived, and excluding `Restates + SpanCompletion`, which claims nothing); added `ReferenceAuthority`, because "unambiguous stable reference" was the next hidden heuristic; replaced "predicates over the instance" with a closed declarative rule language, field-wise merging and explicit `refines` precedence; drew the line on what a profile may read; defined `Unknown`'s semantics; named the locally undecidable cases instead of guessing at them; replaced the assumed failure-mode coverage with a measured one — **three of four wrong-claim modes are caught by nothing today**, which is what forces annotated claim fixtures and a decision ledger | `_synthetic/answer_beside_the_conversation`; `dedup.rs`'s two contradictory uses of a call id; `feed/mod.rs`'s trace-global choiceless gate |
| 3 | assembly, reconciliation, representative selection (stages 4-6) | **occurrences are blocks, not messages**, and identity is a *witness* token, not content — an occurrence is an equivalence class over creation and restatement witnesses, united only by a stable occurrence reference or a producer-declared copy relation; the `Creates`/`Creates` collision (`order_graph.rs:622`, an inner emission and its parent's) was a hole revision 2 could not express; matching became a global injective assignment with authority *tiers* separated from search order, since greedy is provably wrong here; a call/result reference is causality, **not** a matching tier; the provisional-to-confirmed merge algebra written out, with the honest note that the graph merge is monotonic while the returned order legitimately is not — membership and lineage are what stay stable; stages 4 and 5 became **one engine with two policies**, matching against `ReplayPrecedence` rather than the presentation DAG; representative selection became **per block occurrence**, because a whole-copy choice deletes blocks only the other copy held; added the "one fact, one job" table. Also: **the motivating Vercel example was overclaimed** — the committed golden and the new pure-semconv collision fixture are both correctly ordered, so the argument rests on the ripple table, not on that case | `order_graph.rs:622`; the existing backtracking replay matcher; `types.rs`'s block granularity; measurement of the committed `vercel-ai-js/tool-use` golden and `_synthetic/carrier_collision_agent_and_generation` |
| 4 | stages 1, 2, 7 and buildability | **the persistence boundary**, which revisions 1-3 omitted: stages 1-2 run at ingest and 3-7 at read, and the facts stage 3 needs are not persisted — the instrumentation scope is *never captured for spans at all* (`extract/mod.rs:335`; the `scope_name` columns are metrics-only), so a scope-keyed profile has no input at read time. That is an unmentioned persisted-format change, and it makes provenance capture step 1. Stage 1 became a **`CarrierBundle`**, since one emission is routinely spread over sibling attributes (tool name + call id + arguments; OpenInference's dotted keys plus `input.value`; Vercel's `ai.response.*`). Stage 2's restriction was too broad to be true and now separates *syntactic* identity and order (permitted) from *occurrence* identity and *global* order (not), naming the two decoders that violate it. Stage 7: `order_graph` is reusable as algorithms, **not** as a module — its evidence collection reads exactly the `BlockEntry` facts this design deletes, and **SCC condensation does not exist**, contrary to revision 1. Specified the two things that could still make two implementations return different *membership*: injectivity is **per bundle, not global** (two parent snapshots must both match one occurrence, or invariant 6 fails), and the objective is authority-first, then cardinality. Added the per-block winner policy. And, on the reviewer's recommendation, **the document now argues the case against itself** and recommends incremental adoption over a rewrite | `extract/mod.rs:335`, the metrics-only `scope_*` columns, `RawMessage { source, content }`; `messages.rs:896`/`:1071`/`:3767`; `order_graph.rs:105`/`:999` |
| 5 | reliability and scale | **stage 4 as specified is factorial in the worst case** (`n!/(n-m)!` injective assignments; it bites on repeated identical content with no stable ids — the shape that made the existing cross-trace matcher need a budget). Added the whole operational contract that revisions 1-4 lacked: a matching budget mirroring `MATCH_BUDGET = 20_000` with forced unions outside the search, canonical witness-locator traversal, and exhaustion that keeps proven matches and **under-strips** rather than guessing; a batched graph merge, since per-merge recomputation is `O(M(V+E))`; the admission that input-linearity is **not knowable from this document** and needs a stage-4 benchmark before any promotion. **Statelessness restated set-wise** — "the provisional node's id is kept" described mutating state that does not persist, so an occurrence keeps every witness id as an alias with the primary chosen by locator order; and **evidence is not monotonic across reads**, because ClickHouse may have merged away the earlier version, so the merge algebra must be a pure function of the rows now present. Chose **build-time profiles**, since the reconstruction cache is keyed on rows alone and a runtime-editable profile set is exactly the stale-answer failure that key discipline exists to exclude. And **"self-scaling" is the wrong goal for this layer** — autoscaling cannot reduce one 68 MB reconstruction's latency; the six checkable properties replace it | `feed/mod.rs:353`/`:404`; `feed/cache.rs`'s row-only key and `every_field_of_a_row_reaches_the_digest`; ClickHouse's inability to express an as-of read |
| 6 | framework independence | **the claim was not defensible and is now weakened to one that is**: framework identity is not required to decode and conservatively render *supported semconv* carriers, while producer scope and version remain correctness inputs for private dialects and for standard carriers whose occurrence semantics are locally ambiguous. "A conforming producer works automatically" is false — a producer emitting correct semconv **plus** its real answer in a private member (the CrewAI `raw` shape) leaves that answer opaque, because profiles may not read arbitrary payload. Counted the payoff instead of asserting it: 13 of 16 extractor families and ~10 attribute-recovery families are **irreducible** dialect adapters, so what the redesign removes is the **six framework-specific policy couplings**, now listed as the deliverable. Added the semconv-revision analysis — better at meaning changes, slightly *worse* at alias churn unless one canonical versioned compatibility module exists, which is not yet specified. And the ripple table gained a third row, measured in this cycle: the "obviously right" fix of letting the generic reader fill an empty side failed three ways, ending in langgraph 12 → 28 messages, because `output.value` means *answer* on a generation span and *node state* on a chain span | `attributes.rs`'s 28 recognition rules and its private usage-JSON readers; `messages.rs`'s 16 extractor families; measurement of `crewai/files`, `answer_beside_the_conversation`, `nested_state_messages` and the langgraph suite |
| 7 | the invariant set | **the invariants are neither independent nor mostly testable, and the central one is not falsifiable.** Six pairs overlap (3↔9, 3↔12, 3↔5, 5↔12, 4↔6, 6↔7), and two entries are not invariants at all — claim conformance is an oracle requirement, profile permutation a determinism property. Nine have **no test**, five only weaker approximations, one is genuinely covered; `feed/props.rs` cannot generate a single shape the model needs. Six *kept* invariants would falsely accuse a legitimate corpus shape (retried calls, ordered restatement evidence refining order, branch interleavings no producer can serialise, unanswered calls in error turns, the feed's deliberate newest-first order, provably-distinct identical creations). Added the missing invariant — **distinct creation witnesses are never contracted** without independent identity evidence — with the five-case discrimination its test needs, and the admission that it catches a wrong *assembler* and not a wrong *profile*, for which ground truth is not constructible from telemetry that carries no occurrence marker. Restored the answer invariant the list had dropped, in the form that does not falsely accuse (`a turn with explicit completion evidence has an answer`). And a rule for exemptions: a legitimate one **proves the antecedent is false in the source**, and fails when the violation *disappears* | `feed/props.rs`'s generator; `carrier_semantics_are_declared` (blind to a dropped carrier); `reading_more_carriers_only_adds_messages` (rendered output, not decoder output); `PAIRING_EXEMPT`, `NO_ANSWER_EXPECTED`, `KNOWN_DEFAULTED`; `REORDERS_UNDER_PER_CARRIER` as the pattern to copy |
| 8 | the verification apparatus | Constructive rather than critical: specified what to build. **Do not extend the row fuzzer** - it cannot reach one shape the model needs. A scenario generator derives *both* the oracle and the telemetry from a `SourceProgram`, so ground truth comes from the action taken rather than from any reading of the output; shrinking is over the program, under a contract that forbids a shrink from turning "fresh witness" into "re-delivery". Specified the five-case contraction test over **occurrence equivalence classes** (and corrected revision 7: contraction on stable identity must be *required*, or an assembler that ignores it passes), seven metamorphic transformations with what each may and may not change, five mutation controls named per seam, and the dependency order - a test that cannot fail today is worthless as a gate on tomorrow's change. Also: `reading_more_carriers_only_adds_messages` is **not** a decoder-locality test, since it compares rendered output. And the answer on falsifiability without annotation: no for arbitrary telemetry, but **yes** for controlled captures with a machine-generated manifest - drive each real SDK from a generated program against deterministic fake models, recording actions against live span contexts out of band | `feed/props.rs`; `REORDERS_UNDER_PER_CARRIER` as the bidirectional pattern; `PAIRING_EXEMPT`, `NO_ANSWER_EXPECTED`, `KNOWN_DEFAULTED` as the three that skip instead of proving |
