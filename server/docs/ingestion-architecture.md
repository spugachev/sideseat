# Ingestion architecture

**Status**: revision 17, after seventeen consecutive architecture reviews. **The deferred track is closed** — see "The gate, answered". Revision log at the end.

What this describes: how OTLP spans from any GenAI framework become the message list the span, trace,
session and feed views return — **and**, because thirteen reviews established that the semantics cannot
be specified without them, what is persisted at ingest, what is hydrated per read, and what a caller can
see. Revisions 1-12 opened with "not how they are stored, queued or served" and then specified
persistence, cache keys, API versions, retention and data handling. That sentence was false, so it is
gone rather than the content.

## Status of every mechanism in this document

The one table a reader needs first, because the rest of the document describes mechanisms at four
different levels of authorisation and nothing else says which is which.

| Mechanism | Status | Authorised work | Prerequisite | Promotion gate |
| --- | --- | --- | --- | --- |
| **Carrier-instance claims at ingest** (observation type decides a generic carrier's meaning) | **landed** (`1e623a45`) | maintain | — | mutation controls, both directions |
| **Five false-equivalence repairs** — exceptions per span, executions per span, a reportable-leaf error rule, a rescued user turn | **landed** (`416e7599`, `349e4c5f`, `2ea7eaf8`, `b9ede710`) | maintain | — | the conservation checks below |
| **Conservation checks** — a reported exception reaches the trace; a single-trace fixture keeps its user turn | **landed** (`ff30d64c`) | maintain | — | three mutations, each naming its fixture |
| **Plain-text generic I/O** | **landed** | maintain | — | `_synthetic/plain_text_generic_io` |
| **Resolver authority** — one order, `legacy_rank` opaque, feed consumes it | **approved** | build | — | byte-identical corpus output |
| **SCC condensation and a degradation signal** | **approved** | build | resolver authority | a constructed cycle is reported, not silently broken |
| **The request-scoped framing edge** | **approved** | build, shadow, promote | resolver authority | the 27-view ratchet empties |
| **Instrumentation scope + carrier provenance persistence** | **approved** | build | — | a scope-keyed rule becomes expressible |
| **Compact read envelope, stage 0R** | **approved** | build | — | the three already-loaded facts become reachable |
| **Data classes and the `framework_config` rule** | **approved** | build | provenance | the sentinel-token test |
| **Stage 8 / v2 output contract** | **separate project** | schema and read work | scope decision | v2 goldens |
| **Profile language, global occurrence assembler, reconciliation rebuild** | **rejected** | none | — | the gate was tested and not met |

## The gate, answered

The stop/go gate was: *a reproducible defect, pinned by a committed fixture, that the current pipeline
plus a local carrier-instance rule cannot fix and the occurrence model can.* Review 14 was spent trying
to construct one from the 121 committed fixtures.

**It could not be constructed, and the attempt found a real defect instead.**
`openai-agents/image_gen` collapsed three separate `generate_image` failures into one exception — a
genuine false equivalence, the hardest class this document names, hiding in committed data behind a
golden that had recorded it as correct. Under the occurrence model it resolves cleanly: three creation
witnesses, no stable identity connecting them, three occurrences. But **the local repair uses exactly the
same information** — an exception is composed from its span's own fields, so the span belongs in its
identity — and it is four lines. Fixed in `416e7599`; the trace now shows three calls, three exceptions
and three results instead of three, one and three.

The three cases the design calls undecidable do not rescue the track either, because the occurrence model
*relabels* rather than decides them:

| Case | Under the occurrence model |
| --- | --- |
| `gen_ai.assistant.message` on a generation span | decided only by a scope-and-version producer contract — which **is** a local carrier-instance rule |
| the CrewAI `raw` shape | works only through a named dialect decoder; for an unknown producer it stays opaque under either architecture |
| identical id-less repeats | already preserved within one atomic emission by position; across emissions the local witness repair covers it; inside an accumulated snapshot **neither** architecture can know |

And the structural reason this keeps happening: profiles are *forbidden* from consulting other spans,
content equality or trace-wide facts. So where a deciding fact is genuinely non-local, a profile cannot
establish the claim either — assembly can only report the ambiguity. The global machinery adds
provenance and formalism, not correctness that was otherwise unavailable.

**So the profile language, the global occurrence assembler, the merge algebra, the reconciliation
rebuild and occurrence-driven representative selection are rejected**, not deferred — a gate nobody can
meet is worse than a decision, because it keeps unauthorised work looking imminent.

What is retained from that design, on its own merits:

- the **claim vocabulary and decision ledger** as an explanation of the local decisions that already
  exist — documentation and diagnostics, not a new semantic engine;
- the **`SourceProgram` truth generator, manifests and mutation controls** as verification
  infrastructure, which is authorised step 1 and independent of any of it;
- **narrow, versioned scope-keyed carrier-instance rules**, which is what every repair in this document
  turned out to need.

## The decision

**Build three bounded things. Keep the existing extraction, deduplication and reconciliation.**

1. verification and decision-ledger infrastructure;
2. resolver authority, SCC diagnostics, and the framing edge;
3. scope and provenance persistence, the read contract, and data classes.

What thirteen reviews **established**: heuristic coupling is real and measured; the observation-type gate
and plain-text reading were real local repairs needing no schema change; request framing is a
reproducible defect across 27 trace views; and scope persistence, the read projection, the four views'
totals, raw-data handling and cycle diagnostics each have genuine defects.

What they did **not** establish: that the motivating carrier-collision defect exists; that a profile
language would classify occurrences correctly; that a global assembler beats targeted local repairs;
that stage 4 meets normal-path scaling; that false contraction is detectable on arbitrary telemetry; or
that framework independence needs rebuilding — the normalisation layer already reads no framework at all.

So the honest conclusion is not "build it" and not "nothing": it is the three parts above, with the
occurrence model kept as a designed, costed alternative behind an evidence gate.

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
all 121 fixtures and repairs the shape (`1e623a45`,
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

### Extended to every shared sample: one defect, three false alarms, two corrections

The survey was then run over **all eleven sample programs shared by three or more suites**, comparing
role/kind shapes. Four outliers stood out; investigating each against its raw payload is what makes the
technique trustworthy, because three of the four were **not** defects:

| Outlier | Verdict |
| --- | --- |
| `strands/swarm` places its frame after the turn | **real defect** — the same one-position displacement as langgraph and claude-agent-sdk |
| `agent-framework/image_gen` ends on three tool results with no reply | **faithful.** The payload's final message is `{"role":"assistant","parts":[{"type":"reasoning","content":""}],"finish_reason":"stop"}` — the model said *stop* and sent nothing |
| `agent-framework/mcp_tools` shows `system, user, assistant` and no tool calls | **faithful** — the capture contains no tool call at all; the sample never reached its MCP server |
| `adk/swarm` shows three messages for a swarm | **faithful** — a 7 KB capture with one `call_llm` span, where `transfer_to_agent` appears only in the prompt and a tool *definition*. The handoff never happened |

So the technique's yield is one real defect from four candidates, and its cost is that each candidate
needs the raw payload to settle. That is the right trade — but "a suite disagrees" is a *lead*, never a
finding, which is precisely why cross-framework agreement cannot be an automatic gate.

**Two corrections to earlier reviews come out of this**, both from committed fixtures:

- Review 7 proposed refining the answer invariant to *"a turn with explicit successful completion
  evidence has an answer"*. `agent-framework/image_gen` **falsifies it**: `finish_reason: "stop"` with
  no answer content whatsoever. A producer's completion marker is not evidence that content exists, so
  the refinement would falsely accuse a faithful capture. The current weaker form — assistant *or tool*
  activity after the last user turn — is correct for this corpus.
- The evidence that refinement needs is mostly absent anyway: `finish_reason` appears in exactly **one**
  suite's goldens (`vercel-ai-js`). Any rule keyed on it is untestable here.

And a gap the survey exposed in the answer invariant itself, worth stating rather than fixing: a trace
ending on a *tool result* passes, because a tool call counts as activity after the question. That is
deliberate — a trace may legitimately be captured mid-turn — but it means "the tool ran and the model
never came back" is invisible. Of the 12 trace views with no assistant *text* after their last user
turn, eight are `structured_output` samples whose answer legitimately arrives as JSON or a tool call,
one is the known error exemption, and two are synthetic. Only the `image_gen` case above was neither,
and it turned out faithful.

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

So this needs a **hard edge, not a key term**: `system → the other inputs of its request`, resolved
where "before" is a constraint rather than a position. It is the first case in this document where the
constraint form is not merely tidier but *necessary*, and it is an input-framing constraint rather than
a causal one — a class the resolver does not yet have.

Reverted. Four measured attempts now, and this is the only one whose diagnosis names the mechanism that
would work.

### The scope of the framing edge, and the fixtures that falsify a wrong one

"Its request" is **one generation invocation's input envelope** — keyed today by
`(trace_id, generation_span_id)`, and in the target model by a `RequestInputId` from the carrier-bundle
locator. It is emphatically *not* the trace, an arbitrary span, `batch_key` (that is response batching:
`(trace, span, timestamp, direction)`) or dedup's `ResponseKey` (which carries carrier and content shape
only to rank repeated calls).

Every wider scope is already falsified by a committed fixture:

| Wrong scope | Falsified by |
| --- | --- |
| the whole trace | `adk/image_gen` — its second art-critic instruction belongs at index 9, *after* the first turn completed, not before the initial user |
| the whole session | `adk/reasoning` — the same instruction legitimately recurs at indices 0, 4 and 8, one per request boundary |
| "all system messages lead" | `anthropic/session` — changed instructions at indices 0, 5 and 10 |
| anything that reorders a correct request | `adk/image_gen` again — its single-carrier request is already `system, user`, and `assert_carrier_subsequence` requires that array order to survive |

And **`Frame` cannot mean `role == System`**: `developer` normalises to `System`, and an in-band system
or developer message inside an ordered array is a turn *in that array*, keeping only its forward
positional scope. The frame is the detached request prefix — `system_instruction`,
`user_system_prompt` — which is a fact about the carrier, not about the role.

### Measured extent, and a ratchet so it cannot grow silently

`a_system_instruction_precedes_the_first_user_turn` now measures it: **27 trace views across 22
fixtures, and every single one is `system@1, user@0`** — displaced by exactly one position, never more.
One cause, one fix. Three suites: claude-agent-sdk (8 fixtures), claude-agent-sdk-js (8, agreeing
exactly with the Python SDK), langgraph (5), and `strands/swarm` — which the ratchet *found*, since the
cross-framework survey had only looked at `tool_use`.

It is a **known gap, not an exemption**: the invariant's antecedent is true and the output is wrong, so
review 8's exemption rule does not apply. It is listed bidirectionally instead, the
`REORDERS_UNDER_PER_CARRIER` pattern — a new fixture cannot join silently, and a fixture that starts
passing must be removed. Both directions mutation-verified.

The test compares *first* system against *first* user, which is deliberately weaker than the real
per-request relation and immune to the four wrong scopes above. Its own limit is stated where it lives: a
trace whose first request carries no instruction while a later one does would be accused wrongly, and no
fixture has that shape.

### Does the framing class generalise? Mostly no, and that is worth knowing

Checked against the corpus, input framing is the **only** additional ordering relation it proves
necessary — everything else that looked like a candidate is already covered:

| Candidate | Why it is not a new edge class |
| --- | --- |
| tool definitions | not messages at all — `FeedResult.tool_definitions`, deduplicated and sorted alphabetically |
| retrieved RAG context | represented as a tool result, so its placement is *causal* (`strands-js/rag-local`). `ContentBlock::Context` exists but **no committed golden has `entry_type: context`**, so there is no corpus basis for an edge |
| thinking before its text | atomic-emission cohesion plus source position (`adk/reasoning` has them adjacent from one response) |
| assistant preamble before its calls | the same — cohesion, not framing |

So: do not introduce generic role ranks for system, context, thinking or tools. That is the same trap
`dedup.rs` already documents for role ranking, arriving from a new direction.

### The correction that matters most: `order_graph` is not the ordering layer

Revision 8 assumed it was. It is not — **the project feed re-sorts the resolver's output** with a second
scalar tuple, `(order_time, span, message_index, entry_index, after_call, content_hash)`
(`feed/mod.rs:1370`), before reversing response runs. So there are two scalar orderings, and the
resolver is not the final authority for that view. That explains the shape of the failed attempt
precisely: it changed 15 *feed* views because the feed's own sort responded to the altered time, while
the trace views were decided by a tie the span term broke.

Consequently the work is two increments, in this order:

1. **Provably neutral plumbing**: compute today's scalar order once as an opaque `legacy_rank`, pass
   ranks, response groups and edges to the resolver, and make the feed consume resolver rank *within* a
   response instead of rebuilding `feed_positions`. Byte-identical across the corpus is the gate. This
   is much smaller than the stage-7 refactor and it makes the resolver's answer authoritative — which
   nothing else in this document can be built on until it is true.
2. **The framing edge**, request-scoped, shadowed and then promoted one class at a time.

Adding an edge class does *not* require the full refactor: `Constraints` already gates classes
individually, `NEUTRAL` must reproduce the legacy order byte-for-byte, and generation dataflow already
demonstrates the exact pattern of reading pre-dedup evidence and projecting endpoints through lineage.
Two caveats: `NEUTRAL` proves a *disabled* class is neutral, never that the request scope is right — that
needs its own A/B assertions — and a bad framing edge can cycle, which today is silently resolved by
releasing the smallest node, with no SCC diagnostic.

### Which copy's span does the edge attach to?

**None of them.** Edges are derived from *all* pre-dedup observations of the instruction and then mapped
through lineage to survivor units, giving the union `system_survivor → request₁ inputs`,
`… → request₂ inputs`, and so on. Generation dataflow already avoids representative dependence this
way; priorities likewise read every credible copy rather than the winner.

Reading `survivors[i].span_id` instead would make a quality tie decide which request gets the edge, which
is exactly what `which_copy_survives_does_not_change_the_order` forbids — and that test's perturbation
already flips winners in 17 fixtures. Framing eligibility therefore belongs to each observation's
provenance, never to the chosen representative's role.

### What becomes of the sort key

The coherent end state, with each term reassigned rather than deleted:

| Term | Becomes |
| --- | --- |
| `batch_time` | credible-time ready-node **priority**, never precedence |
| `message_index`, `entry_index` | local sequence edges, or order within a contracted unit |
| `after_call` | gone — call → result is an edge |
| `span` | part of request identity and witness location, not semantic order |
| `content_hash` | **must not** decide order: changing a representation would then change placement |
| the final tie-break | a stable occurrence/witness locator |

Transitionally the whole tuple survives as one opaque `legacy_rank`, used only to break ties among
simultaneously-ready nodes — which is precisely the role `unit_min_legacy` already plays under `NEUTRAL`.

Two things this establishes for the design. Cross-SDK agreement belongs in the verification apparatus
as a first-class check, not as an observation. And the ordering layer needs constraints for input
*framing*, not only for causality — which is an argument for the constraint graph that neither the
ripple table nor the invariant audit had produced.

## The span-versus-trace comparison, which is the method that works

The `openai-agents/image_gen` defect was found by comparing **span views against the trace view** —
content present per span that vanishes at trace level. That comparison targets false equivalence
directly, which is the class this document calls hardest to detect, and it had never been run
systematically. Run over all 121 fixtures:

**787 groups** appear on N spans and fewer than N times in their trace. Almost all are the product
working: `langgraph/swarm` sends one user question to 135 generation spans, and collapsing that to one
message is the whole point. So the raw count is not a defect list — the signal is in the exceptions.

Two shapes stood out, and they have different verdicts:

| Fixture | Shape | Verdict |
| --- | --- | --- |
| `openai-agents/image_gen` | three identical exceptions on three spans → **one** in the trace | **defect, fixed** (`416e7599`) |
| `langgraph/tool_use` trace-4 | its user question present on 13 spans, **absent** from the trace view | the documented cross-trace limit — the question is byte-identical to trace-1's, so a genuine repeat is indistinguishable from a replay |
| `strands-js/swarm` trace-1 | its user question present on **one** span, absent from the trace *and* the feed | **defect, fixed** — see below |

### The narrowed check, and the defect it found

A sharper question than "does content collapse": **does a populated trace view show a user turn, when
its own spans carried one?** Corpus-wide that is true of exactly **two** traces, and one of them cannot
be explained by cross-trace stripping.

`strands-js/swarm` has a **single trace**. Its `chat` span holds `system, user, assistant, assistant`;
the trace view holds `system, assistant, assistant, tool` and the feed holds
`assistant, tool, assistant, system`. The user's request — *"Create a simple plan to build a weather
app…"* — survives in one span view and nowhere else. A user opening that trace sees a plan and no
request.

The mechanism, measured rather than assumed — and my first reading of it was **wrong**, which is worth
recording because the wrong reading was plausible. It is *not* the timestamp phase: the user event sits
46,416 ns **after** its chat span's start, so that test is false. Two other phases do it, and both copies
of the question are caught:

| Copy | Marked by |
| --- | --- |
| on the `chat` generation span | the blanket **child-generation** rule — all unprotected content in a non-root generation span |
| on `invoke_agent Planner` | the **accumulator** rule — an input event on a non-root accumulator span |

With both copies history and no non-history equivalent anywhere, the history-only filter drops the class
entirely. The filter is right in general — it is what removes previous turns — and what is wrong is that
the two rules together can cover *every* copy of a turn that demonstrably happened.

The narrowest fix keeps exactly one witness: in the child-generation rule, preserve a single user-input
witness when its timestamp is at or after its generation span's start **and** no non-history equivalent
exists in the trace. Every other child copy stays history, the timestamp rule is untouched, and no
previous turn can be resurrected — a genuine re-send predates the span it was sent to.

**Fixed**, and the acceptance condition was met exactly: the corpus delta is `strands-js/swarm` alone,
trace and feed each gaining the turn, every span view unchanged. The first attempt did ripple — rescuing
*any* history-marked user gave langgraph's `tools` span views a message they had never shown, because a
span view loads one span and "nothing else carries the turn" is trivially true there. Restricting the
rescue to blocks the **child-generation** phase marked is what makes it scope-safe by construction: that
phase only runs when an agent span is in scope, so a single-span view never reaches it.

The trace now reads `system, user, assistant, tool_use, tool_result`.

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
  process and every cache starts empty. Note precisely what this does and does not buy: determinism holds
  **within one build**, and during a rolling deploy two replicas can interpret identical rows
  differently. That is a real weakening of the cross-replica determinism the operational contract asks
  for, and it needs one of three answers — return the normaliser version with the answer, route a session
  consistently for the duration of a rollout, or state the window. It is not free. This is still the
  choice, because the alternative is worse.
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

The stage boundary and the **process** boundary are different, and earlier revisions confused them —
one said the split fell "exactly between stage 2 and stage 3", the diagram persisted raw carriers
*before* decoding, and stage 0R claimed to hydrate stages 3-7. The resolution: **stages 1-2 run at
ingest**, and what crosses the persistence boundary is decoded observations plus provenance and
raw-evidence *references*.

```
at ingest:
  1. carrier capture        (bundles, span context, instrumentation)
  2. syntax decoding        (payload shape → observations, each with its claim)
     ↓ persisted: observations, provenance, data class, raw-evidence references
per read:
  0R. read projection       (hydration - what the later stages are allowed to know)
  4.  occurrences + causal relation
  5.  session replay reconciliation
  6.  representative selection
  7.  presentation projection
  8.  the output contract   (what a caller can see)
```

Each stage may only know what is listed for it. A stage that needs a fact from a later stage is a design
error, not something to thread through. Stage 3 (claims) is numbered with the ingest stages because it is
where the claim is *attached*; whether the claim is computed at ingest or recomputed per read from
persisted observations is a deferred-track question, not one this document settles.

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
    members,           // per member: key or event name, ordinal, and a *reference* to the raw
                       // payload rather than a second copy of it - see the data-class section
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

> **Rejected alternative.** Stages 3-6 below are a designed and costed model that was **tested against
> its own gate in review 14 and rejected**: no defect in the 121-fixture corpus needs it, and every one
> that looked structural was repaired by a local carrier-instance rule. Kept as the record of what was
> examined and priced, so the next structural-looking defect starts from here instead of re-deriving it.
> Nothing below is authorised work.

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

- creates an **explicitly provisional** occurrence when nothing else evidences its content, **provided
  the observation decoded as conversational content**. This is the distinction earlier revisions
  conflated with the data-class rule: an unknown *occurrence semantics* on decoded content is a
  provisional occurrence, while an unknown *data class*, or a carrier that did not decode at all, is
  opaque evidence and is never promoted into the conversation;
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
`replay_stability: Stable`, or by a producer-declared same-emission relation — and only across
*independent* observations. Within **one** `Creates` observation, two positions are always two
occurrences (invariant 5), so two entries of one emission list carrying the same stable id are
**malformed evidence and are diagnosed**, never contracted. **Content equality is never sufficient
anywhere.** Absent such evidence they stay two occurrences, or one explicit ambiguity — never
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
- **stated set-wise, not as a mutation.** Earlier revisions wrote this as "when direct evidence later
  arrives, the provisional node's id is kept and edges are redirected" — describing persistent state that
  does not exist. A read is a pure function of the rows present *now*, so there is no node to keep and
  nothing to redirect. The rule is: an occurrence's witness set is whatever the current rows evidence;
  a class containing a creation witness is a confirmed occurrence, one containing only restatement
  witnesses is provisional; its identity is the class's minimum witness locator, so any replica derives
  the same id from the same rows;
- a confirmed class takes its production anchor, cohesion and edges from its creation witnesses; a
  provisional class has none of the three.

**The consequence, and it is not monotonicity.** Between two reads the evidence set may *shrink* — a
ClickHouse merge can have removed the earlier version of a re-delivered span — so "evidence only
accumulates" is false and cannot be relied on. What is guaranteed is that the answer is a function of
the rows: identical rows give an identical answer on any replica, and a changed row may legitimately
change membership *and* order. Earlier revisions promised stable membership across re-delivery, which is
a promise about the store rather than about this pipeline.

One case earlier revisions could not answer, now explicit: if a provisional `"yes"` is accompanied by
*two* distinct `Creates("yes")` observations, neither claims it. An equally-supported match stays
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

**`order_graph` is not even the whole ordering layer** — the project feed re-sorts its output with a
second scalar tuple (`feed/mod.rs:1370`), so the resolver is not the final authority for that view. And
it is **reusable as algorithms, not as a module** — revision 3 overstated this. What is
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

## Stage 0R: the read projection, because stage 8 cannot build what was never loaded

Revision 10 specified envelopes — model, parameters, response id, tools — and assigned **no stage the
job of hydrating them from storage.** That is review 4's persistence finding repeated one layer later:
a stage describing an output that cannot be constructed from its input.

So the read path is its own stage, and it sits before reconstruction rather than inside stage 8:

```text
stored span rows
   → read projection / hydration contract      (what stages 3-7 are allowed to know)
   → reconstruction stages
   → stage 8 output projection                 (what a caller sees)
```

The split matters because hydration decides three things at once — what the pipeline can know, how wide
the query is, and what enters the cache key — and it is independently testable across both analytics
backends. Stage 8 should decide what callers see; it must not silently also decide what the pipeline was
allowed to load.

### The audit: persisted, loaded, reachable

`MessageSpanRow` is a 21-column projection, and the store is far wider. Taking only the facts a
*debugging* user needs:

| Fact | Persisted | Loaded by the message projection | Reachable from a message endpoint |
| --- | --- | --- | --- |
| requested model, provider, status, total tokens, total cost | yes | yes | direct |
| **input / output tokens** | yes | **yes** | **no** — the DTO carries one `tokens` field and the metadata one total |
| **span start and end** | yes | **yes** | only one derived block timestamp |
| **exception type / message / stacktrace** | yes | **yes** | only synthesised leaf-error text; parent errors are suppressed |
| response model, operation name, provider response id, temperature/top-p/top-k/max tokens, penalties, stop sequences | yes | no | `raw_span` on a *different* endpoint |
| span-level finish reasons, cache and reasoning tokens, the cost split, framework, duration, span name/kind, user, environment | yes | no | a second request, joined on `(trace_id, span_id)` |
| **instrumentation scope name and version** | **no** | no | **nowhere** — extraction ignores `scope_spans.scope` |

The first three rows are the indefensible ones: the bytes are already in hand, so omitting them saves no
database bandwidth. A user currently cannot answer "same prompt and model — did the temperature differ?",
cannot correlate a failure with provider logs by response id, and cannot distinguish an exception type
from its message without issuing a second request and re-deriving both.

There is a subtler trap: model, provider, status and observation type are attached only to **returned
blocks**, so a message-less generation span can move the entity totals while exposing no envelope at all.

### `include_raw_span` does not close it

It is not accepted on the message endpoints at all — only on span, trace and span-feed routes. Even
where it is available, a caller could re-derive parameters and finish reasons only by issuing another
request, joining spans to blocks, **reimplementing the server's dialect-specific fallback chains**, and
reimplementing enrichment such as the corrected token total, which deliberately differs from the raw
provider value. And it can never recover the instrumentation scope, which was never archived.

As a heavy debugging hatch that is coherent. As the implementation of stage 8 it is an admission: a
mandatory envelope field must not require the client to become a second ingestion implementation.

### The four views do not report the same totals

The blocks are structurally consistent — all four go through one `BlockDto` conversion — but the
envelopes around them are not, and the totals mean four different things:

| View | Totals come from |
| --- | --- |
| span | the pipeline's sum over that span row |
| trace | the **trace entity**, overriding the pipeline |
| session | the **session entity**, overriding the pipeline |
| feed | the **page's own rows**, summed *after* `MESSAGE_CONTENT_FILTER` |

The feed's figure is therefore neither the spend for the blocks returned, nor for the traces
represented, nor all spend for the page's activity — it is "spend on message-query-eligible spans of
this page", which is a fourth thing and should be named as one. The field names also differ (`messages`
vs `data`, `total_messages` vs `message_count`) while both count *blocks* rather than provider messages.

And block-level `tokens` and `cost` are the containing span's totals **copied onto every block**, so a
client that sums them multiplies. Names that invite the wrong operation are a defect in the contract,
not a caveat for documentation.

### What widening costs, and what the cache says about it

Recommendation: load a **compact envelope in the same message query** — operation, models, response id,
parameters, raw and normalised finish reasons, framework, the usage and cost breakdown, and the exact
error fields — as one envelope per span rather than repeated per block. Do not join `otel_spans` to
itself; the facts are already on the selected row. Keep `raw_span` and provider bodies lazy and
referenced.

The cost is `additional bytes / 27 MB/s` on the cold path, and compact scalars are negligible beside
replayed message payloads. The alternative — a second session-sized request — roughly doubles the
measured p50 (23.5 ms → ~47 ms on DuckDB, 357 ms → ~715 ms on ClickHouse) *and* introduces a
snapshot-consistency problem between the two reads.

The reconstruction cache settles the design question rather than complicating it. It digests **every**
field of `MessageSpanRow`, enforced structurally, so: adding a field that affects the answer is
**required** — otherwise a changed parameter would serve a stale envelope under an unchanged key — and
adding an irrelevant field is a pure cost, because it causes misses that change nothing.

## Stage 8: the output contract

Revisions 1-9 specified how evidence is classified, assembled and ordered, and said nothing about the
type it lands in. That is a gap, not a scope boundary, and it produces a direct contradiction: the design
promises honest ambiguity, representation completeness and evidence accounting, while the DTO exposes
**none** of the ambiguity, alternatives or lineage. An invariant whose result the caller cannot see is
not a guarantee.

### What the current shape loses

Worth stating precisely, because "SideML is lossy" is already documented for roles and the rest is not:

| Loss | Consequence |
| --- | --- |
| the API returns a **flat list of blocks under a field named `messages`**, and `total_messages` counts blocks | one provider message holding text plus two images is indistinguishable from several adjacent messages, except through `message_index` — which is assigned by enumerating the current parse, so it is not a durable identity |
| roles collapse to four, and an **unknown role becomes `User`** | a parse failure produces a plausible false user turn. `developer` → `System` is the case review 9 showed blocks a correct framing rule |
| the feed derives role from block *type* | every `ToolUse` is Assistant and every `ToolResult` is Tool, whatever the source message said |
| request parameters, provider response id, temperature/top-p/max-tokens, stop sequences | extracted and persisted, but **not loaded by the message projection**, so they cannot reach the client — exactly the fields that answer "why did this answer differ?" |
| structured output has no typed contract | `response_format` and `JsonSchemaDetails` exist but are populated only from a literal message field, and are **dropped when tool blocks are split**. Vercel's schema, output mode and provider metadata are in the corpus and not represented. The same logical structured output is `Json` for Vercel and `ToolUse` for LangGraph, with nothing recording that they are the same thing |
| citations become generic `Context` blocks | no cited range, source id, offsets or relationship to the sentence they support, so they cannot be rendered as annotations |
| streaming becomes completed text | the normaliser recognises `combined_chunk_content` and `chunk_count` and then emits ordinary text, discarding the count. A streamed answer is indistinguishable from an atomic one |
| media variants are sparse | no dimensions, duration, codec, transcript, checksum, page range or **generation lineage** — so a generated image cannot say which tool call and parameters produced it except by adjacency |
| finish reasons collapse to five, unknown ones discarded | the raw reason should survive beside the normalised category |
| `Unknown { raw }` helps only while a block stays unknown | once a handler recognises a block, whatever that handler does not model is dropped |

`BlockEntry` already carries source position, source event, source attribute and the history/correlation
flags; `BlockDto` omits position, source event, source attribute and promotion status. So the pipeline
computes provenance and then declines to send it.

### Three contract mismatches, found and fixed

The web contract is mirrored by hand, and it had drifted from the server in three ways that cost the
client real information:

| Mismatch | Effect |
| --- | --- |
| `ToolResult` omitted `name` | Gemini and ADK identify a result **only** by function name and emit no call id, so the client had nothing to label those results with and fell back to guessing across block-level fields. Now declared and preferred over the derived fields |
| `ToolUse.input` declared `Record<string, unknown>` | the server's type is any JSON value, so a provider sending a bare array or string made the declaration a false statement |
| `Unknown` omitted `raw` | the server's escape hatch against data loss was invisible in the client's type |

That is the argument for **generating** the client types from a schema rather than maintaining a union
by hand: three silent divergences in one file, each of which the compiler would have caught.

### What stage 8 has to be

A versioned projection, with one rule **that binds v2 only**: it may omit heavy evidence by reference,
and may not silently erase provenance, ambiguity or a lossy transformation. The v1 adapter necessarily
erases all three — that is what byte-compatibility with today's shape means — so v1 is labelled a
**lossy compatibility projection** rather than held to a rule it cannot satisfy.

```text
project(occurrences, cohesion_groups, selected_representations, ordering, provenance, diagnostics)
    -> ConversationView { schema_version, envelopes, groups, occurrences, diagnostics }
```

Per occurrence: `occurrence_id`, `cohesion_group_id`, `part_index`, `role { kind, source }`,
`placement` (in-band or request frame), canonical content, witnesses, the selected representation *and
its alternatives*, ambiguity status, field origins, inferred fields, and losses. Per envelope: the
provider protocol, request and response ids, model, parameters, tools, tool choice, response format —
and the raw provider envelope where it was retained.

Two type changes are worth calling out because they are behaviour, not shape. A role becomes
`{ kind, source }` so `human`, `model`, `ipython`, `developer` and an unrecognised value survive
normalisation — and **an unknown role must never silently become `User`**. And `placement` records
whether a message was framing evidence or an in-band turn, which is the fact review 9's framing edge
needs and the current model cannot express.

### Migration, given that the client and 119 goldens both encode today's shape

Keep the current endpoint as v1 and add v2 beside it; generate the TypeScript from the schema instead of
hand-mirroring; dual-run with a byte-compatible v1 adapter. The existing goldens stay as **v1
compatibility tests** — and it is worth being precise that they verify role, block kind, selected content
fields, tool name and finish reason, *not* the whole API contract, so v2 needs its own goldens for
occurrence identity, cohesion, provenance, alternatives and ambiguity. Historical rows whose raw carrier
data was never persisted are marked `fidelity: legacy_incomplete` rather than having fields fabricated
for them.

### Round-tripping, and the scope decision the document owes

A SideML message **cannot** be turned back into a provider request without inventing or losing
something: the original role and instruction priority, frame versus in-band placement, the provider's
block shape, message grouping, citation relationships, media metadata, streaming chunks, the exact
finish reason, the original tool-vs-function protocol, synthesised tool ids (the Gemini path generates
one), tool definitions, schema output mode, request parameters, and provider extensions. Even fields
`ChatMessage` *has* — `response_format`, `tool_choice`, `model`, `stop`, `parallel_tool_calls` — are
inconsistently populated and discarded when tool blocks are split.

So SideML must be one of two things, declared: a **loss-aware canonical model** (byte round-trip needs
the raw envelope retained; semantic replay needs a canonical capsule plus field-level provenance), or an
explicitly scoped **presentation model**. Today it is used as the former and implemented as the latter,
and the `Unknown { raw }` comment about "lossless round-tripping" is evidence that the confusion is not
only mine.

## Invariants

**One corrected set, stated once.** Earlier revisions printed a list and then a long audit rejecting
parts of it, leaving both normative; the audit's conclusions won, so they are the list now. Each is
framework-independent. The column that matters is whether a test exists **today** — nine did not, which
is why the verification work is authorised ahead of every mechanism.

| # | Invariant | Test today |
| --- | --- | --- |
| 1 | **Evidence accounting** — every carrier is decoded, retained as opaque, or diagnosed; nothing disappears silently | none. `carrier_semantics_are_declared` is blind to a *wholly dropped* carrier |
| 2 | **Decoder locality** — decoding one carrier depends only on its payload and dialect, compared **before** claims or rendering | none. `reading_more_carriers_only_adds_messages` compares rendered output |
| 3 | **No unsupported assertion** — every occurrence, multiplicity decision, edge and anchor names its evidence | the structural half only; a wrong claim can be cited as evidence |
| 4 | **Representation independence** — re-encoding or enriching an occurrence does not create another | none |
| 5 | **Multiplicity preservation** — two positions in one `Creates` observation are two occurrences, always; positions in a `Restates` observation prove nothing. A *consequence* of the claim: it catches an assembler that ignores a right claim, never a wrong claim | partial, corpus only |
| 6 | **Restatement idempotence** — a proven restatement changes **membership** not at all. It may refine *order* when it carries `Ordered` sequence evidence. Applies when reconciliation completed; under budget exhaustion see below | only exact re-delivery, which passes trivially |
| 7 | **Replay invariance** — a **producer-valid, branch-scoped** serialisation of a prior order may be re-sent without creating occurrences; matching is injective **per bundle**, so two parent snapshots may both match one occurrence | none |
| 8 | **Authority separation** — choosing a display representation cannot change identity, placement, anchors or order | partial: compares role and kind only |
| 8a | **Representation completeness** — the block-occurrence set is independent of which rendering wins. Invariant 8 forbids *movement* and is silent about *loss* | none |
| 9 | **Witnessed causality** — only local evidenced relations are hard edges: emission member order and cohesion; a result after **a matched call occurrence** (not merely a reused id); matched generation inputs before that generation's output | partial |
| 10 | **Presentation independence** — presentation constraints cannot change the occurrence multiset or the reconciliation result | **covered** — the best-tested invariant here |
| 11 | **Deterministic projection** — acyclic evidence yields a deterministic linear extension; contradiction is localised to an SCC and reported. Applies to the **chronological projection**, not to the feed endpoint, which is newest-first by contract | determinism only; SCC condensation does not exist yet |
| 12 | **Honest ambiguity** — ambiguity is in *matching a restatement*, never in whether two creations exist. Two identical positions in one `Creates` observation, parallel branches and retries are provably distinct | none, and `assert_no_duplicates` currently asserts the opposite for id-less repeats |
| 13 | **No unwitnessed contraction** — distinct creation witnesses are never united without independently verifiable occurrence-identity evidence or a producer-declared copy relation. Content equality is never sufficient | none. This is the one that would catch false equivalence |
| 14 | **A turn shows activity** — after the last user turn there is assistant or tool activity, unless the source proves error or cancellation | **covered** (`assert_has_an_answer`), and it caught three defects in this document |

Two entries from earlier revisions are **not** invariants and have moved to the verification section:
claim conformance is an oracle requirement (it says the output matches annotations, not that the
annotations are true), and profile permutation is a determinism property of a registry.

**Under budget exhaustion, 6 and 7 hold only where reconciliation reported completion.** Exhaustion
deliberately under-strips, which *creates* provisional duplicates — so an unconditional idempotence
promise and a conservative budget cannot both be true. Completion is reported to the caller
(`reconciliation_complete`), and the guarantees are conditioned on it. Earlier revisions asserted both.

Deliberately *not* an invariant: "stable insertion" in its strong form. New evidence may legitimately add
an edge and correct an earlier order; stability applies to redundant and restating evidence only.

### Why the set is shorter than it looks

Six pairs of the earlier list encoded one rule twice — 3↔9, 3↔12, 3↔5, 5↔12, 4↔6, 6↔7 — which is the
same objection that removed `multiplicity_authority` from the claim: a derived rule is a second place to
get it wrong. The genuinely independent dimensions are six: input accounting and locality (1-2); the
claim oracle and registry determinism (now in verification); occurrence non-contraction and idempotence
(4-7, 12, 13); representation versus placement (8, 8a); presentation versus reconciliation (10); graph
validity and projection (9, 11).

And the generator cannot currently produce a single shape most of them need — `feed/props.rs` emits
isolated rows holding one message, with no hierarchy, bundles, copies or branches. That is why
verification is authorised ahead of every mechanism rather than beside it.

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

### Measured: the normalisation layer already reads no framework at all

Every review so far has reported a defect, so it is worth recording the one thing that measures
*better* than the design assumed. Searching the whole of `domain/sideml/` — normalisation, carriers,
dedup, history, correlation, ordering, cache — for any read of the framework:

**There is none.** Every occurrence is a comment naming which producer motivated a shape ("Agent
Framework uses `content` instead of `text`"), never a branch on a `Framework` value. `MessageSpanRow`
does not even carry the field, so the reconstruction stages could not read it if they wanted to.

Two consequences. The claim "framework identity is not the correctness switch" is **already true at the
stage where correctness is decided** — the dependency lives entirely in extraction, which is exactly
where the design says irreducible dialect adapters belong. And review 11's observation that framework is
persisted, filterable, and never reaches reconstruction is therefore not a defect: it is the property
holding.

That narrows the redesign's job. Stages 3-7 need to be *made* explicit and testable; they do not need to
be made framework-independent, because they are.

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

### Data classes, because this design changes what is stored

The corpus contains the proof that this is not hypothetical: `crewai/agent_core` is gitignored because
CrewAI serialises its entire model configuration into a span attribute and the capture held a live
`aws_secret_access_key` and `aws_session_token`. The design's direction is to read *more* carriers more
faithfully, so exposure is a consequence of the architecture rather than a separate concern.

### What happens to such a value today

There is no ingestion-time redactor. What exists (`truncate_for_log`'s neighbour in `utils/string.rs`)
only *recognises* producer-supplied placeholders like `<redacted>`; it transforms nothing. Copies of one
secret-bearing span:

| Copy | When |
| --- | --- |
| `raw_span` | **always** — `build_raw_span_json` copies every span, event, link and resource attribute unfiltered |
| `messages` | only if the generic reader admits that carrier. CrewAI's own extractor reads `crew_tasks` and `output.value`, not the configuration in `input.value`, so today it does not |
| `input_preview` | possibly, truncated to 200 characters, if the value falls early enough |
| the Redis stream | temporarily, as the whole protobuf request, until acknowledged and trimmed |
| `traces.jsonl` | in debug mode, the complete request, append-only |

The four message views load `messages`, never `raw_span` — so a value that stayed out of `messages` is
absent from all four. `raw_span` is reachable through span, trace and span-feed routes with
`include_raw_span=true`, and through MCP's `get_raw_span`. All require project read access.

One place a value's *content* reaches a log: an unparseable attribute's first 100 characters, at
**trace** level (`parse_json_with_fallback`). Off by default, and genuinely useful for diagnosing an
extractor — worth knowing rather than removing.

### The correction: persisting raw payloads is not a pure addition

Revision 11 called provenance capture "a pure addition". For the scope and the locator that is true.
For **lossless raw carrier payloads** it is false: `raw_span` already holds a lossless copy, so
persisting the payloads again duplicates sensitive bytes *and* moves them out of a debugging archive
into the hot reconstruction contract — into the cache key, the projection, and whatever v2 returns —
without defining retention, access or projection boundaries.

So a carrier member persists a **reference**, not the bytes:

```rust
CarrierMemberRecord {
    locator,              // trace / span / source kind / key / ordinal
    source_path,          // where in the payload
    data_class,           // see below
    payload_digest, size,
    raw_evidence_ref,     // addresses the existing raw archive - no second byte copy
    decode_status,        // and losses
}
```

### The data classes, and the one projection rule that follows

The principled line is **not** "does this look like a secret" — that is a regex that will miss the next
credential format, corrupt a legitimate prompt that discusses credentials, and create false confidence.
The line is *what kind of thing the value is*:

| Class | Treatment |
| --- | --- |
| authored content, model output, tool input/output, retrieved context | preserved verbatim. Masking these destroys the product — a user debugging a prompt needs the prompt |
| **framework execution configuration** — provider clients, model objects, credential chains, environment snapshots | evidence about execution, and **never a conversation occurrence** |
| credentials inside that configuration | the exact bytes have almost no debugging value; presence, source, type and expiry do |
| unknown | preserved as raw evidence, and **not promoted** into the conversation |

The rule: **`framework_config` is never a conversation occurrence.** It may inform an envelope, a
diagnostic or a raw-evidence view. That single rule is what stops a CrewAI model configuration from
being rendered as a user turn — which is exactly what the third failed attempt in the ripple table did
to `crewai/files`.

And it is enforceable *because* of this document's central principle. "Carrier instance, not carrier
name" is what lets `input.value` on a CrewAI configuration span be raw evidence while `input.value` on a
generation span is content. The same mechanism, paying off a third time.

### Retention, which is the other half

Canonical conversation content deserves the trace's normal lifetime; lossless raw evidence deserves a
separately configurable and usually shorter one. Today they share it, and the two backends differ in a
way that should be documented rather than discovered: **DuckDB defaults to a count limit only** (five
million spans, no age limit unless configured), while **ClickHouse carries a 90-day TTL** plus optional
age cleanup. So the same deployment decision has different consequences per backend.

### How it is verified

A synthetic span carrying a unique sentinel token, asserting: it appears **once** in retained raw
evidence; **zero** times in `messages`, previews, tool definitions, envelopes and provenance ledgers when
classified as framework configuration; that all four message endpoints omit it; that the authorised raw
endpoint returns it; that an unauthenticated or cross-project reader cannot; that raw-evidence expiry
removes it while canonical content survives; and that debug capture is *deliberately* raw.

### What the operator documentation must say

Not in this document, but it must exist, and it must say plainly: SideSeat stores raw OTLP span, event
and resource attributes, so telemetry can contain prompts, documents, database statements and
credentials a framework serialised by accident; `raw_span`, the Redis stream, backups and debug captures
are all sensitive; debug mode writes complete requests to disk; which routes expose raw data and under
which scope; how to configure age retention and delete affected traces or projects; and that **OTLP
ingestion authentication is optional and defaults off**.

## The honest limit

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
- onboarding a dialect may require a bundle declaration, a decoder, claims, fixtures **and a release** —
  earlier revisions said "adds local claims", which understates it, since 13 dialect decoders are
  irreducible and a private answer stays opaque without one. What must hold is *non-interference*:
  onboarding must not change another producer's semantics, and that needs its own test rather than
  following from field-wise profile merging.

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

## Step 2 measured: the resolver's order is *better*, not identical

The plan's step 2 was gated on byte-identical corpus output. **That gate cannot be met, and the reason is
worth more than the gate was.** The feed does not consume the resolver's order at all: it re-sorts with a
second scalar tuple (`(order_time, span, message_index, entry_index, after_call, content_hash)`) and then
reverses response runs. So "make the resolver authoritative" is a behaviour change, and the question is
how big.

Measured by computing the resolver-authoritative projection beside the current one for every fixture —
group by response *identity* rather than adjacent runs, reverse the groups, preserve the order within
each — and comparing fingerprints. **Eight unique disagreements across five fixtures**, of two kinds:

| Kind | What differs | Which is right |
| --- | --- | --- |
| entry types inverted | the resolver puts `tool_use` before `tool_result`, and `tool_result` before the answer text; the scalar sort inverts both | **the resolver** — it holds the call→result edge, while the scalar key has only `span` and `content_hash` to go on |
| same content, different span | the two pick different *copies* of one block (identical content hash, different `span_id`) | equivalent in what is displayed; they differ only in attribution |

So step 2 is a small, reviewable improvement rather than a refactor — which is the standard this document
demands of everything else ("every delta names its edge"). The gate is restated accordingly: **eight
positions, each attributable to a call→result edge or to a copy choice**, and any ninth is a regression.

Three things this also settles. The two orders are *not* equivalent by construction, so the neutrality
argument that made the resolver safe to run in production does **not** extend to the feed. `feed_positions`
being shared is not sufficient, because the two comparators give its fields different precedence.
And `content_hash` must never become a graph edge merely to force the gate green — it is a tie-break of
last resort, and a resolver that ordered by content would make placement depend on wording.

## The plan

Five separate orderings had accumulated — a five-step recommendation, a nine-step migration, review 9's
two increments, review 8's verification-first sequence and review 10's stage-8 migration — and they could
not all be followed. This is the single one. Steps 1-7 are authorised; 8 is a gate; 9-11 happen only if
it passes.

| # | Step | Done when |
| --- | --- | --- |
| 1 | The `SourceProgram` truth generator, stage observables, mutation controls, and the corrected invariant suite | each new test **fails** against a deliberately broken pipeline |
| 2 | Make the resolver authoritative for trace *and* feed, with `legacy_rank` opaque | **a reviewed 8-position delta**, not byte-identity — measured, see below |
| 3 | SCC condensation and a degradation signal, then the request-scoped framing edge: shadow, then promote | a constructed cycle is reported; the 27-view ratchet empties |
| 4 | Persist instrumentation scope, carrier provenance, ordinals, data class and **raw-evidence references** | a scope-keyed rule is expressible; no second copy of any payload |
| 5 | The compact read envelope; settle the v2 schema and keep v1 explicitly lossy | the three already-loaded facts become reachable |
| 6 | The claim ledger and annotated claims, in shadow mode | a claim's provenance can be printed for every observation |
| 7 | Move the two confirmed cross-carrier decisions out of decoders (OpenInference enrichment, Claude Code suppression) | no output change |
| 8 | The v2 output contract: generated client types and v2 goldens, the v1 adapter byte-compatible | v1 unchanged for existing callers |

Steps 9-11 of the earlier plan — the shadow assembler, the membership switch, the reconciliation
rebuild and the occurrence-driven graph — are **withdrawn**. The gate they waited on was tested in
review 14 and could not be met; see "The gate, answered".

"Resolver last" from review 8 means *occurrence-driven promotion* last — not the neutral authority
plumbing in step 2, which everything else depends on.

**The rule for goldens**, unchanged and load-bearing: do not regenerate until every delta is attributable
to a named claim, union, replay match or edge. "The corpus changed by 22 fixtures" is not evidence of
correctness; "these three occurrences moved because these generation-dataflow edges became available" is.

### What is kept of the rejected design, and why

The profile language, the global assembler, the merge algebra and the reconciliation rebuild remain
described in this document as a **rejected alternative**. Keeping them is deliberate and cheap: fourteen
reviews of costing is the expensive part, and the next time a defect looks structural the right first
question is "is this the shape that was already examined and priced?" — which needs the design present
to answer. Nothing in those sections is authorised work, and the stage-3 header says so.

What crossed over into authorised work is smaller and more useful than the model itself: the claim
vocabulary as diagnostics, the truth generator as verification, and the discovery that a
*carrier-instance* rule — carrier plus span context — is what every repair actually needed.

## Revision log — history, not instructions

**Non-normative.** Each row is a design review that found something the previous revision had wrong.
Kept because the reasoning is the expensive part and re-deriving it would cost more than reading it — but
nothing here is a specification, and a row may describe a mechanism a later revision deleted (revision
3's "one fact, one job" table is one). Where a row and the sections above disagree, the sections win.

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
| 9 | the ordering layer | **`order_graph` is not the ordering layer** - the project feed re-sorts its output with a second scalar tuple, which revision 8 had assumed away and which explains the failed repair exactly (15 feed views moved because the feed's own sort saw the altered time; the trace views were decided by a tie the span term broke). Specified the framing edge's scope as one **generation invocation's input envelope**, with four committed fixtures falsifying every wider scope (`adk/image_gen`'s second instruction at index 9, `adk/reasoning`'s three request boundaries, `anthropic/session`'s changed instructions, and ADK's already-correct single carrier), and established that `Frame` **cannot** mean `role == System`, since `developer` normalises to it and an in-band system message is a turn in its array. Checked whether framing generalises: **it does not** - tool definitions are not messages, RAG context is causal, thinking is cohesion; there is no corpus basis for any other new edge class, and generic role ranks stay forbidden. Edges attach to **no** surviving copy: derived from every pre-dedup observation and mapped through lineage, or a quality tie decides which request gets the edge and `which_copy_survives_does_not_change_the_order` breaks. Reassigned every term of the sort key to its end state, with the whole tuple surviving transitionally as one opaque `legacy_rank`. Recommendation adopted: fix the defect now, in two increments, the first being provably-neutral plumbing that makes the resolver authoritative | measured: 27 trace views across 22 fixtures, every one `system@1 user@0`, now pinned bidirectionally by `a_system_instruction_precedes_the_first_user_turn` (which found `strands/swarm`, a fixture the `tool_use` survey had missed) |
| 10 | the destination type model | Added **stage 8, the output contract**, which the design did not have - and the gap was a contradiction rather than an omission: the invariants promise honest ambiguity, representation completeness and evidence accounting while the DTO exposes none of it, and an invariant the caller cannot observe is not a guarantee. Catalogued what the current shape loses, including that the API returns a **flat list of blocks under a field named `messages`** with `total_messages` counting blocks, so one provider message holding text and two images is indistinguishable from several messages; that an unknown role silently becomes `User`, turning a parse failure into a plausible false turn; that request parameters and the provider response id are extracted and persisted but **not loaded by the message projection**; that structured output's schema and mode are dropped when tool blocks are split, and the same logical structured output is `Json` for one framework and `ToolUse` for another with nothing recording the equivalence; that streaming's chunk count is recognised and discarded; and that `BlockEntry` carries provenance which `BlockDto` declines to send. Fixed three **live contract mismatches** in the hand-mirrored client types, each costing real information. Stated the scope decision the document owes: SideML is used as a canonical model and implemented as a presentation one, and must be declared as one or the other | verified: server `ToolResult.name` (Gemini/ADK identify a result only by name) absent from the client, `ToolUse.input` declared as an object where the server sends any JSON, `Unknown` missing its `raw` |
| 11 | the read path | **Stage 8 specified an output that cannot be built from its input** - revision 10 named envelopes and gave no stage the job of hydrating them, which is review 4's persistence finding one layer later. Added **stage 0R, the read projection**, before reconstruction rather than inside stage 8, because hydration decides what stages 3-7 may know, how wide the query is, and what enters the cache key, and is independently testable per backend. Audited persisted vs loaded vs reachable: three facts are **already loaded and still unreachable** (input/output tokens, both span timestamps, the exact exception fields), so omitting them saves no bandwidth; a dozen more need a second request joined on `(trace_id, span_id)`; and the instrumentation scope is reachable nowhere because extraction ignores `scope_spans.scope`. `include_raw_span` is **not accepted on the message endpoints at all**, and even where it is, using it requires a client to reimplement the dialect fallback chains and the enrichment - so it is a debugging hatch, not a contract. The four views report **four different totals**, the feed's being 'spend on message-query-eligible spans of this page', which is none of the three things a caller might mean; and block-level `tokens`/`cost` are the span's totals copied per block, so the names invite a wrong sum. The cache settles the widening question: a field that affects the answer **must** be in the row or a changed parameter serves a stale envelope under an unchanged key | measured: a second session-sized request roughly doubles p50 (23.5 → ~47 ms DuckDB, 357 → ~715 ms ClickHouse) against `additional bytes / 27 MB/s` for same-query widening |
| 12 | data sensitivity | The corpus already contains the proof: `crewai/agent_core` is gitignored because CrewAI serialises its whole model configuration into a span attribute and the capture held live AWS credentials. **Corrected revision 11's claim that provenance persistence is "a pure addition"** - for the locator and the scope it is, for *lossless raw payloads* it is not: `raw_span` already holds a lossless copy, so persisting them again duplicates sensitive bytes and moves them from a debugging archive into the hot reconstruction contract, the cache key included. A carrier member therefore persists a **reference**, a digest and a data class rather than the bytes. Established the principled line, which is not "does this look like a secret" (a regex that misses the next format and corrupts a prompt discussing credentials) but **what kind of thing the value is** - and one rule follows: `framework_config` is **never a conversation occurrence**, which is exactly what the third ripple-table failure did to `crewai/files`. Enforceable *because* of "carrier instance, not carrier name", the same principle paying off a third time. Traced every copy a secret-bearing span makes today, documented the DuckDB/ClickHouse retention asymmetry (count-only by default versus a 90-day TTL), and specified the sentinel-token verification | verified: `build_raw_span_json` filters nothing; no redactor exists (the helper only recognises producer placeholders); the four message views never load `raw_span`; one trace-level log carries 100 characters of an unparseable value |
| 13 | the document as one artefact | **Restructured rather than extended.** Twelve area reviews had accumulated 12 live contradictions, five incompatible plans, and sections that read as instructions for work nobody had authorised. Fixed: the opening claim "not how they are stored" was false and is gone; the stage diagram now separates the **stage** boundary from the **process** boundary (stages 1-2 at ingest, 0R hydrating the rest); `Unknown` had two incompatible meanings, now split into unknown *occurrence semantics* on decoded content versus an unknown *data class*, which is never promoted; the provisional merge was written as persistent mutation in a stateless system and is now **set-wise**, with the honest consequence that evidence can *shrink* between reads so "monotonic" was wrong; the invariant list and its own audit were both normative, and are replaced by **one corrected set of 14** with a column saying which have a test today (five do); idempotence and full stripping are now **conditioned on `reconciliation_complete`**, since a conservative budget deliberately under-strips; stable-identity contraction is scoped to *independent* observations, so two entries of one emission list are malformed evidence rather than contracted; build-time profiles no longer claim cross-replica determinism "for free"; and stage 8's no-erasure rule binds **v2 only**, since a byte-compatible v1 necessarily erases. Deleted as dead weight: "one fact, one job", "what survives and what goes", and a duplicated falsifiability section. Added **the status table** and **one plan** replacing five. And the verdict that matters: the case is stronger as a *diagnosis* and weaker as a *rewrite* - three bounded parts are authorised, the occurrence model is deferred behind a stated gate | the document's own measurements: the motivating defect did not reproduce, two candidate defects were repaired locally, and the normalisation layer reads no framework |
| 14 | the stop/go gate | **The gate was tested and could not be met, so the deferred track is closed rather than left pending.** Review 14 spent its whole budget trying to construct a defect in the 121 committed fixtures that needs the occurrence model - and found a real defect instead: `openai-agents/image_gen` collapsed **three** separate `generate_image` failures into one exception, a genuine false equivalence hidden behind a golden that recorded it as correct. The occurrence model resolves it cleanly and the **local repair uses the same information in four lines** (`416e7599`), which is the gate's answer in miniature. The three cases the document calls undecidable turn out to be *relabelled* rather than decided: the choiceless-output case is settled by a scope contract, which **is** a local carrier-instance rule; the CrewAI `raw` shape needs a named decoder either way; identical id-less repeats inside an accumulated snapshot are unknowable to both. And the structural reason - profiles may not consult another span, content equality or trace-wide facts, so where the deciding fact is non-local a profile cannot establish the claim either. Rejected: the profile language, the global assembler, the merge algebra, the reconciliation rebuild, occurrence-driven representative selection, and plan steps 9-11. Retained on their own merits: the claim vocabulary as diagnostics, the truth generator as verification, and narrow scope-keyed carrier-instance rules | `openai-agents/image_gen` before and after: three calls, **one** explanation, three results → three, three, three |
| 15 | hunting the same defect class | The span-versus-trace comparison found **two more** real defects and one false alarm. 787 groups appear on N spans and fewer than N times in their trace, almost all of them the product working (one langgraph question reaches 135 spans), so the yield is in the exceptions: two executions of a same-shaped tool call collapsing across **six** fixtures, and `strands/error` losing its error entirely. The tool-call repair needed a discriminator to stay safe — how many calls of one shape a *single response* lists — because trace-wide ranking alone turned a re-sent pair with regenerated ids into four calls, which a unit test pinned. `langgraph/tool_use` trace-4 losing its question is the documented cross-trace limit, not a defect: its question is byte-identical to trace-1's | `349e4c5f`; six fixtures gained their missing executions |
| 16 | finishing the found defects | Verified and fixed the two outstanding ones, and **corrected a mechanism I had documented wrongly**. `strands/error` showed *no error at all* because a parent deferred to an ERROR-status child with nothing renderable; "leaf" now means deepest *reportable* error, and `NO_ANSWER_EXPECTED` is **empty** — its one entry claimed the run produced no answer when it had produced a `ValidationException`, so the exemption was documenting this defect rather than a property of the source, which is precisely what an exemption must never do. And `strands-js/swarm` is **not** the timestamp phase: the user event sits 46,416 ns *after* its span's start. Two other rules mark the two copies, and the fix keeps one witness under conditions that make it scope-safe — restricted to the child-generation phase, because rescuing accumulator-marked blocks gave langgraph's `tools` span views a message they had never shown | `2ea7eaf8`, `b9ede710`; delta exactly the two fixtures |
| 17 | the invariant that would have caught them | **General local-witness conservation is not constructible from the goldens**, and saying so is the finding: establishing the legitimate aliases (history re-send, parent suppression, cross-trace stripping) needs `parent_span_id`, `status_code`, timestamps and session identity that `InvariantRow` does not carry — and using the pipeline's own dedup lineage as proof would let faulty dedup certify its own false equivalence. Two narrow checks were built instead and cover three of the five defects, flagging nothing today and each mutation-verified: **at least one reported exception reaches the trace** (source-backed antecedent, so an ERROR status with no detail is not accused) and **an exception reported by several distinct spans keeps its multiplicity**, plus **a single-trace fixture whose spans carried a user turn shows one**. Rejected as unsound: "every span exception has a trace representative" flags the 18 legitimately suppressed parent copies across ten fixtures, and the tool-call-id form flags 13 trace views because a regenerated id legitimately disappears. Also corrected a claim of mine: `openai-agents/tool_use` looked like a sixth defect and is not — its second NYC id is a *regenerated* one, and the two similar answers have different digests, so the trace is right | `ff30d64c`, with the three mutation messages recorded in its commit |
