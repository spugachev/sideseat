# Message-parsing fixtures

Inputs for `server/src/domain/traces/message_goldens_tests.rs`, which checks that message
**count, content, ordering and absence of duplicates** hold for every framework *that has a
fixture here*, in all four views the API exposes. Coverage is 11 of the 32 frameworks SideSeat
recognises and not every fixture has a session view - see [What is and is not
covered](#what-is-and-is-not-covered), which is the honest version of this sentence:

| View    | Row set                                                | API endpoint                     |
| ------- | ------------------------------------------------------ | -------------------------------- |
| span    | `WHERE span_id = ?`, no content filter                 | `/spans/{trace}/{span}/messages` |
| trace   | whole session when the trace has one, then scoped back | `/traces/{id}/messages`          |
| session | every row of every trace in the session                | `/sessions/{id}/messages`        |

A trace belongs to **exactly one** session: the one on its earliest span, ordered by
`(timestamp_start, span_id)`. Frameworks do put two session ids on one trace — ADK emits its own
beside the caller's — and 24 of these fixtures once recorded a session view under *both*, holding the
identical messages. The goldens had blessed that duplication as correct, which is the failure mode
they are most prone to.
| feed    | every row, newest response first                       | `/feed/messages`                 |

The first three call `process_spans` and differ only in their row set, so each is built with its
own - using `process_feed` for a session tested ordering no session request can return. The feed
has its own entry point and its own ordering, and is here because while it was left out it was the
only view where a duplicate could surface unchecked. Its pagination is not modelled: that is a
property of the endpoint, not of parsing.

Trace, session and feed row sets apply `MESSAGE_CONTENT_FILTER` and `ORDER BY timestamp_start
ASC`, exactly as the queries do — feeding unfiltered rows made whole sessions come back empty.

## Layout

```
<suite>/<sample>/req-001.pb        captured OTLP payload (protobuf, or .json)
<suite>/<sample>/req-002.pb        one file per exported batch, in capture order
<suite>/<sample>/expected.json     committed expectation
```

The fixture is the **raw OTLP payload the framework actually sent**, not database rows. That
is the only input the server really receives, so a fixture cannot drift from reality. The test
replays it through the real ingestion path (`extract_attributes_batch`,
`extract_messages_batch`, SideML conversion, enrichment) before comparing.

## Support matrix

The boundary of "correct for all frameworks": exactly the suites below, at the versions they were captured
against. `the_corpus_matches_the_support_matrix` fails if a suite is added or removed without updating this
table - so the claim stays checked rather than described.

It lives here, beside the fixtures it describes, and not in `CLAUDE.md`. That file is tracked, but project
convention keeps it out of routine commits, so its committed content lags the working copy by however much has
been written since - a test reading it would compare the corpus against whatever state a given checkout
happens to carry, which passes or fails on how recently someone committed a document rather than on whether
the corpus matches it.

| Suite | Version captured against | Samples | Captured requests |
| --- | --- | --- | --- |
| `_synthetic` | hand-written shapes, no SDK | 14 | 14 |
| `adk` | google-adk >=1.27.0 | 8 | 18 |
| `agent-framework` | agent-framework-core >=1.0.0b0 | 10 | 17 |
| `anthropic` | anthropic >=0.84.0 | 7 | 18 |
| `bedrock` | boto3 (bedrock runtime) | 6 | 14 |
| `claude-agent-sdk` | claude-agent-sdk >=0.2.0 | 8 | 17 |
| `claude-agent-sdk-js` | @anthropic-ai/claude-agent-sdk ^0.3.246 | 8 | 17 |
| `crewai` | crewai >=1.10.1 | 9 | 33 |
| `langgraph` | langgraph >=1.1.2 | 9 | 23 |
| `openai` | openai >=1.80.0 | 6 | 8 |
| `openai-agents` | openai-agents >=0.12.1 | 10 | 37 |
| `strands` | strands-agents >=1.30.0 | 10 | 40 |
| `strands-js` | @strands-agents/sdk ^1.14.0 | 7 | 12 |
| `vercel-ai-js` | ai ^7.0.79 | 6 | 13 |
| **14 suites** | | **109** | **272** |

Two further samples exist but are **not in the repository**: `strands-js/image-gen` and
`vercel-ai-js/image-gen`, whose payloads are 15 MB and 7 MB of inlined base64 image data (the Python
`image_gen` fixtures cover the same path in under 100 KB, because media is rewritten to file URIs). They are
gitignored and captured locally when working on image handling, so the counts above are what a checkout has.
`local_only_samples_are_actually_gitignored` stops that exemption from excusing a sample somebody merely
forgot to commit.

## Capturing a suite

Needs working model credentials, since the samples call a real model.

```bash
misc/capture-message-fixtures.sh                    # every suite
misc/capture-message-fixtures.sh strands            # one suite
misc/capture-message-fixtures.sh strands tool_use   # one sample
```

Then record the expectations, **read them**, and only then let them gate:

```bash
UPDATE_GOLDENS=1 cargo test -p sideseat-server message_goldens   # write expectations
misc/review-message-goldens.py                                   # read them: counts, roles, content
misc/review-message-goldens.py --suspicious                      # only fixtures with warnings
misc/review-message-goldens.py strands/tool_use                  # one sample, full detail
git diff server/tests/fixtures/messages
cargo test -p sideseat-server message_goldens                    # from now on it gates
```

`review-message-goldens.py` exists because `git diff` on this much JSON is unreadable. It
renders each view's message count, role sequence and content, and flags patterns that usually
mean a parsing defect (a conversation with no assistant message, unbalanced tool calls, raw
JSON in a text position). Those are heuristics for a human to judge — the hard guarantees are
in the test.

Recording is a separate, explicit step on purpose: a golden written straight from current
output enshrines whatever bugs exist today. `UPDATE_GOLDENS=1` writes the files but still exits
non-zero if an invariant was violated, so known-bad output cannot be committed as reviewed.

The invariants hold regardless of what a golden says, which is what makes a blindly regenerated
snapshot still fail on a real defect:

- every returned block belongs to the scope requested, by exact id (a span view never leaks a
  sibling span; a trace view never survives `scope_feed_to_trace` with another trace's block)
- a session's trace views partition its session view exactly — summing them must equal it. This is
  now asserted for **every** session, and the reason it once could not be is worth keeping: ADK emits
  its own session id alongside the sample's, so a trace named two sessions and appeared under both,
  which made the partition meaningless for exactly the fixtures where it mattered. A trace belongs to
  the session on its earliest span, so that cannot happen — and the check that a session claims no
  foreign trace is an assertion rather than a skip
- no duplicate (role, kind, full-content digest) within one trace. This is also a deliberate
  product limit: a genuine repeat of the same tool call or message inside one trace is collapsed,
  because it is indistinguishable from a history re-send — see the pipeline notes in
  `sideml/feed/mod.rs`
- every tool result's id matches a call in the same trace, and a call is never answered twice.
  Results with no id are outside this check: a result whose framework identifies it only by name
  is linked to its call by position (oldest unclaimed call of that name), and where no call is
  available it stays unlinked rather than acquiring an invented id
- no empty text or thinking blocks
- a view holding a user message also holds something from the assistant or a tool. Every other
  invariant here is about not returning the *wrong* thing; this is the only one that notices
  content which never arrives at all, which is how CrewAI's answers went missing for as long as
  they did — the extractor read the reply from a field it only consulted when no history was
  present, so exactly the runs that had a conversation lost the response. A fixture that
  legitimately has no answer is exempted by name with its reason (only `strands/error`, whose
  sample exists to fail), so the exemption is a claim someone made rather than a silent pass
- the projection is self-consistent (counts, role sequence and message list agree)
- all of the above hold for the **project feed** view as well, which has its own pipeline entry
  point (`process_feed`) and its own ordering - newest response first, each response read
  top-to-bottom. It was the one view outside the harness, and so the only place a duplicate could
  surface unchecked. The answer check is the weaker "something answered a question" there: no
  position in a feed is "the last turn", since it descends across responses and ascends within one
- processing the same fixture twice gives the same answer, checked once per suite
- **redundant evidence changes nothing**: re-delivering every span of a fixture yields the same
  messages in the same order, not merely the same count
- **arrival order decides nothing**: reversing the order spans reach the pipeline yields the same
  answer, so an extraction change that merely shifts arrival order cannot look like a content change
- **a matched tool result follows its call** - causality, not adjacency, since Vercel emits
  `call, call, result, result`. Not applied to the project feed, which descends across responses by
  design: there a call and an earlier response's result are legitimately reversed
- **survivors of one carrier keep the order that carrier stated**, compared only where the carrier
  *has* an order - two entries of one array. Paths that diverge at an object member are not compared,
  because members have no order: Anthropic puts its system prompt in a sibling of `messages` and the
  pipeline rightly renders it first

`UPDATE_GOLDENS=1` reports invariant violations instead of aborting, so one bad fixture does
not hide the rest.

## What is and is not covered

**111 expectation files: 106 captured in 13 suites, plus 5 synthetic.** A suite is not a framework:
`strands`/`strands-js` and `claude-agent-sdk`/`claude-agent-sdk-js` are one framework each in two
languages, so the 13 captured suites cover **11 of the 32** frameworks SideSeat recognises. (32 is
the union of the server's `Framework` classifier and the SDK's framework list, excluding `Unknown`:
28 named server variants plus `anthropic`, `openai`, `google-genai` and `pydantic-ai`, which only the
SDK names.) Every framework is not covered, and the gap is deliberate rather than hidden:

| Covered by fixtures (11) | strands, langgraph, crewai, google-adk, bedrock, openai, openai-agents, anthropic, agent-framework, claude-agent-sdk, vercel-ai — strands and claude-agent-sdk in both languages, vercel-ai in JS only |
| ------------------- | --- |
| Synthetic, not a framework | `_synthetic/*` — hand-written payloads for shapes no captured sample produces, counted in the file total and in neither the suites nor the frameworks. See below. |
| Has samples, no fixtures | `autogen` — its runner has no Bedrock path, so capturing it needs a first-party key. Listed in the capture script and skipped with a message, so its absence is visible. |
| Recognised, no fixtures (21) | ag2, agentscope, agno, autogen, azure-ai-foundry, azure-openai, browser-use, google-genai, haystack, langchain, langflow, livekit, llamaindex, logfire, mlflow, **openinference**, pydantic-ai, semantic-kernel, smolagents, traceloop, vertex-ai |

The second group shares extractors with covered frameworks, so the *parsing logic* is exercised
— but nothing here proves their emitted payloads match what those extractors expect. Adding a
sample suite is what closes that, not adding an expectation file.

Also uneven: 30 fixtures have no session view, because their sample never sets a session id.
Session views are built only for real session ids, since the endpoint cannot be asked for a
session that does not exist. Sessionised captures are what would cover those, not a synthetic
fallback.

## The synthetic fixtures

Hand-written payloads for shapes the captured corpus does not reach. They exist because an invariant
that holds trivially proves nothing: each of these makes a specific rule bite, and the mutation that
breaks that rule is named.

| Fixture | The shape | What it makes bite |
| --- | --- | --- |
| `tool_use` | a Strands call/result pair | the baseline hand-written case |
| `multi_turn_one_carrier` | nine turns in **one** carrier, in conversation order | carrier subsequence across many siblings - the ADK shape, where one span holds a whole conversation |
| `parallel_tool_calls` | two distinct calls in one response, then both results | causality *without* adjacency: `call, call, result, result` must be allowed |
| `resent_history` | a later span re-sending the earlier turn | the re-send collapses onto the original rather than duplicating it |
| `cross_span_tie` | a generation span and its tool span reporting the **identical** instant, with the tool span's id sorting *first* | `adopt_call_positions`. Disable it and this fixture reports the answer at index 1 before its question at index 3; every captured fixture stays green, because none of them ties |

The carrier-overlap defect is documented by `reading_more_carriers_only_adds_messages` instead of by a
fixture: it runs every fixture through both extraction modes and reports what each one gains and what
reorders. A hand-written payload for that shape was tried and dropped - it reproduced the *attributes*
but not the behaviour, because the LangGraph reader claims only on a `langgraph.*` marker and parses a
narrower message shape than the one written by hand, so its exemption would have claimed a cause the
fixture did not exhibit.

The carrier-overlap defect is documented by `reading_more_carriers_only_adds_messages` rather than by a
fixture: it runs every fixture through both extraction modes and reports what each gains and what
reorders (today: 20 views gain messages, 10 reorder, all named). A hand-written payload for that shape
was tried and dropped - it reproduced the *attributes* but not the behaviour, because the LangGraph
reader claims only on a `langgraph.*` marker and parses a narrower message shape than the one written
by hand, so its exemption would have asserted a cause the fixture did not exhibit.

## Capability exemptions

`PAIRING_EXEMPT` in the test names fixtures whose *source* telemetry cannot satisfy tool
pairing, with the reason. Both `claude-agent-sdk*/subagents` are listed: the Claude Code CLI
emits a subagent's tool executions without the matching `tool_use` block, so the result is
callless upstream. A capability limit of a framework is recorded per fixture rather than
weakening the check for everyone.

## `_synthetic/`

Hand-written, not captured: a Strands-shaped tool-use conversation used to exercise the
harness itself where no captured fixture is available. Its event shapes were taken from the
assertions in `server/src/domain/traces/extract/messages_tests.rs` rather than invented — an
unrealistic fixture would produce confident but meaningless results.

Real captures are preferred for every framework. Keep this one: it is the only fixture that
survives a checkout with no credentials, so it keeps the harness itself under test.

## Not committed

`crewai/agent_core` is gitignored: CrewAI serialises its entire model config into a span
attribute, so the captured payload contained a live `aws_secret_access_key` and
`aws_session_token`. A secret in a fixture goes straight into git history, where it cannot be
taken back — `capture-message-fixtures.sh` now discards any fixture whose payload matches that
shape rather than leaving the decision to a later reader.

`strands-js/image-gen` and `vercel-ai-js/image-gen` are gitignored. Those suites inline
generated images as base64 in the OTLP JSON — 7MB and 15MB for a single request — which would
sit in git history permanently for no extra parsing coverage. The Python `image_gen` fixtures
exercise the same path in under 100KB each, because media is rewritten to file URIs before
storage. Capture the JS ones locally when working on image handling; the harness discovers
whatever is present and skips the rest. `capture-message-fixtures.sh` prints a warning for any
payload over 1MB so the next such case is a decision rather than a surprise.
