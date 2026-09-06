use std::sync::Arc;

use chrono::{DateTime, Utc};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, ContentBlock, GetPromptResult, Implementation, PromptMessage, Role,
    ServerCapabilities, ServerInfo,
};
use rmcp::{ServerHandler, prompt, prompt_handler, prompt_router, tool, tool_handler, tool_router};

use crate::api::routes::otel::messages::{build_messages_response, scope_feed_to_trace};
use crate::api::routes::otel::sessions::session_row_to_summary;
use crate::api::routes::otel::stats::stats_result_to_dto;
use crate::api::routes::otel::traces::{MAX_SPANS_PER_TRACE, trace_row_to_summary};
use crate::api::routes::otel::types::SpanEnvelopeDto;
use crate::api::routes::otel::types::{
    SessionSummaryDto, SpanDetailDto, SpanSummaryDto, TraceDetailDto, TraceSummaryDto,
};
use crate::api::types::{MAX_PAGE_LIMIT, OrderBy, OrderDirection};
use crate::data::AnalyticsService;
use crate::data::traits::AnalyticsRepository;
use crate::data::types::{
    ListSessionsParams, ListSpansParams, ListTracesParams, MessageQueryParams, SpanRow, StatsParams,
};
use crate::domain::sideml::{FeedOptions, extract_tools_from_rows, process_spans};

use super::types::*;

type McpError = rmcp::model::ErrorData;

#[derive(Clone)]
pub struct McpServer {
    analytics: Arc<AnalyticsService>,
    project_id: String,
}

impl McpServer {
    pub fn new(analytics: Arc<AnalyticsService>, project_id: String) -> Self {
        // rmcp 3: #[tool_router] / #[prompt_router] generate associated functions that
        // #[tool_handler] / #[prompt_handler] call themselves, so the routers are no
        // longer stored on the struct.
        Self {
            analytics,
            project_id,
        }
    }
}

#[tool_handler]
#[prompt_handler]
impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        // rmcp 3 marks these #[non_exhaustive], so they are assembled through builders
        // and field assignment rather than struct expressions.
        let mut info = ServerInfo::default();
        info.instructions = Some(INSTRUCTIONS.to_string());
        info.capabilities = ServerCapabilities::builder()
            .enable_tools()
            .enable_prompts()
            .build();
        info.server_info = Implementation::new("SideSeat", env!("CARGO_PKG_VERSION"));
        info
    }
}

const INSTRUCTIONS: &str = r#"SideSeat AI Observability - query LLM traces, conversations, and performance data.

WORKFLOW for prompt optimization:
1. list_traces to find relevant traces (filter by session, time, errors)
2. get_messages with trace_id to see the full conversation
3. get_stats for cost/token/latency analysis
4. list_spans with observation_type=Generation for specific LLM calls
5. get_raw_span for raw OTLP data when debugging

KEY CONCEPTS:
- Trace: one end-to-end AI operation (may contain multiple LLM calls)
- Span: single operation within a trace (Generation=LLM call, Tool=tool exec, Agent=agent step)
- Session: multi-turn conversation spanning multiple traces
- Messages: normalized conversation with roles: system, user, assistant, tool

TIPS:
- Start with list_traces(limit=5) for recent activity
- get_messages shows exactly what prompts were sent and responses received
- Filter list_spans by model/framework to compare across providers
- get_stats shows cost breakdown by model for optimization decisions"#;

#[tool_router]
impl McpServer {
    #[tool(
        description = "List recent AI traces. Returns trace name, duration, tokens, costs, I/O previews, error status."
    )]
    async fn list_traces(
        &self,
        Parameters(input): Parameters<ListTracesInput>,
    ) -> Result<CallToolResult, McpError> {
        let repo = self.analytics.repository();
        let params = ListTracesParams {
            project_id: self.project_id.clone(),
            page: clamp_page(input.page),
            limit: clamp_limit(input.limit),
            order_by: Some(OrderBy {
                column: "start_time".into(),
                direction: OrderDirection::Desc,
            }),
            session_id: input.session_id,
            environment: input.environment.map(|e| vec![e]),
            from_timestamp: parse_opt_ts(input.from_timestamp),
            to_timestamp: parse_opt_ts(input.to_timestamp),
            ..Default::default()
        };
        let (rows, total) = repo.list_traces(&params).await.map_err(mcp_err)?;
        let traces: Vec<TraceSummaryDto> = rows.into_iter().map(trace_row_to_summary).collect();
        ok_json(&serde_json::json!({ "traces": traces, "total": total }))
    }

    #[tool(
        description = "Get trace execution structure: span tree with agent steps, LLM calls, tool invocations, timing, models, tokens."
    )]
    async fn get_trace(
        &self,
        Parameters(input): Parameters<GetTraceInput>,
    ) -> Result<CallToolResult, McpError> {
        let repo = self.analytics.repository();
        let trace = repo
            .get_trace(&self.project_id, &input.trace_id)
            .await
            .map_err(mcp_err)?
            .ok_or_else(|| McpError::invalid_params("trace not found", None))?;

        let spans = repo
            .get_spans_for_trace(&self.project_id, &input.trace_id)
            .await
            .map_err(mcp_err)?;

        let span_details: Vec<SpanDetailDto> = spans_to_dtos(
            &*repo,
            &self.project_id,
            &spans[..spans.len().min(MAX_SPANS_PER_TRACE)],
            false,
        )
        .await?
        .into_iter()
        .map(|summary| SpanDetailDto { summary })
        .collect();

        let summary = trace_row_to_summary(trace);
        ok_json(&TraceDetailDto {
            summary,
            spans: span_details,
        })
    }

    #[tool(
        description = "Get normalized LLM conversation. Returns messages with roles (system/user/assistant/tool), content blocks (text, tool_use, tool_result, thinking), tokens, costs. Provide trace_id, or session_id, or span_id together with its trace_id (a span id is unique only within a trace, and a session id will not do instead)."
    )]
    async fn get_messages(
        &self,
        Parameters(input): Parameters<GetMessagesInput>,
    ) -> Result<CallToolResult, McpError> {
        let repo = self.analytics.repository();
        let options = FeedOptions::new().with_role(input.role);

        // Simple path: span or session scoped (no cross-trace dedup needed)
        if input.span_id.is_some() || input.session_id.is_some() {
            // A span id is 8 bytes and unique only within a trace, so it needs its trace to identify
            // a span at all. The HTTP route always carries both; a caller here could send the span
            // alone, and the answer was then whatever spans in the project happened to share that id
            // - two traces' messages merged into one conversation, with nothing saying so. Declining
            // is better than answering a question that has more than one answer.
            if span_lacks_its_trace(input.span_id.as_deref(), input.trace_id.as_deref()) {
                return Err(McpError::invalid_params(
                    "span_id identifies a span only within a trace, because a span id is 8 bytes \
                     and traces reuse them. Pass trace_id as well - list_spans and get_trace both \
                     return it - or ask by trace_id or session_id instead.",
                    None,
                ));
            }

            // Not applied to a session query - a session spans several traces, and adding one
            // would return part of it while the response still claims to be the session.
            let trace_id = input.span_id.as_ref().and(input.trace_id.clone());
            let params = MessageQueryParams {
                project_id: self.project_id.clone(),
                span_id: input.span_id,
                session_id: input.session_id,
                trace_id,
                ..Default::default()
            };
            let result = repo.get_messages(&params).await.map_err(mcp_err)?;
            let envelopes: Vec<SpanEnvelopeDto> =
                result.rows.iter().map(SpanEnvelopeDto::from_row).collect();
            let processed = process_spans(result.rows, &options);
            // A session's totals come from the session, as the HTTP endpoint takes them: the
            // pipeline only ever saw rows carrying messages, tools or an error, so a span billed
            // with nothing to show counted as free, and nothing applied the parent/child billing
            // dedup that keeps a nested generation from counting twice. A span view has one span,
            // where neither applies.
            let session_totals = match &params.session_id {
                Some(session_id) if params.span_id.is_none() => repo
                    .get_session(&self.project_id, session_id)
                    .await
                    .map_err(mcp_err)?
                    .map(|s| (s.total_tokens, s.total_cost)),
                _ => None,
            };
            return ok_json(&build_messages_response(
                processed,
                session_totals,
                envelopes,
            ));
        }

        // Trace path: session-aware loading for cross-trace dedup
        let trace_id = input.trace_id.ok_or_else(|| {
            McpError::invalid_params("provide trace_id, span_id, or session_id", None)
        })?;

        let trace = repo
            .get_trace(&self.project_id, &trace_id)
            .await
            .map_err(mcp_err)?;
        let session_id = trace
            .as_ref()
            .and_then(|t| t.session_id.as_ref())
            .filter(|s| !s.is_empty());

        let params = MessageQueryParams {
            project_id: self.project_id.clone(),
            session_id: session_id.map(|s| s.to_string()),
            trace_id: if session_id.is_none() {
                Some(trace_id.clone())
            } else {
                None
            },
            ..Default::default()
        };
        let result = repo.get_messages(&params).await.map_err(mcp_err)?;

        let scoped_tools = session_id.map(|_| {
            extract_tools_from_rows(result.rows.iter().filter(|r| r.trace_id == trace_id))
        });
        // Envelope scope follows the view: the query loads the whole session so cross-trace
        // stripping can run, and the caller asked about one trace.
        let envelopes: Vec<SpanEnvelopeDto> = result
            .rows
            .iter()
            .filter(|r| r.trace_id == trace_id)
            .map(SpanEnvelopeDto::from_row)
            .collect();

        let mut processed = process_spans(result.rows, &options);

        if let Some(scoped_tools) = scoped_tools {
            scope_feed_to_trace(&mut processed, scoped_tools, &trace_id);
        }

        let trace_totals = trace.map(|t| (t.total_tokens, t.total_cost));
        ok_json(&build_messages_response(processed, trace_totals, envelopes))
    }

    #[tool(
        description = "Search operations across traces. Filter by observation_type (Generation=LLM, Tool=tool exec, Agent=agent step), model, framework, error status."
    )]
    async fn list_spans(
        &self,
        Parameters(input): Parameters<ListSpansInput>,
    ) -> Result<CallToolResult, McpError> {
        let repo = self.analytics.repository();
        let params = ListSpansParams {
            project_id: self.project_id.clone(),
            page: clamp_page(input.page),
            limit: clamp_limit(input.limit),
            order_by: Some(OrderBy {
                column: "timestamp_start".into(),
                direction: OrderDirection::Desc,
            }),
            trace_id: input.trace_id,
            session_id: input.session_id,
            observation_type: input.observation_type,
            framework: input.framework,
            gen_ai_request_model: input.model,
            status_code: input.status_code,
            from_timestamp: parse_opt_ts(input.from_timestamp),
            to_timestamp: parse_opt_ts(input.to_timestamp),
            ..Default::default()
        };
        let (rows, total) = repo.list_spans(&params).await.map_err(mcp_err)?;
        let spans = spans_to_dtos(&*repo, &self.project_id, &rows, false).await?;
        ok_json(&serde_json::json!({ "spans": spans, "total": total }))
    }

    #[tool(
        description = "Get raw OTLP span data: all attributes, events, resource metadata. For debugging framework-specific behavior."
    )]
    async fn get_raw_span(
        &self,
        Parameters(input): Parameters<GetRawSpanInput>,
    ) -> Result<CallToolResult, McpError> {
        let repo = self.analytics.repository();
        let span = repo
            .get_span(&self.project_id, &input.trace_id, &input.span_id)
            .await
            .map_err(mcp_err)?
            .ok_or_else(|| McpError::invalid_params("span not found", None))?;

        let dtos =
            spans_to_dtos(&*repo, &self.project_id, std::slice::from_ref(&span), true).await?;
        ok_json(&SpanDetailDto {
            summary: dtos.into_iter().next().unwrap(),
        })
    }

    #[tool(
        description = "List multi-turn sessions. Each groups related traces across user interactions. Returns summaries with counts, tokens, costs."
    )]
    async fn list_sessions(
        &self,
        Parameters(input): Parameters<ListSessionsInput>,
    ) -> Result<CallToolResult, McpError> {
        let repo = self.analytics.repository();
        let params = ListSessionsParams {
            project_id: self.project_id.clone(),
            page: clamp_page(input.page),
            limit: clamp_limit(input.limit),
            order_by: Some(OrderBy {
                column: "start_time".into(),
                direction: OrderDirection::Desc,
            }),
            user_id: input.user_id,
            environment: input.environment.map(|e| vec![e]),
            from_timestamp: parse_opt_ts(input.from_timestamp),
            to_timestamp: parse_opt_ts(input.to_timestamp),
            ..Default::default()
        };
        let (rows, total) = repo.list_sessions(&params).await.map_err(mcp_err)?;
        let sessions: Vec<SessionSummaryDto> =
            rows.into_iter().map(session_row_to_summary).collect();
        ok_json(&serde_json::json!({ "sessions": sessions, "total": total }))
    }

    #[tool(
        description = "Project analytics for a time period: costs and tokens by model/framework, trace/session/span counts, trends, avg latency."
    )]
    async fn get_stats(
        &self,
        Parameters(input): Parameters<GetStatsInput>,
    ) -> Result<CallToolResult, McpError> {
        let from_ts = parse_ts(&input.from_timestamp)
            .ok_or_else(|| McpError::invalid_params("invalid from_timestamp", None))?;
        let to_ts = parse_ts(&input.to_timestamp)
            .ok_or_else(|| McpError::invalid_params("invalid to_timestamp", None))?;

        if from_ts >= to_ts {
            return Err(McpError::invalid_params(
                "from_timestamp must be before to_timestamp",
                None,
            ));
        }
        if (to_ts - from_ts).num_days() > 90 {
            return Err(McpError::invalid_params(
                "time range cannot exceed 90 days",
                None,
            ));
        }

        let params = StatsParams {
            project_id: self.project_id.clone(),
            from_timestamp: from_ts,
            to_timestamp: to_ts,
            timezone: input.timezone,
        };
        let repo = self.analytics.repository();
        let result = repo.get_project_stats(&params).await.map_err(mcp_err)?;
        ok_json(&stats_result_to_dto(result, from_ts, to_ts))
    }
}

#[prompt_router]
impl McpServer {
    #[prompt(
        description = "Get setup instructions for integrating SideSeat telemetry. Specify a framework for tailored code examples (SDK one-liner + direct OTLP fallback)."
    )]
    async fn setup_guide(&self, Parameters(args): Parameters<SetupGuideArgs>) -> GetPromptResult {
        let content = build_setup_guide(&self.project_id, args.framework.as_deref());
        let mut result = GetPromptResult::new(vec![PromptMessage::new_text(Role::User, content)]);
        result.description = Some("SideSeat integration guide".to_string());
        result
    }
}

/// Fetch event/link counts and build SpanSummaryDto for a slice of spans.
async fn spans_to_dtos(
    repo: &(dyn AnalyticsRepository + Send + Sync),
    project_id: &str,
    spans: &[SpanRow],
    include_raw: bool,
) -> Result<Vec<SpanSummaryDto>, McpError> {
    let span_keys: Vec<(String, String)> = spans
        .iter()
        .map(|r| (r.trace_id.clone(), r.span_id.clone()))
        .collect();
    let counts = repo
        .get_span_counts_bulk(project_id, &span_keys)
        .await
        .map_err(mcp_err)?;

    Ok(spans
        .iter()
        .map(|span| {
            let key = (span.trace_id.clone(), span.span_id.clone());
            let c = counts.get(&key);
            SpanSummaryDto::from_row(
                span,
                c.map(|c| c.event_count).unwrap_or(0),
                c.map(|c| c.link_count).unwrap_or(0),
                include_raw,
            )
        })
        .collect())
}

/// Which ecosystem a framework belongs to. Determines whether the guide emits
/// pip/Python or npm/TypeScript instructions.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Lang {
    Python,
    TypeScript,
}

#[derive(Copy, Clone)]
struct FrameworkSetup {
    display: &'static str,
    lang: Lang,
    /// Package to install alongside the SDK. pip name for Python, npm name for TypeScript.
    pip_pkg: &'static str,
    /// Optional-dependency extra the SDK needs for this framework's instrumentation.
    /// Without it the import fails, `instrument()` logs a warning and returns false, and
    /// the app runs with no spans at all - so the SDK install line must carry it.
    sdk_extra: &'static str,
    sdk_variant: &'static str,
    sdk_snippet: &'static str,
    no_sdk_extra_pkgs: &'static str,
    no_sdk_extra_setup: &'static str,
}

/// Module scope rather than inside `get_framework` so tests can iterate the whole table.
/// A hardcoded list in a test only covers the entries someone remembered to add to it.
const FRAMEWORKS: &[FrameworkSetup] = &[
    FrameworkSetup {
        display: "Strands Agents",
        lang: Lang::Python,
        pip_pkg: "strands-agents",
        sdk_extra: "",
        sdk_variant: "Strands",
        sdk_snippet: "from strands import Agent\n\nagent = Agent()\nprint(agent(\"Hello\"))",
        no_sdk_extra_pkgs: "",
        no_sdk_extra_setup: "",
    },
    FrameworkSetup {
        display: "LangChain",
        lang: Lang::Python,
        pip_pkg: "langchain-openai",
        sdk_extra: "langchain",
        sdk_variant: "LangChain",
        sdk_snippet: "from langchain_openai import ChatOpenAI\nllm = ChatOpenAI(model=\"gpt-4o-mini\")\nprint(llm.invoke(\"Hello\").content)",
        no_sdk_extra_pkgs: "openinference-instrumentation-langchain",
        no_sdk_extra_setup: "from openinference.instrumentation.langchain import LangChainInstrumentor\nLangChainInstrumentor().instrument(tracer_provider=provider, skip_dep_check=True)",
    },
    FrameworkSetup {
        display: "LangGraph",
        lang: Lang::Python,
        pip_pkg: "langgraph langchain-openai",
        sdk_extra: "langgraph",
        sdk_variant: "LangGraph",
        sdk_snippet: "from langgraph.prebuilt import create_react_agent\nfrom langchain_openai import ChatOpenAI\nagent = create_react_agent(ChatOpenAI(model=\"gpt-4o-mini\"), [])\nprint(agent.invoke({\"messages\": [(\"user\", \"Hello\")]}))",
        no_sdk_extra_pkgs: "openinference-instrumentation-langchain",
        no_sdk_extra_setup: "from openinference.instrumentation.langchain import LangChainInstrumentor\nLangChainInstrumentor().instrument(tracer_provider=provider, skip_dep_check=True)",
    },
    FrameworkSetup {
        display: "CrewAI",
        lang: Lang::Python,
        pip_pkg: "crewai",
        sdk_extra: "crewai",
        sdk_variant: "CrewAI",
        sdk_snippet: "from crewai import Agent, Task, Crew\na = Agent(role=\"R\", goal=\"G\", backstory=\"B\")\nt = Task(description=\"D\", expected_output=\"O\", agent=a)\nprint(Crew(agents=[a], tasks=[t]).kickoff())",
        no_sdk_extra_pkgs: "openinference-instrumentation-crewai",
        no_sdk_extra_setup: "from openinference.instrumentation.crewai import CrewAIInstrumentor\nCrewAIInstrumentor().instrument(tracer_provider=provider, skip_dep_check=True)",
    },
    FrameworkSetup {
        display: "AutoGen",
        lang: Lang::Python,
        // autogen_ext.models.openai lives in autogen-ext, and the openai extra is what
        // pulls its OpenAI client dependencies.
        pip_pkg: "autogen-agentchat \"autogen-ext[openai]\"",
        sdk_extra: "autogen",
        sdk_variant: "AutoGen",
        sdk_snippet: "import asyncio\nfrom autogen_agentchat.agents import AssistantAgent\nfrom autogen_ext.models.openai import OpenAIChatCompletionClient\nagent = AssistantAgent(\"a\", model_client=OpenAIChatCompletionClient(model=\"gpt-4o-mini\"))\nasyncio.run(agent.run(task=\"Hello\"))",
        no_sdk_extra_pkgs: "openinference-instrumentation-autogen-agentchat",
        no_sdk_extra_setup: "from openinference.instrumentation.autogen_agentchat import AutogenAgentChatInstrumentor\nAutogenAgentChatInstrumentor().instrument(tracer_provider=provider, skip_dep_check=True)",
    },
    FrameworkSetup {
        display: "OpenAI Agents SDK",
        lang: Lang::Python,
        pip_pkg: "openai-agents",
        sdk_extra: "openai-agents",
        sdk_variant: "OpenAIAgents",
        sdk_snippet: "from agents import Agent, Runner\nprint(Runner.run_sync(Agent(name=\"A\", instructions=\"Helpful.\"), \"Hello\").final_output)",
        no_sdk_extra_pkgs: "\"logfire>=4.29.0\"",
        no_sdk_extra_setup: "import logfire\nlogfire.configure(send_to_logfire=False, console=False)\nlogfire.instrument_openai_agents()",
    },
    FrameworkSetup {
        display: "PydanticAI",
        lang: Lang::Python,
        pip_pkg: "pydantic-ai",
        sdk_extra: "pydantic-ai",
        sdk_variant: "PydanticAI",
        sdk_snippet: "from pydantic_ai import Agent\nprint(Agent(\"openai:gpt-4o-mini\").run_sync(\"Hello\").output)",
        no_sdk_extra_pkgs: "logfire[pydantic-ai]",
        no_sdk_extra_setup: "import logfire\nlogfire.configure(send_to_logfire=False, console=False)\nlogfire.instrument_pydantic_ai()",
    },
    FrameworkSetup {
        display: "Google ADK",
        lang: Lang::Python,
        pip_pkg: "google-adk",
        sdk_extra: "",
        sdk_variant: "GoogleADK",
        sdk_snippet: "import asyncio\n\nfrom google.adk.agents import LlmAgent\nfrom google.adk.runners import Runner\nfrom google.adk.sessions import InMemorySessionService\nfrom google.genai import types\n\nagent = LlmAgent(model=\"gemini-2.0-flash\", name=\"assistant\", instruction=\"Be helpful.\")\n\nasync def main():\n    sessions = InMemorySessionService()\n    await sessions.create_session(app_name=\"demo\", user_id=\"u1\", session_id=\"s1\")\n    runner = Runner(agent=agent, app_name=\"demo\", session_service=sessions)\n    async for event in runner.run_async(\n        session_id=\"s1\",\n        user_id=\"u1\",\n        new_message=types.Content(role=\"user\", parts=[types.Part(text=\"Hello\")]),\n    ):\n        if event.content and event.content.parts:\n            for part in event.content.parts:\n                if getattr(part, \"text\", None):\n                    print(part.text)\n\nasyncio.run(main())",
        no_sdk_extra_pkgs: "",
        no_sdk_extra_setup: "",
    },
    FrameworkSetup {
        display: "Microsoft Agent Framework",
        lang: Lang::Python,
        pip_pkg: "agent-framework",
        sdk_extra: "",
        sdk_variant: "AgentFramework",
        sdk_snippet: "import asyncio\nfrom agent_framework import Agent\nfrom agent_framework.openai import OpenAIChatClient\nprint(asyncio.run(Agent(client=OpenAIChatClient(model=\"gpt-5-nano-2025-08-07\"), instructions=\"Helpful.\").run(\"Hello\")).text)",
        no_sdk_extra_pkgs: "",
        no_sdk_extra_setup: "from agent_framework.observability import OBSERVABILITY_SETTINGS\nOBSERVABILITY_SETTINGS.enable_instrumentation = True\nOBSERVABILITY_SETTINGS.enable_sensitive_data = True",
    },
    FrameworkSetup {
        display: "Amazon Bedrock",
        lang: Lang::Python,
        pip_pkg: "boto3",
        sdk_extra: "aws",
        sdk_variant: "Bedrock",
        sdk_snippet: "import boto3\nr = boto3.client(\"bedrock-runtime\", region_name=\"us-east-1\").converse(modelId=\"anthropic.claude-haiku-4-5-20251001-v1:0\", messages=[{\"role\": \"user\", \"content\": [{\"text\": \"Hello\"}]}])\nprint(r[\"output\"][\"message\"][\"content\"][0][\"text\"])",
        no_sdk_extra_pkgs: "opentelemetry-instrumentation-botocore",
        no_sdk_extra_setup: "from opentelemetry.instrumentation.botocore import BotocoreInstrumentor\nBotocoreInstrumentor().instrument(tracer_provider=provider)",
    },
    FrameworkSetup {
        display: "Claude Agent SDK",
        lang: Lang::Python,
        pip_pkg: "claude-agent-sdk",
        sdk_extra: "",
        sdk_variant: "ClaudeAgentSDK",
        sdk_snippet: "import asyncio\nfrom claude_agent_sdk import query, ClaudeAgentOptions\n\n# The Agent SDK emits no telemetry itself: it spawns the Claude Code CLI, which\n# carries the OTel instrumentation and is configured via these env vars.\nOTEL_ENV = {\n    \"CLAUDE_CODE_ENABLE_TELEMETRY\": \"1\",\n    # Span tracing is beta and off without this flag.\n    \"CLAUDE_CODE_ENHANCED_TELEMETRY_BETA\": \"1\",\n    # Second beta tier. Without these two the message feed stays empty:\n    # assistant reply text exists nowhere else on the trace.\n    \"ENABLE_BETA_TRACING_DETAILED\": \"1\",\n    \"BETA_TRACING_ENDPOINT\": \"__OTLP_BASE__\",\n    # Never \"console\": the CLI writes telemetry to stdout, which is the SDK's\n    # message channel, and would corrupt the stream.\n    \"OTEL_TRACES_EXPORTER\": \"otlp\",\n    \"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL\": \"http/protobuf\",\n    \"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT\": \"__OTLP_ENDPOINT__\",\n    # Content is redacted by default, leaving the message feed empty.\n    \"OTEL_LOG_USER_PROMPTS\": \"1\",\n    \"OTEL_LOG_TOOL_DETAILS\": \"1\",\n}\n\nasync def main():\n    options = ClaudeAgentOptions(env=OTEL_ENV, allowed_tools=[\"Read\", \"Glob\"])\n    async for message in query(prompt=\"What is 2+2?\", options=options):\n        print(message)\n\nasyncio.run(main())",
        no_sdk_extra_pkgs: "",
        no_sdk_extra_setup: "",
    },
    FrameworkSetup {
        display: "Anthropic",
        lang: Lang::Python,
        pip_pkg: "anthropic",
        sdk_extra: "anthropic",
        sdk_variant: "Anthropic",
        sdk_snippet: "import anthropic\nprint(anthropic.Anthropic().messages.create(model=\"claude-haiku-4-5-20251001\", max_tokens=256, messages=[{\"role\": \"user\", \"content\": \"Hello\"}]).content[0].text)",
        no_sdk_extra_pkgs: "logfire[anthropic]",
        no_sdk_extra_setup: "import logfire\nlogfire.configure(send_to_logfire=False, console=False)\nlogfire.instrument_anthropic()",
    },
    FrameworkSetup {
        display: "OpenAI",
        lang: Lang::Python,
        pip_pkg: "openai",
        sdk_extra: "openai",
        sdk_variant: "OpenAI",
        sdk_snippet: "from openai import OpenAI\nprint(OpenAI().chat.completions.create(model=\"gpt-4o-mini\", messages=[{\"role\": \"user\", \"content\": \"Hello\"}]).choices[0].message.content)",
        no_sdk_extra_pkgs: "logfire[openai]",
        no_sdk_extra_setup: "import logfire\nlogfire.configure(send_to_logfire=False, console=False)\nlogfire.instrument_openai()",
    },
    FrameworkSetup {
        display: "Google Gemini",
        lang: Lang::Python,
        pip_pkg: "google-genai",
        sdk_extra: "google-genai",
        sdk_variant: "GoogleGenAI",
        sdk_snippet: "from google import genai\nprint(genai.Client(api_key=\"YOUR_KEY\").models.generate_content(model=\"gemini-2.5-flash\", contents=\"Hello\").text)",
        no_sdk_extra_pkgs: "logfire[google-genai]",
        no_sdk_extra_setup: "import logfire\nlogfire.configure(send_to_logfire=False, console=False)\nlogfire.instrument_google_genai()",
    },
    FrameworkSetup {
        display: "Google Vertex AI",
        lang: Lang::Python,
        pip_pkg: "google-cloud-aiplatform vertexai",
        sdk_extra: "vertex-ai",
        sdk_variant: "VertexAI",
        sdk_snippet: "import vertexai\nfrom vertexai.generative_models import GenerativeModel\nvertexai.init(project=\"PROJECT_ID\", location=\"us-central1\")\nprint(GenerativeModel(\"gemini-2.5-flash\").generate_content(\"Hello\").text)",
        no_sdk_extra_pkgs: "opentelemetry-instrumentation-vertexai",
        no_sdk_extra_setup: "from opentelemetry.instrumentation.vertexai import VertexAIInstrumentor\nVertexAIInstrumentor().instrument(tracer_provider=provider)",
    },
    FrameworkSetup {
        display: "Azure OpenAI",
        lang: Lang::Python,
        pip_pkg: "openai",
        // Azure OpenAI is reached through the OpenAI SDK, so it reuses the openai
        // extra and Frameworks.OpenAI - there is no separate SDK constant for it.
        sdk_extra: "openai",
        sdk_variant: "OpenAI",
        sdk_snippet: "from openai import AzureOpenAI\n\nazure = AzureOpenAI(\n    api_key=\"your-api-key\",\n    api_version=\"2024-02-01\",\n    azure_endpoint=\"https://your-resource.openai.azure.com\",\n)\nresponse = azure.chat.completions.create(\n    model=\"gpt-5-mini\",\n    messages=[{\"role\": \"user\", \"content\": \"Hello\"}],\n)\nprint(response.choices[0].message.content)",
        no_sdk_extra_pkgs: "logfire",
        no_sdk_extra_setup: "import logfire\nlogfire.configure(send_to_logfire=False, console=False)\nlogfire.instrument_openai()",
    },
    FrameworkSetup {
        display: "Agno",
        lang: Lang::Python,
        pip_pkg: "agno openai",
        sdk_extra: "agno",
        sdk_variant: "Agno",
        sdk_snippet: "from agno.agent import Agent\nfrom agno.models.openai import OpenAIChat\n\nagent = Agent(model=OpenAIChat(id=\"gpt-5-mini\"))\nagent.print_response(\"Hello\")",
        no_sdk_extra_pkgs: "openinference-instrumentation-agno",
        no_sdk_extra_setup: "from openinference.instrumentation.agno import AgnoInstrumentor\nAgnoInstrumentor().instrument(tracer_provider=provider)",
    },
    FrameworkSetup {
        display: "Smolagents",
        lang: Lang::Python,
        pip_pkg: "smolagents",
        sdk_extra: "smolagents",
        sdk_variant: "Smolagents",
        sdk_snippet: "from smolagents import CodeAgent, InferenceClientModel\n\nagent = CodeAgent(tools=[], model=InferenceClientModel())\nprint(agent.run(\"What is 2+2?\"))",
        no_sdk_extra_pkgs: "openinference-instrumentation-smolagents",
        no_sdk_extra_setup: "from openinference.instrumentation.smolagents import SmolagentsInstrumentor\nSmolagentsInstrumentor().instrument(tracer_provider=provider)",
    },
    FrameworkSetup {
        display: "AG2",
        lang: Lang::Python,
        pip_pkg: "\"ag2[openai]<1.0\"",
        sdk_extra: "ag2",
        sdk_variant: "AG2",
        sdk_snippet: "from autogen import ConversableAgent\n\nassistant = ConversableAgent(\n    name=\"assistant\",\n    llm_config={\"model\": \"gpt-5-mini\"},\n)\nprint(assistant.generate_reply(messages=[{\"role\": \"user\", \"content\": \"Hello\"}]))",
        no_sdk_extra_pkgs: "openinference-instrumentation-autogen",
        no_sdk_extra_setup: "from openinference.instrumentation.autogen import AutogenInstrumentor\nAutogenInstrumentor().instrument(tracer_provider=provider)",
    },
    FrameworkSetup {
        display: "AgentScope",
        lang: Lang::Python,
        pip_pkg: "agentscope",
        sdk_extra: "",
        sdk_variant: "AgentScope",
        // Runnable as a script: AgentScope's agent call is async, so it needs an
        // asyncio entry point rather than a bare top-level await.
        sdk_snippet: "import asyncio\nimport os\nfrom agentscope.agent import Agent\nfrom agentscope.message import Msg, TextBlock\nfrom agentscope.model import OpenAIChatModel\n\n# AgentScope emits OpenTelemetry itself; it only needs the global provider.\nasync def main():\n    agent = Agent(\n        name=\"assistant\",\n        system_prompt=\"You are a helpful assistant.\",\n        model=OpenAIChatModel(credential=os.environ[\"OPENAI_API_KEY\"], model=\"gpt-5-mini\"),\n    )\n    reply = await agent(Msg(name=\"user\", content=[TextBlock(type=\"text\", text=\"Hello\")], role=\"user\"))\n    print(reply)\n\nasyncio.run(main())",
        no_sdk_extra_pkgs: "",
        no_sdk_extra_setup: "",
    },
    FrameworkSetup {
        display: "Langflow",
        lang: Lang::Python,
        pip_pkg: "langflow",
        sdk_extra: "",
        sdk_variant: "Langflow",
        sdk_snippet: "# Langflow emits OpenTelemetry itself; run it with the provider configured\n# in the same process, or point its OTLP exporter at SideSeat.",
        no_sdk_extra_pkgs: "",
        no_sdk_extra_setup: "",
    },
    FrameworkSetup {
        display: "Haystack",
        lang: Lang::Python,
        pip_pkg: "haystack-ai",
        sdk_extra: "haystack",
        sdk_variant: "Haystack",
        sdk_snippet: "from haystack import Pipeline\nfrom haystack.components.generators.chat import OpenAIChatGenerator\nfrom haystack.dataclasses import ChatMessage\n\npipeline = Pipeline()\npipeline.add_component(\"llm\", OpenAIChatGenerator(model=\"gpt-5-mini\"))\nresult = pipeline.run({\"llm\": {\"messages\": [ChatMessage.from_user(\"Hello\")]}})\nprint(result[\"llm\"][\"replies\"][0].text)",
        no_sdk_extra_pkgs: "openinference-instrumentation-haystack",
        no_sdk_extra_setup: "from openinference.instrumentation.haystack import HaystackInstrumentor\nHaystackInstrumentor().instrument(tracer_provider=provider)",
    },
    FrameworkSetup {
        display: "browser-use",
        lang: Lang::Python,
        pip_pkg: "browser-use",
        sdk_extra: "",
        sdk_variant: "BrowserUse",
        sdk_snippet: "import asyncio\nfrom browser_use import Agent, ChatOpenAI\n\n# browser-use emits OpenTelemetry itself and uses the global provider.\nasync def main():\n    agent = Agent(task=\"Find the docs\", llm=ChatOpenAI(model=\"gpt-5-mini\"))\n    print(await agent.run())\n\nasyncio.run(main())",
        no_sdk_extra_pkgs: "",
        no_sdk_extra_setup: "",
    },
    FrameworkSetup {
        display: "Vercel AI SDK",
        lang: Lang::TypeScript,
        pip_pkg: "ai @ai-sdk/otel @ai-sdk/amazon-bedrock",
        sdk_extra: "",
        sdk_variant: "VercelAI",
        sdk_snippet: "import { generateText, registerTelemetry } from 'ai';\n\
                          import { LegacyOpenTelemetry } from '@ai-sdk/otel';\n\
                          import { bedrock } from '@ai-sdk/amazon-bedrock';\n\n\
                          // AI SDK 7 delivers telemetry only to registered integrations.\n\
                          // Register after init(): the integration captures a tracer in its constructor.\n\
                          registerTelemetry(new LegacyOpenTelemetry());\n\n\
                          const { text } = await generateText({\n\
                          \u{20}\u{20}model: bedrock('us.anthropic.claude-sonnet-4-5-20250929-v1:0'),\n\
                          \u{20}\u{20}prompt: 'What is 2+2?',\n\
                          \u{20}\u{20}experimental_telemetry: { isEnabled: true },\n});\nconsole.log(text);",
        no_sdk_extra_pkgs: "",
        // Empty on purpose: the snippet above already imports `registerTelemetry` and
        // `LegacyOpenTelemetry` and calls them, and the no-SDK template emits
        // `{extra_setup}{snippet}` - so repeating them here produced a module that declares each
        // import twice and registers telemetry twice, which does not compile. The snippet's own
        // registration is correctly placed for both paths: after `init()` in one and after
        // `sdk.start()` in the other, because the template puts the snippet after both.
        no_sdk_extra_setup: "",
    },
    FrameworkSetup {
        display: "Strands TypeScript",
        lang: Lang::TypeScript,
        pip_pkg: "@strands-agents/sdk",
        sdk_extra: "",
        sdk_variant: "Strands",
        sdk_snippet: "import { Agent } from '@strands-agents/sdk';\n\n\
                          const agent = new Agent({\n\
                          \u{20}\u{20}model: 'global.anthropic.claude-haiku-4-5-20251001-v1:0',\n\
                          });\n\
                          const result = await agent.invoke('Hello');\n\
                          console.log(result.toString());",
        no_sdk_extra_pkgs: "",
        no_sdk_extra_setup: "",
    },
    FrameworkSetup {
        // No parentheses: the alias is derived from the display name by lowercasing and
        // replacing spaces with hyphens, so "(TypeScript)" would make it unmatchable.
        display: "Claude Agent SDK TypeScript",
        lang: Lang::TypeScript,
        pip_pkg: "@anthropic-ai/claude-agent-sdk",
        sdk_extra: "",
        sdk_variant: "ClaudeAgentSDK",
        sdk_snippet: "import { query } from '@anthropic-ai/claude-agent-sdk';\n\n\
                          // The Agent SDK emits no telemetry itself: the Claude Code CLI it spawns\n\
                          // self-instruments and is configured through CLAUDE_CODE_* / OTEL_* env vars\n\
                          // on the subprocess. See the Claude Agent SDK integration page.\n\
                          // The CLI subprocess exports OTLP itself; these are its entire\n\
                          // configuration. Span tracing is beta, and message content needs a\n\
                          // second beta tier on top or the Messages tab stays empty.\n\
                          // options.env REPLACES the environment in TypeScript, so process.env is\n\
                          // spread or the subprocess loses PATH and credentials.\n\
                          const options = {\n\
                          \u{20}\u{20}env: {\n\
                          \u{20}\u{20}\u{20}\u{20}...process.env,\n\
                          \u{20}\u{20}\u{20}\u{20}CLAUDE_CODE_ENABLE_TELEMETRY: '1',\n\
                          \u{20}\u{20}\u{20}\u{20}CLAUDE_CODE_ENHANCED_TELEMETRY_BETA: '1',\n\
                          \u{20}\u{20}\u{20}\u{20}ENABLE_BETA_TRACING_DETAILED: '1',\n\
                          \u{20}\u{20}\u{20}\u{20}BETA_TRACING_ENDPOINT: '__OTLP_BASE__',\n\
                          \u{20}\u{20}\u{20}\u{20}OTEL_TRACES_EXPORTER: 'otlp',\n\
                          \u{20}\u{20}\u{20}\u{20}OTEL_METRICS_EXPORTER: 'none',\n\
                          \u{20}\u{20}\u{20}\u{20}OTEL_LOGS_EXPORTER: 'none',\n\
                          \u{20}\u{20}\u{20}\u{20}OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: 'http/protobuf',\n\
                          \u{20}\u{20}\u{20}\u{20}OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: '__OTLP_ENDPOINT__',\n\
                          \u{20}\u{20}\u{20}\u{20}OTEL_LOG_USER_PROMPTS: '1',\n\
                          \u{20}\u{20}\u{20}\u{20}OTEL_LOG_TOOL_DETAILS: '1',\n\
                          \u{20}\u{20}},\n\
                          };\n\n\
                          for await (const msg of query({ prompt: 'Hello', options })) console.log(msg);",
        no_sdk_extra_pkgs: "",
        no_sdk_extra_setup: "",
    },
];

/// The names `get_framework` accepts, as a caller would type them.
fn supported_framework_names() -> Vec<String> {
    let mut names: Vec<String> = FRAMEWORKS
        .iter()
        .map(|f| f.display.to_lowercase().replace(' ', "-"))
        .collect();
    names.sort();
    names.dedup();
    names
}

fn get_framework(name: &str) -> Option<FrameworkSetup> {
    FRAMEWORKS
        .iter()
        .find(|f| {
            f.display.to_lowercase().replace(' ', "-") == name
                || f.sdk_variant.to_lowercase() == name
                || f.sdk_variant.to_lowercase() == name.replace('-', "")
                || f.pip_pkg.split_whitespace().any(|p| p == name)
        })
        .copied()
}

fn build_setup_guide(project_id: &str, framework: Option<&str>) -> String {
    let otlp_url = format!("http://localhost:5388/otel/{project_id}/v1/traces");

    // Snippets are inserted as values, not re-formatted, so a snippet that needs the
    // endpoint carries this placeholder and is substituted after formatting.
    let guide = build_setup_guide_template(&otlp_url, framework);
    // BETA_TRACING_ENDPOINT takes the collector base URL, not the /v1/traces path.
    let otlp_base = otlp_url.trim_end_matches("/v1/traces");
    guide
        .replace("__OTLP_ENDPOINT__", &otlp_url)
        .replace("__OTLP_BASE__", otlp_base)
}

fn build_setup_guide_template(otlp_url: &str, framework: Option<&str>) -> String {
    match framework.and_then(|f| get_framework(&f.to_lowercase())) {
        Some(fw) if fw.lang == Lang::TypeScript => {
            let extra_setup = if fw.no_sdk_extra_setup.is_empty() {
                String::new()
            } else {
                format!("{}\n\n", fw.no_sdk_extra_setup)
            };
            format!(
                "## With SideSeat SDK (recommended)\n\n\
                 ```bash\nnpm install @sideseat/sdk {npm}\n```\n\n\
                 ```typescript\nimport {{ init, Frameworks }} from '@sideseat/sdk';\n\n\
                 init({{ framework: Frameworks.{variant} }});\n\n{snippet}\n```\n\n\
                 ## Without SDK (direct OTLP)\n\n\
                 ```bash\nnpm install {npm} @opentelemetry/sdk-node @opentelemetry/exporter-trace-otlp-http\n```\n\n\
                 ```typescript\nimport {{ NodeSDK }} from '@opentelemetry/sdk-node';\n\
                 import {{ OTLPTraceExporter }} from '@opentelemetry/exporter-trace-otlp-http';\n\n\
                 const sdk = new NodeSDK({{\n\
                 \u{20}\u{20}traceExporter: new OTLPTraceExporter({{ url: '{otlp}' }}),\n}});\n\
                 sdk.start();\n\n{extra_setup}{snippet}\n```",
                npm = fw.pip_pkg,
                variant = fw.sdk_variant,
                snippet = fw.sdk_snippet,
                extra_setup = extra_setup,
                otlp = otlp_url,
            )
        }
        Some(fw) => {
            // `sideseat[extra]` is quoted: bare brackets are glob metacharacters in zsh.
            let sdk_pkg = if fw.sdk_extra.is_empty() {
                "sideseat".to_string()
            } else {
                format!("\"sideseat[{}]\"", fw.sdk_extra)
            };
            let extra_pkgs = if fw.no_sdk_extra_pkgs.is_empty() {
                String::new()
            } else {
                format!(" {}", fw.no_sdk_extra_pkgs)
            };
            format!(
                "## With SideSeat SDK (recommended)\n\n\
                 ```bash\npip install {sdk_pkg} {pip}\n```\n\n\
                 ```python\nfrom sideseat import SideSeat, Frameworks\n\
                 SideSeat(framework=Frameworks.{variant})\n\n{snippet}\n```\n\n\
                 ## Without SDK (direct OTLP)\n\n\
                 ```bash\npip install {pip} opentelemetry-sdk opentelemetry-exporter-otlp-proto-http{extra_pkgs}\n```\n\n\
                 ```python\nfrom opentelemetry import trace\n\
                 from opentelemetry.sdk.trace import TracerProvider\n\
                 from opentelemetry.sdk.trace.export import BatchSpanProcessor\n\
                 from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter\n\n\
                 provider = TracerProvider()\n\
                 provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(\n\
                     endpoint=\"{otlp}\"\n)))\n\
                 trace.set_tracer_provider(provider)\n\n\
                 {extra_setup}\n\n{snippet}\n```",
                pip = fw.pip_pkg,
                variant = fw.sdk_variant,
                snippet = fw.sdk_snippet,
                extra_pkgs = extra_pkgs,
                extra_setup = fw.no_sdk_extra_setup,
                otlp = otlp_url,
            )
        }
        None => format!(
            "## Generic OTLP Setup\n\n\
             ```bash\npip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http\n```\n\n\
             ```python\nfrom opentelemetry import trace\n\
             from opentelemetry.sdk.trace import TracerProvider\n\
             from opentelemetry.sdk.trace.export import BatchSpanProcessor\n\
             from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter\n\n\
             provider = TracerProvider()\n\
             provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(\n\
                 endpoint=\"{otlp}\"\n)))\n\
             trace.set_tracer_provider(provider)\n```\n\n\
             Supported frameworks: {frameworks}",
            otlp = otlp_url,
            // Listed from the table that answers the request, not from a second hand-written list: the
            // hand-written one named fifteen of the frameworks `get_framework` accepts and omitted the
            // rest, so a caller was told its framework was unsupported when the guide had an entry for it.
            frameworks = supported_framework_names().join(", "),
        ),
    }
}

fn ok_json(value: &impl serde::Serialize) -> Result<CallToolResult, McpError> {
    let json = serde_json::to_string(value).map_err(mcp_err)?;
    Ok(CallToolResult::success(vec![ContentBlock::text(json)]))
}

fn mcp_err(e: impl std::fmt::Display) -> McpError {
    tracing::debug!(error = %e, "MCP tool error");
    McpError::internal_error(e.to_string(), None)
}

fn clamp_page(page: Option<u32>) -> u32 {
    page.unwrap_or(1).max(1)
}

fn clamp_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(20).clamp(1, MAX_PAGE_LIMIT)
}

fn parse_opt_ts(s: Option<String>) -> Option<DateTime<Utc>> {
    crate::api::types::parse_timestamp_param(&s).ok().flatten()
}

fn parse_ts(s: &str) -> Option<DateTime<Utc>> {
    parse_opt_ts(Some(s.to_string()))
}

/// True when a request names a span but not the trace it belongs to.
///
/// A span id is 8 bytes and unique only within a trace, so on its own it can match spans in several
/// traces at once.
///
/// A session id is *not* a substitute, though it looks like one: the message query gives `span_id`
/// precedence and ignores `session_id` when both are set, so accepting the pair let exactly the
/// cross-trace merge this guard exists to stop back in - and the first version of this function
/// asserted that pair was fine.
fn span_lacks_its_trace(span_id: Option<&str>, trace_id: Option<&str>) -> bool {
    span_id.is_some() && trace_id.is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A span id alone is not a question with one answer, so the tool must decline it.
    #[test]
    fn a_span_id_without_its_trace_is_refused() {
        assert!(
            span_lacks_its_trace(Some("abc123"), None),
            "a bare span id can match spans in several traces and must be refused"
        );
        assert!(
            !span_lacks_its_trace(Some("abc123"), Some("trace-1")),
            "a span id with its trace identifies one span"
        );
        // Nothing to refuse when no span was asked for.
        assert!(!span_lacks_its_trace(None, None));
        assert!(!span_lacks_its_trace(None, Some("trace-1")));
    }

    /// A session id does not scope a span lookup, however much it looks as though it should.
    ///
    /// `get_messages` gives `span_id` precedence and never applies `session_id` in that branch, so a
    /// span id paired with a session id is still a span id with no trace - and the first version of
    /// this guard accepted the pair, with a test that said so.
    #[test]
    fn a_session_id_does_not_stand_in_for_the_trace() {
        assert!(
            span_lacks_its_trace(Some("abc123"), None),
            "span + session must be refused: the query ignores the session when a span is given"
        );
    }

    /// Every name the generic guide advertises is a name the guide can actually serve.
    ///
    /// It listed fifteen frameworks from a hand-written string while `get_framework` accepted every entry in
    /// the table, so a caller whose framework *was* supported read that it was not. Derived from the table
    /// now, and checked in both directions.
    #[test]
    fn the_advertised_framework_names_are_the_ones_the_guide_serves() {
        let names = supported_framework_names();
        assert!(names.len() > 20, "the table was not read: {names:?}");
        for name in &names {
            assert!(
                get_framework(name).is_some(),
                "the guide advertises `{name}` but cannot resolve it"
            );
        }
        for fw in FRAMEWORKS {
            let name = fw.display.to_lowercase().replace(' ', "-");
            assert!(
                names.contains(&name),
                "{} has an entry but is not advertised",
                fw.display
            );
        }
    }

    /// A framework's direct-OTLP instrumentation reads the same here as in the telemetry UI.
    ///
    /// The two are written by hand in different languages and are the setup a user copies, so drift between
    /// them is a false instruction rather than a cosmetic difference: AutoGen's UI snippet omitted
    /// `skip_dep_check=True`, which the MCP guide and the framework page both pass, and without it the
    /// instrumentor refuses versions that work. Compared per *instrumentor*, not as whole sets, because the
    /// two surfaces deliberately cover different framework lists - the UI has entries with no MCP twin.
    #[test]
    fn a_shared_instrumentor_line_reads_the_same_in_the_telemetry_ui() {
        let ui = include_str!("../../../../web/src/pages/configuration/telemetry-frameworks.ts");

        /// The instrumentor's name and the whole call, from any line that instruments one.
        fn calls(source: &str) -> std::collections::HashMap<String, String> {
            let mut found = std::collections::HashMap::new();
            for line in source.lines() {
                // Both, in any order: a TS line ends `...)`,` and a Rust one `...)",`.
                let line = line.trim().trim_end_matches(['`', ',', '"', ';']);
                let Some(open) = line.find("Instrumentor().instrument(") else {
                    continue;
                };
                // The name is the identifier ending at `Instrumentor`, back to the last non-word char.
                let head = &line[..open + "Instrumentor".len()];
                let start = head
                    .rfind(|c: char| !c.is_alphanumeric() && c != '_')
                    .map(|i| i + 1)
                    .unwrap_or(0);
                let name = head[start..].to_string();
                let call = line[start..].trim_end_matches("\\n").to_string();
                found.insert(name, call);
            }
            found
        }

        let mcp: std::collections::HashMap<String, String> = FRAMEWORKS
            .iter()
            .flat_map(|fw| calls(fw.no_sdk_extra_setup))
            .collect();
        let ui_calls = calls(ui);
        assert!(
            mcp.len() > 3 && ui_calls.len() > 3,
            "nothing was parsed, so this test proves nothing: mcp={:?} ui={:?}",
            mcp.keys(),
            ui_calls.keys()
        );

        for (name, mcp_call) in &mcp {
            if let Some(ui_call) = ui_calls.get(name) {
                assert_eq!(
                    mcp_call, ui_call,
                    "{name} is instrumented differently in the MCP setup guide and the telemetry UI; \
                     one of them is telling users the wrong thing"
                );
            }
        }
    }

    /// The extra setup and the snippet are concatenated, so no line may appear in both.
    ///
    /// Vercel's entry declared `registerTelemetry` and `LegacyOpenTelemetry` in *both*, and the no-SDK
    /// template emits `{extra_setup}{snippet}` - so the module it generated imported each twice and
    /// registered telemetry twice, which does not compile. Nothing caught it: the guide is assembled from
    /// strings, so a redeclaration is only visible to whoever pastes it. A line-level comparison is the
    /// whole check that was missing.
    #[test]
    fn the_extra_setup_never_repeats_a_line_of_the_snippet() {
        for fw in FRAMEWORKS {
            if fw.no_sdk_extra_setup.is_empty() {
                continue;
            }
            let snippet_lines: std::collections::HashSet<&str> = fw
                .sdk_snippet
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.starts_with("//") && !l.starts_with('#'))
                .collect();
            for line in fw.no_sdk_extra_setup.lines().map(str::trim) {
                if line.is_empty() || line.starts_with("//") || line.starts_with('#') {
                    continue;
                }
                assert!(
                    !snippet_lines.contains(line),
                    "{}: `{line}` is in both no_sdk_extra_setup and sdk_snippet, and the guide \
                     concatenates them - the generated code declares it twice",
                    fw.display
                );
            }
        }
    }

    #[test]
    fn test_python_snippets_have_no_top_level_await() {
        // A bare top-level `await` is a SyntaxError when the snippet is saved and run as a
        // script, which is exactly how a user consumes this guide.
        for name in [
            "strands",
            "langgraph",
            "crewai",
            "autogen",
            "agno",
            "smolagents",
            "ag2",
            "agentscope",
            "langflow",
            "haystack",
            "browser-use",
            "bedrock",
            "anthropic",
            "openai",
            "google-adk",
            "openai-agents",
            "pydantic-ai",
        ] {
            let guide =
                build_setup_guide_template("http://localhost:5388/otel/default", Some(name));
            for line in guide.lines() {
                let t = line.trim_start();
                let indented = line.len() != t.len();
                if (t.starts_with("await ") || t.contains("= await ")) && !indented {
                    panic!("{name}: top-level await is not runnable as a script:\n  {line}");
                }
            }
        }
    }

    #[test]
    fn test_typescript_frameworks_emit_npm_instructions() {
        // The guide used to be Python-only, so a TypeScript framework silently produced
        // pip instructions. Both paths must be present and in the right ecosystem.
        for name in [
            "vercel-ai",
            "strands-typescript",
            "claude-agent-sdk-typescript",
        ] {
            let guide =
                build_setup_guide_template("http://localhost:5388/otel/default", Some(name));
            assert!(
                guide.contains("npm install @sideseat/sdk"),
                "{name} should emit an npm SDK install, got:\n{guide}"
            );
            assert!(
                guide.contains("```typescript"),
                "{name} should emit TypeScript snippets, got:\n{guide}"
            );
            assert!(
                !guide.contains("pip install"),
                "{name} must not emit pip instructions, got:\n{guide}"
            );
            assert!(
                guide.contains("## Without SDK (direct OTLP)")
                    && guide.contains("@opentelemetry/sdk-node"),
                "{name} needs a no-SDK path too, got:\n{guide}"
            );
            // Every symbol the snippet calls must be imported in the same snippet.
            for symbol in ["registerTelemetry", "generateText", "Agent", "query"] {
                if guide.contains(&format!("{symbol}(")) {
                    assert!(
                        guide.contains(&format!("import {{ {symbol}"))
                            || guide.contains(&format!(", {symbol} }}"))
                            || guide.contains(&format!("{symbol}, ")),
                        "{name} uses {symbol} without importing it, got:\n{guide}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_python_frameworks_still_emit_pip_instructions() {
        for name in ["strands", "langgraph", "crewai"] {
            let guide =
                build_setup_guide_template("http://localhost:5388/otel/default", Some(name));
            assert!(guide.contains("pip install"), "{name} lost its pip install");
            assert!(
                guide.contains("```python"),
                "{name} lost its Python snippets"
            );
            assert!(!guide.contains("npm install"), "{name} must not emit npm");
        }
    }

    /// Every Python `sdk_snippet` must be self-contained: the guide template supplies only
    /// the SideSeat/OTel imports, so a snippet that uses `Agent` without importing it
    /// generates a NameError for the user. Eight frameworks shipped that way (Strands, Agno,
    /// Smolagents, AG2, AgentScope, Haystack, browser-use, Azure OpenAI) before this test.
    ///
    /// Checked structurally rather than by running Python, so it needs no interpreter: every
    /// capitalised name the snippet calls must be bound by an import or an assignment in the
    /// same snippet.
    /// Names bound by an import, assignment or def inside the snippet itself.
    fn bound_names(snippet: &str) -> Vec<String> {
        let mut bound: Vec<String> = Vec::new();
        for line in snippet.lines() {
            let t = line.trim();
            if let Some(rest) = t
                .strip_prefix("from ")
                .and_then(|r| r.split(" import ").nth(1))
            {
                bound.extend(rest.split(',').map(|n| n.trim().to_string()));
            } else if let Some(rest) = t.strip_prefix("import ") {
                bound.extend(rest.split(',').map(|n| n.trim().to_string()));
            } else if let Some(rest) = t.strip_prefix("async def ").or(t.strip_prefix("def ")) {
                if let Some((name, _)) = rest.split_once('(') {
                    bound.push(name.trim().to_string());
                }
            } else if let Some(rest) = t.strip_prefix("async for ").or(t.strip_prefix("for ")) {
                if let Some((name, _)) = rest.split_once(" in ") {
                    bound.extend(name.split(',').map(|n| n.trim().to_string()));
                }
            } else if let Some((lhs, _)) = t.split_once('=') {
                let lhs = lhs.trim();
                if !lhs.is_empty() && !lhs.contains(' ') && !lhs.contains('(') {
                    bound.push(lhs.to_string());
                }
            }
        }
        bound
    }

    #[test]
    fn test_python_snippets_define_every_name_they_use() {
        for fw in FRAMEWORKS {
            if fw.lang != Lang::Python {
                continue;
            }
            let snippet = fw.sdk_snippet;
            let bound = bound_names(snippet);
            // Any Capitalised identifier immediately followed by `(` is a constructor call.
            let mut used: Vec<String> = Vec::new();
            let bytes: Vec<char> = snippet.chars().collect();
            let mut i = 0;
            while i < bytes.len() {
                if bytes[i].is_ascii_uppercase()
                    && (i == 0
                        || !(bytes[i - 1].is_alphanumeric()
                            || bytes[i - 1] == '_'
                            || bytes[i - 1] == '.'))
                {
                    let start = i;
                    while i < bytes.len() && (bytes[i].is_alphanumeric() || bytes[i] == '_') {
                        i += 1;
                    }
                    if i < bytes.len() && bytes[i] == '(' {
                        used.push(bytes[start..i].iter().collect());
                    }
                } else {
                    i += 1;
                }
            }
            for name in used {
                assert!(
                    bound.contains(&name),
                    "{}: snippet calls `{}` but never imports or defines it:\n{}",
                    fw.display,
                    name,
                    snippet
                );
            }
        }
    }

    /// The variables a Claude Agent SDK integration cannot work without.
    ///
    /// The Claude Code CLI produces telemetry only when told to, by environment variable, and
    /// omitting any one of these yields either no spans or spans with no message content. The
    /// list is asserted against every place we hand a user this configuration - see
    /// [`claude_configuration_agrees_everywhere_it_is_duplicated`].
    const CLAUDE_REQUIRED_ENV: [&str; 9] = [
        "CLAUDE_CODE_ENABLE_TELEMETRY",
        "CLAUDE_CODE_ENHANCED_TELEMETRY_BETA",
        "ENABLE_BETA_TRACING_DETAILED",
        "BETA_TRACING_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_TRACES_EXPORTER",
        "OTEL_LOG_USER_PROMPTS",
        "OTEL_LOG_TOOL_DETAILS",
    ];

    /// Every placeholder must be substituted, and the Claude Agent SDK guides must carry the
    /// exporter configuration in both languages.
    ///
    /// The TypeScript snippet declared `options` but set only CLAUDE_CODE_ENABLE_TELEMETRY, so
    /// it emitted no spans anywhere: the CLI subprocess needs the endpoint, the two beta tiers
    /// and the content flags, exactly as the Python copy has.
    #[test]
    fn test_claude_guides_carry_the_exporter_configuration() {
        for name in ["claude-agent-sdk", "claude-agent-sdk-typescript"] {
            let guide = build_setup_guide("default", Some(name));
            assert!(
                !guide.contains("__OTLP_"),
                "{name}: an OTLP placeholder was not substituted:\n{guide}"
            );
            for required in CLAUDE_REQUIRED_ENV {
                assert!(
                    guide.contains(required),
                    "{name}: guide omits {required}, so the CLI would emit nothing useful"
                );
            }
        }
    }

    /// This configuration is spelled out in seven places, and drift means a user copies a setup
    /// that produces no telemetry.
    ///
    /// Two are executable (the Python and TypeScript sample suites, which are run), one is a
    /// script, and four are copies handed to users: the MCP setup guide, the framework page, the
    /// docs homepage and the telemetry configuration UI. Deriving the four from one source would
    /// mean generating MDX and TypeScript from Rust; asserting they agree costs one test and
    /// fails the moment they do not. A new copy has to be added here, which makes adding one a
    /// decision rather than an accident.
    #[test]
    fn claude_configuration_agrees_everywhere_it_is_duplicated() {
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("repo root");

        let copies = [
            "docs/src/content/docs/docs/integrations/frameworks/claude-agent-sdk.mdx",
            "docs/src/content/docs/docs/index.mdx",
            "web/src/pages/configuration/telemetry-frameworks.ts",
            "misc/samples/python/claude-agent-sdk/telemetry_setup.py",
            "misc/samples/js/src/shared/telemetry.ts",
            "misc/scripts/run-claude.sh",
        ];

        for relative in copies {
            let path = repo.join(relative);
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{relative}: {e} - was this copy moved or deleted?"));
            for required in CLAUDE_REQUIRED_ENV {
                assert!(
                    text.contains(required),
                    "{relative} omits {required}, so it disagrees with the MCP setup guide - \
                     the CLI would emit nothing useful for anyone following it"
                );
            }
        }
    }

    /// TypeScript snippets must declare every identifier they use, same as the Python ones.
    ///
    /// The Python check below never covered them, and two shipped broken: the Strands snippet
    /// passed `{ model }` and the Claude Agent SDK snippet passed `options`, neither declared.
    /// A user pasting either got a ReferenceError.
    #[test]
    fn test_typescript_snippets_declare_every_identifier() {
        for fw in FRAMEWORKS {
            if fw.lang != Lang::TypeScript {
                continue;
            }
            let snippet = fw.sdk_snippet;
            let mut bound: Vec<String> = Vec::new();
            for line in snippet.lines() {
                let t = line.trim();
                // `import { a, b } from '...'`
                if let Some(rest) = t.strip_prefix("import {").and_then(|r| r.split('}').next()) {
                    bound.extend(rest.split(',').map(|n| n.trim().to_string()));
                }
                // `const x = ...` / `let x = ...`
                for kw in ["const ", "let ", "var "] {
                    if let Some(rest) = t.strip_prefix(kw) {
                        let name = rest
                            .split(['=', ':', ' ', '('])
                            .next()
                            .unwrap_or("")
                            .trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
                        if !name.is_empty() {
                            bound.push(name.to_string());
                        }
                        // Destructuring: `const { text } = ...`
                        if rest.trim_start().starts_with('{')
                            && let Some(inner) =
                                rest.split('{').nth(1).and_then(|r| r.split('}').next())
                        {
                            bound.extend(inner.split(',').map(|n| n.trim().to_string()));
                        }
                    }
                }
                // `for await (const msg of ...)`
                if let Some(rest) = t.split("const ").nth(1)
                    && t.contains(" of ")
                    && let Some(name) = rest.split_whitespace().next()
                {
                    bound.push(name.to_string());
                }
            }

            // Identifiers the snippet USES, from three shapes:
            //   `{ model }`      shorthand object property
            //   `f(options)`     bare identifier argument
            //   `agent.invoke()` member access
            //
            // A hardcoded list of six names only guarded the cases that had already broken;
            // deleting a `const agent = ...` line passed because `agent` appeared only as member
            // access, which nothing looked at.
            const RUNTIME_GLOBALS: &[&str] = &["process", "console", "JSON", "Math"];
            // Declared by the guide template that wraps every snippet, not by the snippet:
            // `import { init, Frameworks } from '@sideseat/sdk'` on the SDK path and the NodeSDK
            // block on the direct-OTLP path. A snippet using these is correct.
            const TEMPLATE_PROVIDED: &[&str] =
                &["init", "sdk", "query", "generateText", "registerTelemetry"];
            const KEYWORDS: &[&str] = &[
                "const",
                "let",
                "var",
                "await",
                "for",
                "of",
                "new",
                "import",
                "from",
                "async",
                "return",
                "true",
                "false",
                "null",
                "undefined",
            ];
            let plausible = |t: &str| {
                t.len() > 1
                    && t.chars().all(|c| c.is_ascii_lowercase() || c == '_')
                    && !KEYWORDS.contains(&t)
                    && !RUNTIME_GLOBALS.contains(&t)
                    && !TEMPLATE_PROVIDED.contains(&t)
            };

            // String literals are stripped first: a model id like 'us.anthropic.claude-...' would
            // otherwise be read as member access on an undeclared `us`.
            let mut code = String::with_capacity(snippet.len());
            let mut quote: Option<char> = None;
            for ch in snippet.chars() {
                match quote {
                    Some(q) => {
                        if ch == q {
                            quote = None;
                        }
                    }
                    None => {
                        if ch == '\'' || ch == '"' || ch == '`' {
                            quote = Some(ch);
                        } else {
                            code.push(ch);
                        }
                    }
                }
            }
            let snippet_code = code.as_str();

            let mut used: Vec<String> = Vec::new();
            for seg in snippet_code.split(['{', '}', '(', ')', ',', ';']) {
                let t = seg.trim();
                if plausible(t) {
                    used.push(t.to_string());
                }
            }
            // Direct callees: `model: bedrock(...)` is a use of `bedrock`, but it matched neither
            // the shorthand-property nor the member-access shape, so deleting its import passed.
            for (idx, _) in snippet_code.match_indices('(') {
                let head: String = snippet_code[..idx]
                    .chars()
                    .rev()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect::<Vec<char>>()
                    .into_iter()
                    .rev()
                    .collect();
                // Skip a method call: in `console.log(...)` the callee is `log`, which is not an
                // identifier the snippet must declare - the member-access scan below covers
                // `console` instead.
                let is_method = snippet_code[..idx - head.len()].ends_with('.');
                if !is_method && plausible(&head) {
                    used.push(head);
                }
            }
            for tok in snippet_code.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.'))
            {
                if let Some((head, rest)) = tok.split_once('.')
                    && !rest.is_empty()
                    && plausible(head)
                {
                    used.push(head.to_string());
                }
            }
            used.sort();
            used.dedup();

            for token in &used {
                assert!(
                    bound.iter().any(|b| b == token),
                    "{}: TypeScript snippet uses `{token}` without declaring it:\n{}",
                    fw.display,
                    snippet
                );
            }
        }
    }

    /// Lowercase names passed as keyword arguments must be bound in the snippet too.
    /// `model=model`, `llm=llm`, `llm_config=llm_config` and `retriever` all shipped as
    /// bare placeholders that read as if defined elsewhere, giving the user a NameError.
    /// Unlike the check above this covers lowercase names, which are not constructor calls.
    #[test]
    fn test_python_snippets_have_no_unbound_placeholders() {
        for fw in FRAMEWORKS {
            if fw.lang != Lang::Python {
                continue;
            }
            let bound = bound_names(fw.sdk_snippet);
            for candidate in [
                "model",
                "llm",
                "llm_config",
                "retriever",
                "options",
                "tools",
            ] {
                let used = fw.sdk_snippet.contains(&format!("={candidate})"))
                    || fw.sdk_snippet.contains(&format!("={candidate},"));
                if used {
                    assert!(
                        bound.iter().any(|b| b == candidate),
                        "{}: snippet passes `{}` but never binds it:\n{}",
                        fw.display,
                        candidate,
                        fw.sdk_snippet
                    );
                }
            }
            assert!(
                !fw.sdk_snippet.contains("[...]"),
                "{}: snippet has an elided `[...]` argument, which is not runnable:\n{}",
                fw.display,
                fw.sdk_snippet
            );
        }
    }

    #[test]
    fn test_documented_framework_values_all_resolve() {
        // docs/src/content/docs/docs/mcp.mdx publishes this list to users. A value that
        // does not resolve makes setup_guide silently fall back to the generic guide.
        for name in [
            "strands",
            "langchain",
            "langgraph",
            "crewai",
            "autogen",
            "openai-agents",
            "pydantic-ai",
            "google-adk",
            "agent-framework",
            "claude-agent-sdk",
            "bedrock",
            "openai",
            "anthropic",
            "google-genai",
            "vertex-ai",
            "agno",
            "smolagents",
            "ag2",
            "agentscope",
            "langflow",
            "haystack",
            "browser-use",
            "azure-openai",
            // TypeScript
            "vercel-ai",
            "strands-typescript",
            "claude-agent-sdk-typescript",
        ] {
            assert!(
                get_framework(name).is_some(),
                "documented framework value {name:?} does not resolve"
            );
        }
    }

    #[test]
    fn test_setup_guide_carries_required_sdk_extra() {
        // A framework whose instrumentation lives behind an optional extra must have it on
        // the SDK install line. Without the extra the import fails, instrument() only logs a
        // warning, and the user gets a running app that emits no spans at all.
        for (name, extra) in [
            ("langgraph", "langgraph"),
            ("crewai", "crewai"),
            ("autogen", "autogen"),
            ("bedrock", "aws"),
            ("anthropic", "anthropic"),
            ("vertex-ai", "vertex-ai"),
        ] {
            let guide =
                build_setup_guide_template("http://localhost:5388/otel/default", Some(name));
            assert!(
                guide.contains(&format!("pip install \"sideseat[{extra}]\"")),
                "{name} must install sideseat[{extra}], got:\n{guide}"
            );
        }
        // Frameworks that need no extra keep the bare package.
        for name in ["strands", "google-adk", "claude-agent-sdk"] {
            let guide =
                build_setup_guide_template("http://localhost:5388/otel/default", Some(name));
            assert!(
                guide.contains("pip install sideseat "),
                "{name} should install plain sideseat, got:\n{guide}"
            );
        }
    }

    #[test]
    fn test_setup_guide_resolves_claude_agent_sdk() {
        let guide = build_setup_guide("demo", Some("claude-agent-sdk"));
        assert!(guide.contains("Frameworks.ClaudeAgentSDK"));
        assert!(guide.contains("CLAUDE_CODE_ENHANCED_TELEMETRY_BETA"));
        // Snippets are inserted as values, so the endpoint placeholder must be
        // substituted after formatting or it leaks into the output verbatim.
        assert!(!guide.contains("__OTLP_ENDPOINT__"));
        assert!(guide.contains("http://localhost:5388/otel/demo/v1/traces"));
    }

    #[test]
    fn test_setup_guide_matches_framework_aliases() {
        // get_framework() matches on the kebab-cased display name, the lowercased
        // sdk_variant (with and without hyphens), and the pip package.
        for name in [
            "claude-agent-sdk",
            "claudeagentsdk",
            "Claude-Agent-SDK",
            "CLAUDE-AGENT-SDK",
        ] {
            let guide = build_setup_guide("demo", Some(name));
            assert!(
                guide.contains("Frameworks.ClaudeAgentSDK"),
                "'{name}' should resolve to the Claude Agent SDK entry"
            );
        }
    }

    #[test]
    fn test_parse_opt_ts_valid_iso8601() {
        let result = parse_opt_ts(Some("2025-01-15T12:00:00Z".to_string()));
        assert!(result.is_some());
        assert_eq!(result.unwrap().timestamp(), 1736942400);
    }

    #[test]
    fn test_parse_opt_ts_none() {
        assert!(parse_opt_ts(None).is_none());
    }

    #[test]
    fn test_parse_opt_ts_invalid() {
        assert!(parse_opt_ts(Some("not-a-date".to_string())).is_none());
    }

    #[test]
    fn test_parse_ts_valid() {
        assert!(parse_ts("2025-01-15T12:00:00Z").is_some());
    }

    #[test]
    fn test_parse_ts_invalid() {
        assert!(parse_ts("garbage").is_none());
    }

    #[test]
    fn test_ok_json_serializes() {
        let val = serde_json::json!({"key": "value"});
        let result = ok_json(&val);
        assert!(result.is_ok());
        let call_result = result.unwrap();
        assert!(!call_result.content.is_empty());
    }

    #[test]
    fn test_clamp_page() {
        assert_eq!(clamp_page(None), 1);
        assert_eq!(clamp_page(Some(0)), 1);
        assert_eq!(clamp_page(Some(1)), 1);
        assert_eq!(clamp_page(Some(5)), 5);
    }

    #[test]
    fn test_clamp_limit() {
        assert_eq!(clamp_limit(None), 20);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(1)), 1);
        assert_eq!(clamp_limit(Some(50)), 50);
        assert_eq!(clamp_limit(Some(1000)), MAX_PAGE_LIMIT);
    }
}
