//! Attribute extraction for spans.
//!
//! Extracts GenAI attributes, semantic conventions, and classifies spans.

#![allow(clippy::collapsible_if)]

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use opentelemetry_proto::tonic::trace::v1::Span;
use serde_json::{Value as JsonValue, json};

use crate::core::constants;
use crate::data::types::{Framework, ObservationType, SpanCategory};
use crate::domain::pricing;
use crate::utils::string::parse_string_array;
use crate::utils::time::nanos_to_datetime;

use super::truncate_bytes;

use super::{extract_json, keys};

// ============================================================================
// SHARED HELPER FUNCTIONS
// ============================================================================

/// Check if haystack contains needle (case-insensitive, ASCII only).
/// Zero-allocation alternative to `haystack.to_lowercase().contains(needle)`.
#[inline]
fn contains_ascii_ignore_case(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    if haystack.len() < needle.len() {
        return false;
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

/// Merge tags from multiple attribute keys, deduplicating
pub(super) fn merge_tags(attrs: &HashMap<String, String>, tag_keys: &[&str]) -> Vec<String> {
    let mut tags = Vec::new();
    let mut seen = HashSet::new();
    for key in tag_keys {
        if let Some(val) = attrs.get(*key) {
            for tag in parse_string_array(val) {
                if seen.insert(tag.clone()) {
                    tags.push(tag);
                }
            }
        }
    }
    tags
}

/// Get first matching value from attribute keys.
///
/// An **empty** value is not a value, so the chain keeps looking. This is what a fallback chain is for: a
/// framework that sets `session.id=""` alongside `gen_ai.conversation.id="conv-1"` would otherwise have the
/// empty string win, and retrieval treats a stored empty session id as *no session* - so the conversation got
/// no session view, a trace read could not load its siblings, and the project feed could not widen its context,
/// which lets replayed history through as duplicates. Present-but-empty is exactly the shape a chain exists to
/// step over, and no caller wants an empty string in preference to a real one.
pub(super) fn get_first(attrs: &HashMap<String, String>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|k| attrs.get(*k))
        .find(|v| !v.is_empty())
        .cloned()
}

/// Parse a value from attributes.
pub(super) fn parse_opt<T: std::str::FromStr>(
    attrs: &HashMap<String, String>,
    key: &str,
) -> Option<T> {
    attrs.get(key).and_then(|v| v.parse().ok())
}

// ============================================================================
// OTLP CORE FIELD EXTRACTION
// ============================================================================

pub(super) fn set_core_fields(s: &mut SpanData, span: &Span) {
    s.trace_id = hex::encode(&span.trace_id);
    s.span_id = hex::encode(&span.span_id);
    s.parent_span_id = if span.parent_span_id.is_empty() {
        None
    } else {
        Some(hex::encode(&span.parent_span_id))
    };
    s.trace_state = if span.trace_state.is_empty() {
        None
    } else {
        Some(span.trace_state.clone())
    };
    s.span_name = span.name.clone();
    s.span_kind = Some(span_kind_to_string(span.kind).to_string());
    s.status_code = span
        .status
        .as_ref()
        .map(|st| status_code_to_string(st.code).to_string());
    s.status_message = span.status.as_ref().and_then(|st| {
        if st.message.is_empty() {
            None
        } else if st.message.len() > constants::ERROR_MESSAGE_MAX_LEN {
            Some(format!(
                "{}...",
                truncate_bytes(&st.message, constants::ERROR_MESSAGE_MAX_LEN)
            ))
        } else {
            Some(st.message.clone())
        }
    });
    s.timestamp_start = nanos_to_datetime(span.start_time_unix_nano);
    s.timestamp_end = if span.end_time_unix_nano > 0 {
        Some(nanos_to_datetime(span.end_time_unix_nano))
    } else {
        None
    };
    s.duration_ms = if span.end_time_unix_nano > span.start_time_unix_nano {
        ((span.end_time_unix_nano - span.start_time_unix_nano) / 1_000_000) as i64
    } else {
        0
    };
}

fn span_kind_to_string(kind: i32) -> &'static str {
    match kind {
        0 => "UNSPECIFIED",
        1 => "INTERNAL",
        2 => "SERVER",
        3 => "CLIENT",
        4 => "PRODUCER",
        5 => "CONSUMER",
        _ => "UNKNOWN",
    }
}

fn status_code_to_string(code: i32) -> &'static str {
    match code {
        0 => "UNSET",
        1 => "OK",
        2 => "ERROR",
        _ => "UNKNOWN",
    }
}

// ============================================================================
// SPAN NAME RESOLUTION
// ============================================================================

/// Resolve the display span name from attributes.
///
/// Some instrumentation libraries store a template or internal identifier as the
/// OTLP span name and put the human-readable resolved name in an attribute.
/// This function checks for those patterns and overrides `span_name` when a
/// better display name is available.
///
/// Currently handles:
/// - **Logfire** (`logfire.msg_template` + `logfire.msg`): Span name is a Python
///   f-string template like `"Chat Completion with {request_data[model]!r}"`.
///   When `logfire.msg_template` exists, the resolved `logfire.msg` is used.
pub(super) fn resolve_span_name(span: &mut SpanData, attrs: &HashMap<String, String>) {
    // Logfire: span name is the unresolved msg_template; logfire.msg is the resolved version
    if attrs.contains_key(keys::LOGFIRE_MSG_TEMPLATE) {
        if let Some(resolved) = attrs.get(keys::LOGFIRE_MSG) {
            if !resolved.is_empty() {
                span.span_name = resolved.clone();
            }
        }
    }
}

// ============================================================================
// SPAN DATA
// ============================================================================

/// Extracted span data for pipeline processing.
#[derive(Debug, Clone, Default)]
pub struct SpanData {
    // Identity
    pub project_id: Option<String>,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub trace_state: Option<String>,

    // Session/User
    pub session_id: Option<String>,
    pub user_id: Option<String>,

    // Classification
    pub span_name: String,
    pub span_kind: Option<String>,
    pub span_category: Option<SpanCategory>,
    pub observation_type: Option<ObservationType>,
    pub framework: Option<Framework>,
    /// The instrumentation scope that produced this span - `ScopeSpans.scope.name`/`.version`.
    ///
    /// The one fact about a span nothing else derives: the resource names the *process*, the span
    /// attributes name the call, and only the scope names the **library** that emitted the telemetry,
    /// versioned. It was never captured for spans (the metrics path always had it), which meant no
    /// rule could ever be keyed on a producer's identity-and-version - the design record's stage 3
    /// calls that the fact that makes an undeclared producer decidable.
    pub scope_name: Option<String>,
    pub scope_version: Option<String>,
    pub status_code: Option<String>,
    pub status_message: Option<String>,
    pub exception_type: Option<String>,
    pub exception_message: Option<String>,
    pub exception_stacktrace: Option<String>,

    // Time
    pub timestamp_start: DateTime<Utc>,
    pub timestamp_end: Option<DateTime<Utc>>,
    pub duration_ms: i64,

    // Environment
    pub environment: Option<String>,

    // GenAI Core
    pub gen_ai_system: Option<String>,
    pub gen_ai_operation_name: Option<String>,
    pub gen_ai_request_model: Option<String>,
    pub gen_ai_response_model: Option<String>,
    pub gen_ai_response_id: Option<String>,

    // GenAI Parameters
    pub gen_ai_temperature: Option<f64>,
    pub gen_ai_top_p: Option<f64>,
    pub gen_ai_top_k: Option<i64>,
    pub gen_ai_max_tokens: Option<i64>,
    pub gen_ai_frequency_penalty: Option<f64>,
    pub gen_ai_presence_penalty: Option<f64>,
    pub gen_ai_stop_sequences: Vec<String>,
    pub gen_ai_finish_reasons: Vec<String>,

    // GenAI Agent/Tool
    pub gen_ai_agent_id: Option<String>,
    pub gen_ai_agent_name: Option<String>,
    pub gen_ai_tool_name: Option<String>,
    pub gen_ai_tool_call_id: Option<String>,

    // GenAI Performance
    pub gen_ai_server_ttft_ms: Option<i64>,
    pub gen_ai_server_request_duration_ms: Option<i64>,

    // Token Usage
    pub gen_ai_usage_input_tokens: i64,
    pub gen_ai_usage_output_tokens: i64,
    pub gen_ai_usage_total_tokens: i64,
    /// The total the provider *stated*, or 0 when it stated none. Not persisted.
    ///
    /// Kept apart from the synthesised total above so enrichment can redo the synthesis once pricing has
    /// resolved which provider's convention applies. Folding the two together meant the synthesised value
    /// became a floor that could not be lowered: `system=anthropic, model=gpt-4o` synthesised
    /// `input + output + cache` here, pricing then resolved OpenAI (cache already inside the input), and
    /// `max(synthesised, corrected)` kept the too-large number.
    pub gen_ai_usage_total_tokens_reported: i64,
    pub gen_ai_usage_cache_read_tokens: i64,
    pub gen_ai_usage_cache_write_tokens: i64,
    pub gen_ai_usage_reasoning_tokens: i64,
    pub gen_ai_usage_details: JsonValue,

    // Pre-calculated costs (from OpenInference llm.cost.* or other sources)
    // These are used as fallback when pricing service cannot calculate costs
    pub extracted_cost_total: Option<f64>,
    pub extracted_cost_input: Option<f64>,
    pub extracted_cost_output: Option<f64>,

    // External Services
    pub http_method: Option<String>,
    pub http_url: Option<String>,
    pub http_status_code: Option<i64>,
    pub db_system: Option<String>,
    pub db_name: Option<String>,
    pub db_operation: Option<String>,
    pub db_statement: Option<String>,
    pub storage_system: Option<String>,
    pub storage_bucket: Option<String>,
    pub storage_object: Option<String>,
    pub messaging_system: Option<String>,
    pub messaging_destination: Option<String>,

    // Tags/Metadata
    pub tags: Vec<String>,
    pub metadata: JsonValue,
}

// ============================================================================
// TOKEN USAGE CONFIGURATION
// ============================================================================

/// Token count extraction configuration with fallback keys.
struct TokenConfig {
    primary: &'static str,
    fallbacks: &'static [&'static str],
    /// Fallbacks whose names are too generic to consult globally (e.g. a bare
    /// `input_tokens`). Only read for spans that `scope` accepts, so a framework that
    /// happens to use the same name is never credited with tokens or cost.
    scoped_fallbacks: &'static [&'static str],
}

impl TokenConfig {
    const fn new(primary: &'static str, fallbacks: &'static [&'static str]) -> Self {
        Self {
            primary,
            fallbacks,
            scoped_fallbacks: &[],
        }
    }

    const fn with_scoped(
        primary: &'static str,
        fallbacks: &'static [&'static str],
        scoped_fallbacks: &'static [&'static str],
    ) -> Self {
        Self {
            primary,
            fallbacks,
            scoped_fallbacks,
        }
    }

    pub(super) fn extract(&self, attrs: &HashMap<String, String>) -> i64 {
        self.extract_for_span(attrs, "")
    }

    pub(super) fn extract_for_span(&self, attrs: &HashMap<String, String>, span_name: &str) -> i64 {
        self.extract_opt_for_span(attrs, span_name).unwrap_or(0)
    }

    /// The counter if any key in the chain carried one, distinguishing **absent** from a genuine `0`.
    ///
    /// The stored column is never NULL - `extract_for_span` still defaults to 0 - but the framework fallbacks
    /// need the difference: they treated `0` as "missing", so a completion that genuinely produced no output
    /// tokens had its 0 replaced by whatever the framework's JSON happened to report. Presence is a fact about
    /// the payload and cannot be recovered from the value.
    pub(super) fn extract_opt_for_span(
        &self,
        attrs: &HashMap<String, String>,
        span_name: &str,
    ) -> Option<i64> {
        let scoped = if is_claude_code_span(span_name) {
            self.scoped_fallbacks
        } else {
            &[][..]
        };
        attrs
            .get(self.primary)
            .or_else(|| self.fallbacks.iter().find_map(|k| attrs.get(*k)))
            .or_else(|| scoped.iter().find_map(|k| attrs.get(*k)))
            .and_then(|v| v.parse().ok())
    }
}

/// Spans emitted by the Claude Code CLI, which names token attributes outside the
/// `gen_ai.usage.*` conventions.
fn is_claude_code_span(span_name: &str) -> bool {
    span_name.starts_with("claude_code.")
}

const INPUT_TOKENS: TokenConfig = TokenConfig::with_scoped(
    "gen_ai.usage.input_tokens",
    &[
        "gen_ai.usage.prompt_tokens",
        "llm.usage.prompt_tokens",
        "llm.token_count.prompt",
        "ai.usage.promptTokens",
    ],
    &["input_tokens"], // Claude Code CLI (Claude Agent SDK)
);

const OUTPUT_TOKENS: TokenConfig = TokenConfig::with_scoped(
    "gen_ai.usage.output_tokens",
    &[
        "gen_ai.usage.completion_tokens",
        "llm.usage.completion_tokens",
        "llm.token_count.completion",
        "ai.usage.completionTokens",
    ],
    &["output_tokens"], // Claude Code CLI (Claude Agent SDK)
);

const TOTAL_TOKENS: TokenConfig =
    TokenConfig::new("gen_ai.usage.total_tokens", &["llm.token_count.total"]);

const CACHE_READ_TOKENS: TokenConfig = TokenConfig::with_scoped(
    "gen_ai.usage.cache_read_input_tokens",
    &[
        "gen_ai.usage.cache_read_tokens",
        // Dotted semconv spelling, used by Pipecat among others.
        "gen_ai.usage.cache_read.input_tokens",
        "llm.usage.cache_read_input_tokens",
        "ai.usage.cachedInputTokens",
    ],
    &["cache_read_tokens"], // Claude Code CLI (Claude Agent SDK)
);

const CACHE_WRITE_TOKENS: TokenConfig = TokenConfig::with_scoped(
    "gen_ai.usage.cache_creation_input_tokens",
    &[
        "gen_ai.usage.cache_write_input_tokens", // Strands
        "gen_ai.usage.cache_write_tokens",
        "gen_ai.usage.cache_creation.input_tokens",
        "llm.usage.cache_creation_input_tokens",
    ],
    &["cache_creation_tokens"], // Claude Code CLI (Claude Agent SDK)
);

const REASONING_TOKENS: TokenConfig = TokenConfig::new(
    "gen_ai.usage.output_reasoning_tokens",
    &[
        "gen_ai.usage.thoughts_token_count",
        // Dotted and bare semconv spellings.
        "gen_ai.usage.reasoning.output_tokens",
        "gen_ai.usage.reasoning_tokens",
        "ai.usage.reasoningTokens",
    ],
);

const KNOWN_USAGE_FIELDS: &[&str] = &[
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "prompt_tokens",
    "completion_tokens",
    "cache_read_input_tokens",
    "cache_read_tokens",
    "cache_creation_input_tokens",
    "cache_write_tokens",
    "output_reasoning_tokens",
    "thoughts_token_count",
];

// ============================================================================
// FRAMEWORK DETECTION
// ============================================================================

/// Custom matcher function type for complex framework detection logic
type CustomMatcher = fn(&str, &HashMap<String, String>, &HashMap<String, String>) -> bool;

/// Framework detection rule for declarative matching
struct FrameworkRule {
    framework: Framework,
    /// Match if span name equals or starts with any of these
    span_name_match: &'static [&'static str],
    /// Match if any attribute key starts with any of these prefixes
    attr_prefix: &'static [&'static str],
    /// Match if attribute equals (key, value)
    attr_equals: &'static [(&'static str, &'static str)],
    /// Match if service.name equals or contains any of these
    service_name: &'static [&'static str],
    /// Match if any of these attribute keys exist
    attr_exists: &'static [&'static str],
    /// Match if metadata JSON contains any of these strings
    metadata_contains: &'static [&'static str],
    /// Custom matcher for complex logic (return true to match)
    custom: Option<CustomMatcher>,
}

/// Default rule for struct update syntax in const context
const DEFAULT_RULE: FrameworkRule = FrameworkRule {
    framework: Framework::Unknown,
    span_name_match: &[],
    attr_prefix: &[],
    attr_equals: &[],
    service_name: &[],
    attr_exists: &[],
    metadata_contains: &[],
    custom: None,
};

/// Macro to create FrameworkRule with defaults for unspecified fields
macro_rules! rule {
    ($framework:expr $(, $field:ident : $value:expr)* $(,)?) => {
        FrameworkRule {
            framework: $framework,
            $($field: $value,)*
            ..DEFAULT_RULE
        }
    };
}

impl FrameworkRule {
    fn matches(
        &self,
        span_name: &str,
        span_attrs: &HashMap<String, String>,
        resource_attrs: &HashMap<String, String>,
    ) -> bool {
        // Span name match
        if !self.span_name_match.is_empty()
            && self
                .span_name_match
                .iter()
                .any(|p| span_name == *p || span_name.starts_with(p))
        {
            return true;
        }

        // Attribute prefix match
        if !self.attr_prefix.is_empty()
            && self
                .attr_prefix
                .iter()
                .any(|p| span_attrs.keys().any(|k| k.starts_with(p)))
        {
            return true;
        }

        // Attribute equals match
        if !self.attr_equals.is_empty()
            && self
                .attr_equals
                .iter()
                .any(|(k, v)| span_attrs.get(*k).is_some_and(|val| val == *v))
        {
            return true;
        }

        // Service name match
        if !self.service_name.is_empty() {
            if let Some(svc) = resource_attrs.get(keys::SERVICE_NAME) {
                if self
                    .service_name
                    .iter()
                    .any(|s| svc == *s || svc.contains(s))
                {
                    return true;
                }
            }
        }

        // Attribute exists match
        if !self.attr_exists.is_empty()
            && self.attr_exists.iter().any(|k| span_attrs.contains_key(*k))
        {
            return true;
        }

        // Metadata contains match
        if !self.metadata_contains.is_empty() {
            if let Some(metadata) = span_attrs.get(keys::METADATA) {
                if self.metadata_contains.iter().any(|s| metadata.contains(s)) {
                    return true;
                }
            }
        }

        // Custom matcher
        if let Some(f) = self.custom {
            if f(span_name, span_attrs, resource_attrs) {
                return true;
            }
        }

        false
    }
}

/// Vercel AI SDK custom matcher - has complex prefix matching
fn vercel_ai_matcher(
    _: &str,
    span_attrs: &HashMap<String, String>,
    _: &HashMap<String, String>,
) -> bool {
    span_attrs.keys().any(|k| {
        k.starts_with("ai.prompt.")
            || k.starts_with("ai.completion.")
            || k.starts_with("ai.settings.")
            || k.starts_with("ai.telemetry.")
            || k.starts_with("ai.stream.")
            || k.starts_with("ai.finishReason")
            || k.starts_with("ai.usage.")
    })
}

/// Logfire SDK name matcher
fn logfire_sdk_matcher(
    _: &str,
    _: &HashMap<String, String>,
    resource_attrs: &HashMap<String, String>,
) -> bool {
    resource_attrs
        .get(keys::TELEMETRY_SDK_NAME)
        .is_some_and(|v| v.contains("logfire"))
}

/// Strands Agents custom matcher — case-insensitive search for "strands" + separator + "agent"
/// in the span name or gen_ai.agent.name attribute.
/// Separators: space, hyphen, underscore (e.g. "Strands Agent", "strands-agent", "strands_agent").
fn strands_agents_matcher(
    span_name: &str,
    span_attrs: &HashMap<String, String>,
    _: &HashMap<String, String>,
) -> bool {
    let contains_strands_agent = |s: &str| {
        let lower = s.to_lowercase();
        lower.contains("strands agent")
            || lower.contains("strands-agent")
            || lower.contains("strands_agent")
    };
    contains_strands_agent(span_name)
        || span_attrs
            .get("gen_ai.agent.name")
            .is_some_and(|v| contains_strands_agent(v))
}

/// Traceloop SDK name matcher
fn traceloop_sdk_matcher(
    _: &str,
    _: &HashMap<String, String>,
    resource_attrs: &HashMap<String, String>,
) -> bool {
    resource_attrs
        .get(keys::TELEMETRY_SDK_NAME)
        .is_some_and(|v| v.contains("traceloop"))
}

/// Framework detection rules in priority order (first match wins)
///
/// IMPORTANT: All specific attribute-based rules come BEFORE generic service-name fallbacks.
/// The sideseat SDK defaults service.name to "strands-agents", so service_name-based detection
/// must be the LAST check to avoid misidentifying other frameworks.
const FRAMEWORK_RULES: &[FrameworkRule] = &[
    // AutoGen - check gen_ai.system and span name prefix
    // (OpenInference AutoGen sets gen_ai.system="autogen" but service.name may be default)
    rule!(Framework::AutoGen,
        span_name_match: &["autogen ", "autogen."],
        attr_prefix: &["autogen."],
        attr_equals: &[(keys::GEN_AI_SYSTEM, "autogen")],
    ),
    // Google ADK - check gcp.vertex.agent.* attributes BEFORE service name fallback
    rule!(Framework::GoogleAdk,
        attr_prefix: &["google.adk.", "gcp.vertex.agent."],
        attr_equals: &[(keys::GEN_AI_SYSTEM, "gcp.vertex.agent")],
    ),
    // CrewAI - check specific attributes
    rule!(Framework::CrewAI,
        service_name: &["crewAI-telemetry"],
        attr_exists: &["crewai_version", "crew_key", "crew_id", "crew_fingerprint", "task_key"],
    ),
    // LangGraph (before LangChain - more specific)
    rule!(Framework::LangGraph,
        span_name_match: &["LangGraph", "LangGraph."],
        attr_prefix: &["langgraph."],
        metadata_contains: &["langgraph_", "\"langgraph_"],
    ),
    // LangChain
    rule!(Framework::LangChain, attr_prefix: &["langchain.", "langsmith."]),
    // LlamaIndex
    rule!(Framework::LlamaIndex, attr_prefix: &["llama_index."]),
    // Frameworks instrumented *through* OpenInference. Each emits its own `X.*`
    // attributes alongside `openinference.*`, so these rules must precede the
    // OpenInference rule below or that broader rule claims the spans first.
    //
    // Detection is by attribute prefix only, never service.name: service-name matching is
    // a substring test (`svc.contains(s)`), so `"agno"` would also match a user service
    // called `diagnostics`.
    rule!(Framework::Agno, attr_prefix: &["agno."]),
    rule!(Framework::Smolagents, attr_prefix: &["smolagents."]),
    rule!(Framework::AgentScope, attr_prefix: &["agentscope."]),
    rule!(Framework::Langflow, attr_prefix: &["langflow."]),
    rule!(Framework::Ag2, attr_prefix: &["ag2."]),
    rule!(Framework::Haystack, attr_prefix: &["haystack."]),
    // browser-use sets gen_ai.provider.name unconditionally on every span it emits.
    rule!(Framework::BrowserUse, attr_equals: &[(keys::GEN_AI_PROVIDER_NAME, "browser_use")]),
    // OpenInference
    rule!(Framework::OpenInference, attr_prefix: &["openinference."]),
    // Semantic Kernel
    rule!(Framework::SemanticKernel, attr_prefix: &["semantic_kernel."]),
    // Azure OpenAI (before AzureAIFoundry - more specific)
    rule!(Framework::AzureOpenAI,
        attr_equals: &[
            (keys::GEN_AI_SYSTEM, "azure_openai"),
            (keys::GEN_AI_SYSTEM, "azure.openai"),
            (keys::GEN_AI_PROVIDER_NAME, "azure_openai"),
        ],
        attr_prefix: &["azure.openai."],
    ),
    // Azure AI Foundry
    rule!(Framework::AzureAIFoundry, attr_prefix: &["az.ai."]),
    // Vertex AI — opentelemetry-instrumentation-vertexai (openllmetry) uses vertexai.* span names
    rule!(Framework::VertexAI, span_name_match: &["vertexai."]),
    // Vercel AI SDK
    rule!(Framework::VercelAISdk,
        attr_exists: &["ai.operationId", "ai.telemetry.functionId", "ai.telemetry.metadata"],
        custom: Some(vercel_ai_matcher),
    ),
    // Logfire
    rule!(Framework::Logfire, attr_prefix: &["logfire."], custom: Some(logfire_sdk_matcher)),
    // MLflow
    rule!(Framework::MLFlow, attr_prefix: &["mlflow."]),
    // TraceLoop
    rule!(Framework::TraceLoop, attr_prefix: &["traceloop."], custom: Some(traceloop_sdk_matcher)),
    // LiveKit
    rule!(Framework::LiveKit, attr_prefix: &["livekit.", "lk."]),
    // OpenAI Agents SDK
    rule!(Framework::OpenAIAgents,
        attr_prefix: &["openai.agents."],
        service_name: &["openai-agents", "openai_agents"],
    ),
    // Microsoft Agent Framework
    rule!(Framework::AgentFramework,
        attr_equals: &[(keys::GEN_AI_PROVIDER_NAME, "microsoft.agent_framework")],
        service_name: &["agent-framework-core"],
    ),
    // AWS Bedrock
    rule!(Framework::AWSBedrock,
        attr_prefix: &["aws.bedrock."],
        attr_equals: &[(keys::GEN_AI_SYSTEM, "aws_bedrock"), (keys::GEN_AI_SYSTEM, "aws.bedrock")],
    ),
    // Claude Agent SDK - the Claude Code CLI subprocess emits claude_code.* spans
    // (interaction, llm_request, tool, tool.execution, tool.blocked_on_user, hook).
    // The prefix covers all of them. Two service names because the CLI reports
    // "claude-code" while the host process wrapping it reports "claude-agent-sdk".
    rule!(Framework::ClaudeAgentSdk,
        span_name_match: &["claude_code."],
        service_name: &["claude-code", "claude-agent-sdk"],
    ),
    // Strands Agents - LAST because service.name="strands-agents" is the sideseat SDK default
    // Only match if gen_ai.system explicitly says "strands-agents" or no other framework matched.
    // Custom matcher does case-insensitive search for "strands" + separator + "agent"
    // in the span name or gen_ai.agent.name attribute (covers space, hyphen, underscore).
    rule!(Framework::StrandsAgents,
        attr_equals: &[
            (keys::GEN_AI_SYSTEM, "strands-agents"),
            (keys::GEN_AI_PROVIDER_NAME, "strands-agents"),
        ],
        service_name: &["strands-agents"],
        custom: Some(strands_agents_matcher),
    ),
];

/// Detect framework from span and resource attributes.
///
/// Evidence from the span wins; a declaration only fills the gap. The SDKs write
/// `sideseat.framework` into the resource, and it is consulted **last** - after every rule has failed -
/// because it is a statement about the *process*, not about this span: a process configured for Strands can
/// still emit LangChain spans from a nested library, and those carry `langchain.*` for a rule to find.
/// Overriding on the declaration would relabel them.
///
/// It is consulted at all because the current OTel GenAI conventions are framework-neutral by design: the
/// Vercel AI SDK's current integration emits pure `gen_ai.*` with no `ai.*` attributes, so no rule can
/// attribute it and no rule should have to. A declaration is the only evidence that exists.
pub(crate) fn detect_framework(
    span_name: &str,
    span_attrs: &HashMap<String, String>,
    resource_attrs: &HashMap<String, String>,
) -> Framework {
    for rule in FRAMEWORK_RULES {
        if rule.matches(span_name, span_attrs, resource_attrs) {
            return rule.framework;
        }
    }
    declared_framework(resource_attrs).unwrap_or(Framework::Unknown)
}

/// The framework an SDK declared, when it declared exactly one this server recognises.
///
/// A list is accepted because the SDKs accept one (`framework=[Strands, Bedrock]`), and resolved only when
/// it names a single *framework*: provider slugs return `None` from `from_sdk_slug`, so declaring
/// `[Strands, Bedrock]` still resolves to Strands, while two genuine frameworks resolve to nothing. Two
/// answers is not an answer, and guessing between them would put a label on a span with no evidence for it.
fn declared_framework(resource_attrs: &HashMap<String, String>) -> Option<Framework> {
    let declared = resource_attrs.get(keys::SIDESEAT_FRAMEWORK)?;
    let mut frameworks = declared
        .split(',')
        .filter_map(Framework::from_sdk_slug)
        .collect::<Vec<_>>();
    frameworks.dedup();
    match frameworks.as_slice() {
        [one] => Some(*one),
        _ => None,
    }
}

// ============================================================================
// SEMANTIC KIND CLASSIFICATION
// ============================================================================

#[derive(Clone, Copy)]
#[allow(clippy::upper_case_acronyms)]
enum SemanticKind {
    LLM,
    Embedding,
    Agent,
    Tool,
    Chain,
    Retriever,
    Guardrail,
    Evaluator,
}

impl SemanticKind {
    fn parse(kind: &str) -> Option<Self> {
        match kind.to_uppercase().as_str() {
            "LLM" => Some(Self::LLM),
            "EMBEDDING" => Some(Self::Embedding),
            "AGENT" => Some(Self::Agent),
            "TOOL" => Some(Self::Tool),
            "CHAIN" => Some(Self::Chain),
            "RETRIEVER" => Some(Self::Retriever),
            "GUARDRAIL" => Some(Self::Guardrail),
            "EVALUATOR" => Some(Self::Evaluator),
            _ => None,
        }
    }

    fn to_category(self) -> SpanCategory {
        match self {
            Self::LLM => SpanCategory::LLM,
            Self::Embedding => SpanCategory::Embedding,
            Self::Agent => SpanCategory::Agent,
            Self::Tool => SpanCategory::Tool,
            Self::Chain => SpanCategory::Chain,
            Self::Retriever => SpanCategory::Retriever,
            Self::Guardrail | Self::Evaluator => SpanCategory::Other,
        }
    }

    fn to_observation_type(self) -> ObservationType {
        match self {
            Self::LLM => ObservationType::Generation,
            Self::Embedding => ObservationType::Embedding,
            Self::Agent => ObservationType::Agent,
            Self::Tool => ObservationType::Tool,
            Self::Chain => ObservationType::Chain,
            Self::Retriever => ObservationType::Retriever,
            Self::Guardrail => ObservationType::Guardrail,
            Self::Evaluator => ObservationType::Evaluator,
        }
    }
}

// ============================================================================
// SPAN CLASSIFICATION
// ============================================================================

/// Categorize span based on attributes and name patterns.
pub(crate) fn categorize_span(span_name: &str, attrs: &HashMap<String, String>) -> SpanCategory {
    // Priority 0: External service indicators (HTTP/RPC/DB) are NEVER GenAI spans
    // This must be checked FIRST to prevent AWS Bedrock API calls (rpc.system=aws-api)
    // from being classified as LLM even if they have gen_ai.* attributes: when a framework
    // SDK is also instrumenting, the transport span is a duplicate view of the real
    // generation span. Traces whose ONLY span is one of these still appear in the trace
    // list - that is handled by the list filter, not here.
    if attrs.contains_key(keys::HTTP_METHOD)
        || attrs.contains_key(keys::HTTP_REQUEST_METHOD)
        || attrs.contains_key(keys::RPC_SYSTEM)
    {
        return SpanCategory::HTTP;
    }
    if attrs.contains_key(keys::DB_SYSTEM) {
        return SpanCategory::DB;
    }
    if attrs.contains_key(keys::MESSAGING_SYSTEM) {
        return SpanCategory::Messaging;
    }
    if attrs.keys().any(|k| k.starts_with("aws.s3.")) {
        return SpanCategory::Storage;
    }

    // Priority 1: gen_ai.operation.name (with embedding model override)
    if let Some(op) = attrs.get(keys::GEN_AI_OPERATION_NAME) {
        match op.as_str() {
            "chat" | "text_completion" => {
                // Check if model name indicates embedding (e.g., amazon.titan-embed-text-v2:0)
                // Some telemetry incorrectly reports embedding operations as text_completion
                if let Some(model) = attrs
                    .get(keys::GEN_AI_REQUEST_MODEL)
                    .or_else(|| attrs.get(keys::GEN_AI_RESPONSE_MODEL))
                {
                    if contains_ascii_ignore_case(model, "embed") {
                        return SpanCategory::Embedding;
                    }
                }
                return SpanCategory::LLM;
            }
            "embeddings" => return SpanCategory::Embedding,
            "execute_tool" => return SpanCategory::Tool,
            "invoke_agent" | "invoke_swarm" | "execute_event_loop_cycle" => {
                return SpanCategory::Agent;
            }
            _ => {}
        }
    }

    // Priority 2: Tool/Agent indicators
    if attrs.contains_key(keys::GEN_AI_TOOL_NAME) {
        return SpanCategory::Tool;
    }
    if attrs.contains_key(keys::GEN_AI_AGENT_NAME) {
        return SpanCategory::Agent;
    }

    // Priority 3: OpenInference span kind
    if let Some(kind) = attrs
        .get(keys::OPENINFERENCE_SPAN_KIND)
        .and_then(|k| SemanticKind::parse(k))
    {
        return kind.to_category();
    }

    // Priority 4: Span name patterns
    let name_lower = span_name.to_lowercase();
    if name_lower.contains("llm") || name_lower.contains("chat") {
        return SpanCategory::LLM;
    }
    if name_lower.contains("embed") {
        return SpanCategory::Embedding;
    }
    if name_lower.contains("retriev") {
        return SpanCategory::Retriever;
    }

    SpanCategory::Other
}

/// Detect observation type from span attributes.
pub(crate) fn detect_observation_type(
    span_name: &str,
    attrs: &HashMap<String, String>,
) -> ObservationType {
    // Priority 0: External service calls (HTTP/RPC/DB) are NEVER GenAI observations, for
    // the same reason as in categorize_span - with a framework SDK present the transport
    // span duplicates the real generation. Visibility of transport-only traces is handled
    // by the trace-list filter.
    if attrs.contains_key(keys::HTTP_METHOD)
        || attrs.contains_key(keys::HTTP_REQUEST_METHOD)
        || attrs.contains_key(keys::RPC_SYSTEM)
        || attrs.contains_key(keys::DB_SYSTEM)
    {
        return ObservationType::Span;
    }

    // Priority 1: gen_ai.operation.name (with embedding model override)
    if let Some(op) = attrs.get(keys::GEN_AI_OPERATION_NAME) {
        match op.as_str() {
            "chat" | "text_completion" => {
                // Check if model name indicates embedding (e.g., amazon.titan-embed-text-v2:0)
                // Some telemetry incorrectly reports embedding operations as text_completion
                if let Some(model) = attrs
                    .get(keys::GEN_AI_REQUEST_MODEL)
                    .or_else(|| attrs.get(keys::GEN_AI_RESPONSE_MODEL))
                {
                    if contains_ascii_ignore_case(model, "embed") {
                        return ObservationType::Embedding;
                    }
                }
                return ObservationType::Generation;
            }
            "embeddings" => return ObservationType::Embedding,
            "create_agent"
                if attrs
                    .get(keys::GEN_AI_SYSTEM)
                    .is_some_and(|s| s == "autogen") =>
            {
                return ObservationType::Span;
            }
            _ => {}
        }
    }

    // Priority 2: SDK span kinds
    for key in [keys::OPENINFERENCE_SPAN_KIND, keys::LANGSMITH_SPAN_KIND] {
        if let Some(kind) = attrs.get(key).and_then(|k| SemanticKind::parse(k)) {
            return kind.to_observation_type();
        }
    }

    // Priority 3: Vercel AI SDK
    if attrs.contains_key(keys::AI_MODEL_ID) || attrs.contains_key(keys::AI_MODEL_PROVIDER) {
        if attrs
            .get(keys::AI_OPERATION_ID)
            .is_some_and(|v| v.contains("embed"))
        {
            return ObservationType::Embedding;
        }
        return ObservationType::Generation;
    }

    // Priority 4: Attribute presence
    if attrs.contains_key(keys::GEN_AI_AGENT_NAME) || attrs.contains_key(keys::GEN_AI_AGENT_ID) {
        return ObservationType::Agent;
    }
    if attrs.contains_key(keys::GEN_AI_TOOL_NAME) || attrs.contains_key(keys::GEN_AI_TOOL_CALL_ID) {
        return ObservationType::Tool;
    }

    // Priority 5: Span name patterns
    let name_lower = span_name.to_lowercase();
    for (pattern, obs_type) in [
        ("embed", ObservationType::Embedding),
        ("agent", ObservationType::Agent),
        ("tool", ObservationType::Tool),
        ("retriev", ObservationType::Retriever),
        ("guardrail", ObservationType::Guardrail),
        ("eval", ObservationType::Evaluator),
    ] {
        if name_lower.contains(pattern) {
            return obs_type;
        }
    }

    // Priority 6: Logfire tags (logfire.tags: ["LLM"])
    if let Some(tags) = attrs.get("logfire.tags") {
        let tags_lower = tags.to_lowercase();
        if tags_lower.contains("llm") {
            return ObservationType::Generation;
        }
    }

    // Priority 7: Has model = Generation
    if attrs.contains_key(keys::GEN_AI_REQUEST_MODEL)
        || attrs.contains_key(keys::GEN_AI_RESPONSE_MODEL)
    {
        return ObservationType::Generation;
    }

    ObservationType::Span
}

/// Sum `models_usage.prompt_tokens` / `completion_tokens` from AutoGen `output.value`.
/// Only extracts from chain spans (`output.value.messages[]`) to avoid double-counting —
/// the same message appears in multiple routing (process) spans.
fn extract_autogen_tokens(attrs: &HashMap<String, String>) -> (i64, i64) {
    let output = match extract_json::<JsonValue>(attrs, keys::OUTPUT_VALUE) {
        Some(v) => v,
        None => return (0, 0),
    };
    let msgs = match output.get("messages").and_then(|m| m.as_array()) {
        Some(m) => m,
        None => return (0, 0),
    };
    let mut pt: i64 = 0;
    let mut ct: i64 = 0;
    for m in msgs {
        if let Some(mu) = m.get("models_usage").filter(|v| v.is_object()) {
            pt += mu
                .get("prompt_tokens")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            ct += mu
                .get("completion_tokens")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
        }
    }
    (pt, ct)
}

// ============================================================================
// ATTRIBUTE EXTRACTION
// ============================================================================

pub(crate) fn extract_semantic(span: &mut SpanData, attrs: &HashMap<String, String>) {
    let metadata: Option<JsonValue> = extract_json(attrs, keys::METADATA);

    // Session ID with framework fallbacks (including Vercel AI telemetry metadata)
    span.session_id = get_first(
        attrs,
        &[
            keys::SESSION_ID,
            // Standard semconv 1.37 conversation id - every compliant emitter sets this,
            // and without it their spans do not group into sessions.
            "gen_ai.conversation.id",
            keys::LANGSMITH_SESSION_ID,
            keys::LANGSMITH_TRACE_SESSION_ID, // LangSmith OTEL exporter
            keys::GCP_VERTEX_SESSION_ID,
            keys::AI_TELEMETRY_SESSION_ID, // Vercel AI SDK
            keys::LANGGRAPH_THREAD_ID,     // LangGraph
            keys::MLFLOW_TRACE_SESSION,    // MLflow
        ],
    )
    .or_else(|| {
        // Try thread_id or langgraph_thread_id from metadata
        metadata.as_ref().and_then(|m| {
            m.get("thread_id")
                .or_else(|| m.get("langgraph_thread_id"))
                .and_then(|v| v.as_str())
                .map(String::from)
        })
    });

    // User ID (including Vercel AI telemetry metadata)
    span.user_id = get_first(
        attrs,
        &[
            keys::USER_ID,
            keys::ENDUSER_ID,
            keys::AI_TELEMETRY_USER_ID, // Vercel AI SDK
            keys::MLFLOW_TRACE_USER,    // MLflow
        ],
    )
    .or_else(|| {
        metadata
            .as_ref()?
            .get("user_id")?
            .as_str()
            .map(String::from)
    });

    // HTTP
    span.http_method = get_first(attrs, &[keys::HTTP_METHOD, keys::HTTP_REQUEST_METHOD]);
    span.http_url = get_first(attrs, &[keys::HTTP_URL, keys::URL_FULL]);
    span.http_status_code = get_first(
        attrs,
        &[keys::HTTP_STATUS_CODE, keys::HTTP_RESPONSE_STATUS_CODE],
    )
    .and_then(|v| v.parse().ok());

    // Database
    span.db_system = attrs.get(keys::DB_SYSTEM).cloned();
    span.db_name = attrs.get(keys::DB_NAME).cloned();
    span.db_operation = attrs.get(keys::DB_OPERATION).cloned();
    span.db_statement = attrs.get(keys::DB_STATEMENT).cloned();

    // Storage
    span.storage_system = attrs.get(keys::CLOUD_PROVIDER).cloned();
    span.storage_bucket = get_first(attrs, &[keys::AWS_S3_BUCKET, keys::GCP_GCS_BUCKET]);
    span.storage_object = get_first(attrs, &[keys::AWS_S3_KEY, keys::GCP_GCS_OBJECT]);

    // Messaging
    span.messaging_system = attrs.get(keys::MESSAGING_SYSTEM).cloned();
    span.messaging_destination = get_first(
        attrs,
        &[
            keys::MESSAGING_DESTINATION,
            keys::MESSAGING_DESTINATION_NAME,
        ],
    );

    // Tags (merge and dedupe from multiple sources)
    span.tags = merge_tags(attrs, &[keys::TAGS, keys::LANGSMITH_TAGS, keys::TAG_TAGS]);
}

pub(crate) fn extract_genai(span: &mut SpanData, attrs: &HashMap<String, String>, span_name: &str) {
    // System and operation
    span.gen_ai_system = get_first(
        attrs,
        &[
            keys::GEN_AI_PROVIDER_NAME,
            keys::GEN_AI_SYSTEM,
            "az.ai.inference.model_provider",
            "ai.model.provider",
            "llm.provider",
        ],
    );
    span.gen_ai_operation_name = attrs.get(keys::GEN_AI_OPERATION_NAME).cloned();

    // Models (including embedding/reranker model names as fallback)
    span.gen_ai_request_model = get_first(
        attrs,
        &[
            keys::GEN_AI_REQUEST_MODEL,
            "ai.model.id",
            "llm.model_name",
            keys::EMBEDDING_MODEL_NAME,
            keys::RERANKER_MODEL_NAME,
        ],
    );
    span.gen_ai_response_model =
        get_first(attrs, &[keys::GEN_AI_RESPONSE_MODEL, "llm.response.model"]);
    span.gen_ai_response_id = attrs.get(keys::GEN_AI_RESPONSE_ID).cloned();

    // Google ADK: model from llm_request JSON
    if span.gen_ai_request_model.is_none() {
        if let Some(req) = extract_json::<JsonValue>(attrs, keys::GCP_VERTEX_LLM_REQUEST) {
            if let Some(model) = req.get("model").and_then(|v| v.as_str()) {
                if !model.is_empty() {
                    span.gen_ai_request_model = Some(model.to_string());
                }
            }
        }
    }

    // CrewAI: model from crew_agents JSON (agent.llm field)
    if span.gen_ai_request_model.is_none() {
        if let Some(agents) = extract_json::<JsonValue>(attrs, "crew_agents") {
            if let Some(arr) = agents.as_array() {
                for agent in arr {
                    if let Some(model) = agent.get("llm").and_then(|v| v.as_str()) {
                        if !model.is_empty() {
                            span.gen_ai_request_model = Some(model.to_string());
                            break;
                        }
                    }
                }
            }
        }
    }

    // Logfire: model, system, operation name and max tokens from request_data JSON.
    //
    // The gate covers *everything this block can fill*, not just the model and system. Gated on those two
    // alone, a span that already carried a flat model and provider skipped the parse entirely - so
    // `request_data.max_tokens` / `max_completion_tokens` and the operation-name fallback were unreachable
    // exactly when the rest of the span was well populated, which is the common Logfire shape rather than an
    // edge one. Each field inside is still filled only when it is the one missing.
    if span.gen_ai_request_model.is_none()
        || span.gen_ai_system.is_none()
        || span.gen_ai_operation_name.is_none()
        || span.gen_ai_max_tokens.is_none()
    {
        if let Some(req) = extract_json::<JsonValue>(attrs, keys::REQUEST_DATA) {
            if span.gen_ai_request_model.is_none() {
                if let Some(model) = req.get("model").and_then(|v| v.as_str()) {
                    if !model.is_empty() {
                        span.gen_ai_request_model = Some(model.to_string());
                    }
                }
            }
            if span.gen_ai_system.is_none() {
                // Anthropic: top-level "system" key (string/array); OpenAI: messages[0].role=system
                if req.get("system").is_some() {
                    span.gen_ai_system = Some("anthropic".to_string());
                } else if req.get("messages").is_some() {
                    span.gen_ai_system = Some("openai".to_string());
                }
            }
            if span.gen_ai_operation_name.is_none() && req.get("messages").is_some() {
                span.gen_ai_operation_name = Some("chat".to_string());
            }
            if span.gen_ai_max_tokens.is_none() {
                span.gen_ai_max_tokens = req
                    .get("max_tokens")
                    .or_else(|| req.get("max_completion_tokens"))
                    .and_then(|v| v.as_i64());
            }
        }
    }

    // Request parameters
    span.gen_ai_temperature = parse_opt(attrs, keys::GEN_AI_TEMPERATURE);
    span.gen_ai_top_p = parse_opt(attrs, keys::GEN_AI_TOP_P);
    span.gen_ai_top_k = parse_opt(attrs, keys::GEN_AI_TOP_K);
    // Only when the flat attribute is actually present. Assigning unconditionally overwrote the
    // `request_data` fallback above with `None` whenever the flat attribute was absent - which is every
    // Logfire span, so `request_data.max_tokens` / `max_completion_tokens` never survived extraction.
    if let Some(max_tokens) = parse_opt(attrs, keys::GEN_AI_MAX_TOKENS) {
        span.gen_ai_max_tokens = Some(max_tokens);
    }
    span.gen_ai_frequency_penalty = parse_opt(attrs, keys::GEN_AI_FREQUENCY_PENALTY);
    span.gen_ai_presence_penalty = parse_opt(attrs, keys::GEN_AI_PRESENCE_PENALTY);

    // OpenInference llm.invocation_parameters fallback
    if let Some(params_json) = attrs.get(keys::LLM_INVOCATION_PARAMETERS) {
        if let Ok(params) = serde_json::from_str::<JsonValue>(params_json) {
            if span.gen_ai_temperature.is_none() {
                span.gen_ai_temperature = params.get("temperature").and_then(|v| v.as_f64());
            }
            if span.gen_ai_top_p.is_none() {
                span.gen_ai_top_p = params.get("top_p").and_then(|v| v.as_f64());
            }
            if span.gen_ai_top_k.is_none() {
                span.gen_ai_top_k = params.get("top_k").and_then(|v| v.as_i64());
            }
            if span.gen_ai_max_tokens.is_none() {
                span.gen_ai_max_tokens = params
                    .get("max_tokens")
                    .or_else(|| params.get("max_output_tokens"))
                    .and_then(|v| v.as_i64());
            }
            if span.gen_ai_frequency_penalty.is_none() {
                span.gen_ai_frequency_penalty =
                    params.get("frequency_penalty").and_then(|v| v.as_f64());
            }
            if span.gen_ai_presence_penalty.is_none() {
                span.gen_ai_presence_penalty =
                    params.get("presence_penalty").and_then(|v| v.as_f64());
            }
        }
    }

    if let Some(stops) = attrs.get(keys::GEN_AI_STOP_SEQUENCES) {
        span.gen_ai_stop_sequences = parse_string_array(stops);
    }
    if let Some(reasons) = attrs.get(keys::GEN_AI_FINISH_REASONS) {
        span.gen_ai_finish_reasons = parse_string_array(reasons);
    }

    // Agent fields
    span.gen_ai_agent_id = get_first(attrs, &[keys::GEN_AI_AGENT_ID, keys::AWS_BEDROCK_AGENT_ID]);
    span.gen_ai_agent_name = get_first(
        attrs,
        &[
            keys::GEN_AI_AGENT_NAME,
            // OpenInference agent span attribute.
            "agent.name",
            "agent_role",
            "recipient_agent_class",
            "sender_agent_class",
        ],
    );

    // Tool fields - logfire.msg is used by Pydantic AI for descriptive tool names
    span.gen_ai_tool_name = get_first(
        attrs,
        &[
            keys::GEN_AI_TOOL_NAME,
            "tool.name",
            "tool_name",
            keys::LOGFIRE_MSG,
        ],
    )
    .or_else(|| span_name.strip_prefix("execute_tool ").map(String::from));
    span.gen_ai_tool_call_id = attrs.get(keys::GEN_AI_TOOL_CALL_ID).cloned();

    // Performance
    span.gen_ai_server_ttft_ms = parse_opt(attrs, keys::GEN_AI_TTFT);
    span.gen_ai_server_request_duration_ms = parse_opt(attrs, keys::GEN_AI_REQUEST_DURATION);

    // Token usage
    // Presence, not value, is what the framework fallbacks below must test - a genuine `0` is a reported
    // count and must not be replaced.
    let flat_input = INPUT_TOKENS.extract_opt_for_span(attrs, span_name);
    let flat_output = OUTPUT_TOKENS.extract_opt_for_span(attrs, span_name);
    span.gen_ai_usage_input_tokens = flat_input.unwrap_or(0);
    span.gen_ai_usage_output_tokens = flat_output.unwrap_or(0);
    // Whether each counter has been *supplied* - by the flat attributes or by a fallback that already ran.
    // Every fallback below reads and updates these rather than testing the stored value, for two reasons: a
    // reported `0` is a supplied count and must not be replaced, and the fallbacks run in sequence, so
    // testing the value let a later JSON source overwrite what an earlier one had legitimately provided.
    let mut input_supplied = flat_input.is_some();
    let mut output_supplied = flat_output.is_some();

    // MLflow token usage from JSON blob (only for a counter nothing has supplied yet)
    if !input_supplied || !output_supplied {
        if let Some(usage) = extract_json::<JsonValue>(attrs, keys::MLFLOW_CHAT_TOKEN_USAGE) {
            if !input_supplied {
                if let Some(v) = usage
                    .get("prompt_tokens")
                    .or_else(|| usage.get("input_tokens"))
                    .and_then(|v| v.as_i64())
                {
                    span.gen_ai_usage_input_tokens = v;
                    input_supplied = true;
                }
            }
            if !output_supplied {
                if let Some(v) = usage
                    .get("completion_tokens")
                    .or_else(|| usage.get("output_tokens"))
                    .and_then(|v| v.as_i64())
                {
                    span.gen_ai_usage_output_tokens = v;
                    output_supplied = true;
                }
            }
        }
    }

    // Google ADK: tokens from llm_response JSON.
    //
    // Entered when *either* counter is missing, and each field is then filled independently. Requiring both
    // to be zero meant a span that reported only one flat counter - input 100, output absent - kept the other
    // at 0 even though `usage_metadata.candidates_token_count` had it, understating the total and the cost.
    if !input_supplied || !output_supplied {
        if let Some(resp) = extract_json::<JsonValue>(attrs, keys::GCP_VERTEX_LLM_RESPONSE) {
            if let Some(usage) = resp.get("usage_metadata") {
                if !input_supplied {
                    if let Some(v) = usage.get("prompt_token_count").and_then(|v| v.as_i64()) {
                        span.gen_ai_usage_input_tokens = v;
                        input_supplied = true;
                    }
                }
                if !output_supplied {
                    if let Some(v) = usage.get("candidates_token_count").and_then(|v| v.as_i64()) {
                        span.gen_ai_usage_output_tokens = v;
                        output_supplied = true;
                    }
                }
            }
        }
    }

    // Logfire: tokens from response_data.usage JSON
    // Anthropic: {input_tokens, output_tokens, cache_read_input_tokens, cache_creation_input_tokens}
    // OpenAI: {prompt_tokens, completion_tokens}
    // Same rule as the ADK block: either counter missing enters, and each is filled on its own, so a span
    // reporting only one flat counter still gets the other from the JSON.
    if !input_supplied || !output_supplied {
        if let Some(resp) = extract_json::<JsonValue>(attrs, keys::RESPONSE_DATA) {
            if let Some(usage) = resp.get("usage") {
                if !input_supplied {
                    if let Some(v) = usage
                        .get("input_tokens")
                        .or_else(|| usage.get("prompt_tokens"))
                        .and_then(|v| v.as_i64())
                    {
                        span.gen_ai_usage_input_tokens = v;
                        input_supplied = true;
                    }
                }
                if !output_supplied {
                    if let Some(v) = usage
                        .get("output_tokens")
                        .or_else(|| usage.get("completion_tokens"))
                        .and_then(|v| v.as_i64())
                    {
                        span.gen_ai_usage_output_tokens = v;
                        output_supplied = true;
                    }
                }
            }
        }
    }

    // Read after the last source, which is also what keeps every source maintaining the tracker: a chain
    // whose final link need not update it is a chain the next link will not either. A generation span with no
    // counter at all is worth seeing - it bills as free, and the usual cause is an instrumentation version
    // that moved its attribute names.
    // Held rather than applied: the total is synthesised once, at the end, from every counter the
    // fallbacks below may still fill in. Computed here it could only ever floor at input + output, which
    // is *below* the true total for a provider that reports its cache counters beside them.
    // Presence, not the value, for the total as well: with only the number, "the provider said 1,100" and
    // "nobody said anything" are the same 0, and a framework fallback's `max` could then raise a total the
    // provider had stated explicitly.
    let total_supplied = TOTAL_TOKENS
        .extract_opt_for_span(attrs, span_name)
        .is_some();
    let mut reported_total = TOTAL_TOKENS.extract(attrs);
    // Presence, not the value: a reported `0` is a fact the framework fallbacks must not overwrite, exactly
    // as for the input and output sides.
    //
    // `mut`, because a *later source* supplying the counter makes it supplied too. As a record of "a flat
    // attribute existed", the Logfire path below could fill in a cache read of 17 and the CrewAI path then
    // overwrite it with 100 - the flag has to describe the span, not one of the sources that write to it.
    let mut cache_read_supplied = CACHE_READ_TOKENS
        .extract_opt_for_span(attrs, span_name)
        .is_some();
    let mut cache_write_supplied = CACHE_WRITE_TOKENS
        .extract_opt_for_span(attrs, span_name)
        .is_some();
    span.gen_ai_usage_cache_read_tokens = CACHE_READ_TOKENS.extract_for_span(attrs, span_name);
    span.gen_ai_usage_cache_write_tokens = CACHE_WRITE_TOKENS.extract_for_span(attrs, span_name);
    span.gen_ai_usage_reasoning_tokens = REASONING_TOKENS.extract(attrs);

    // Logfire: cache tokens from response_data.usage (after flat attribute extraction).
    //
    // Gated on presence, not on the value being zero - a reported `0` is a fact about the call, and testing
    // the value replaced it with whatever this payload said, inflating both the total and the cache charge.
    // The same conflation the input and output sides had.
    if !cache_read_supplied || !cache_write_supplied {
        if let Some(resp) = extract_json::<JsonValue>(attrs, keys::RESPONSE_DATA) {
            if let Some(usage) = resp.get("usage") {
                if !cache_read_supplied
                    && let Some(v) = usage
                        .get("cache_read_input_tokens")
                        .and_then(|v| v.as_i64())
                {
                    span.gen_ai_usage_cache_read_tokens = v;
                    cache_read_supplied = true;
                }
                if !cache_write_supplied
                    && let Some(v) = usage
                        .get("cache_creation_input_tokens")
                        .and_then(|v| v.as_i64())
                {
                    span.gen_ai_usage_cache_write_tokens = v;
                    cache_write_supplied = true;
                }
            }
        }
    }

    // CrewAI: tokens from output.value JSON (CrewOutput.token_usage)
    // CrewAI embeds token usage in the serialized CrewOutput object, not as flat attributes.
    //
    // Gated on what was *supplied*, per side, rather than on the stored value being zero. A zero is two
    // different facts - "the provider said 0" and "nobody said anything" - and testing it conflated them in
    // both directions: with a flat input of 200 and no output attribute the whole fallback was skipped, so
    // the output stayed 0 and its cost was never charged; and an explicit flat `0/0` was overwritten by
    // whatever the fallback found. The `*_supplied` flags exist precisely to tell those apart.
    // The *outer* gate is only "is this CrewAI": the block also carries a cache counter and a reported
    // total, and neither is reachable through a gate that asks whether a *side* is missing. With both flat
    // sides present, `{prompt:500, completion:600, total:2000, cached:100}` stored a total of 1,100 and no
    // cache at all. Each value inside decides for itself whether it was already supplied.
    {
        let is_crewai = attrs.contains_key("crew_key")
            || attrs.contains_key("crew_id")
            || attrs.contains_key("crew_tasks")
            || attrs.contains_key("task_key");
        if is_crewai {
            if let Some(output) = extract_json::<JsonValue>(attrs, keys::OUTPUT_VALUE) {
                if let Some(usage) = output.get("token_usage") {
                    let mut took_input = false;
                    let mut took_output = false;
                    if !input_supplied
                        && let Some(v) = usage.get("prompt_tokens").and_then(|v| v.as_i64())
                    {
                        span.gen_ai_usage_input_tokens = v;
                        input_supplied = true;
                        took_input = true;
                    }
                    if !output_supplied
                        && let Some(v) = usage.get("completion_tokens").and_then(|v| v.as_i64())
                    {
                        span.gen_ai_usage_output_tokens = v;
                        output_supplied = true;
                        took_output = true;
                    }
                    if !cache_read_supplied
                        && let Some(v) = usage.get("cached_prompt_tokens").and_then(|v| v.as_i64())
                    {
                        span.gen_ai_usage_cache_read_tokens = v;
                        cache_read_supplied = true;
                    }
                    // The embedded total describes the embedded *parts*, so it is usable exactly when the
                    // parts actually stored are those parts - either this payload supplied a side, or the
                    // flat attribute already agreed with it. Requiring that *this* payload supplied both
                    // discarded a perfectly good total whenever one side happened to be reported twice with
                    // the same value; taking it regardless produced a row whose total did not match its own
                    // input and output, claiming 1,099 for 0 + 100 tokens.
                    let side_agrees = |took: bool, stored: i64, key: &str| {
                        took || usage.get(key).and_then(|v| v.as_i64()) == Some(stored)
                    };
                    // And only when the provider did not state a total itself: `max` against an explicit
                    // flat total can only raise it, which replaces the provider's own statement about the
                    // call with the framework's - flat `500/600` and a flat total of 1,100 became 2,000.
                    if !total_supplied
                        && side_agrees(took_input, span.gen_ai_usage_input_tokens, "prompt_tokens")
                        && side_agrees(
                            took_output,
                            span.gen_ai_usage_output_tokens,
                            "completion_tokens",
                        )
                    {
                        reported_total = usage
                            .get("total_tokens")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0)
                            .max(reported_total);
                    }
                }
            }
        }
    }

    // AutoGen: tokens from output.value.messages[].models_usage on chain spans.
    // The models_usage field is AutoGen-specific; safe to check without framework guard.
    if !input_supplied || !output_supplied {
        let (pt, ct) = extract_autogen_tokens(attrs);
        if pt > 0 || ct > 0 {
            // Per side, for the same reason as the CrewAI fallback above.
            if !input_supplied {
                span.gen_ai_usage_input_tokens = pt;
                input_supplied = true;
            }
            if !output_supplied {
                span.gen_ai_usage_output_tokens = ct;
                output_supplied = true;
            }
        }
    }

    // After every fallback, not before them: reported ahead of the CrewAI and AutoGen paths it announced
    // "no counters" for spans those paths went on to fill in.
    //
    // Every counter, not only the two sides: a span reporting cache tokens and nothing else is billed, so
    // announcing it as free would be wrong - and reading each flag here is also what keeps every source
    // maintaining it, since a chain whose final link need not update the tracker is a chain the next link
    // will not either. That is exactly how `cache_read_supplied` came to mean "a flat attribute existed"
    // rather than "the span has one", letting a later source overwrite an earlier source's value.
    if !input_supplied && !output_supplied && !cache_read_supplied && !cache_write_supplied {
        tracing::trace!(
            span_name,
            "No token counters were supplied by any source; this span bills as free"
        );
    }

    // The synthesised floor counts what the provider reports *beside* its input and output, and counts
    // nothing it reports inside them. The convention is `pricing`'s, not a second copy of it: a total that
    // assumes the cache counters are already in the input while the charge assumes they are extra describes
    // two different calls, and the one on screen would contradict the money. So an Anthropic response with
    // `input=10, cache_write=17_649, output=205` totals 17_864 rather than 215, and an OpenAI one with
    // `input=1_000` of which `cached=800` still totals its own input.
    let counted_beside_input =
        if pricing::cache_counters_are_separate(span.gen_ai_system.as_deref()) {
            span.gen_ai_usage_cache_read_tokens + span.gen_ai_usage_cache_write_tokens
        } else {
            0
        };
    let counted_beside_output = if pricing::reasoning_is_separate(span.gen_ai_system.as_deref()) {
        span.gen_ai_usage_reasoning_tokens
    } else {
        0
    };
    span.gen_ai_usage_total_tokens_reported = reported_total;
    span.gen_ai_usage_total_tokens = reported_total.max(
        span.gen_ai_usage_input_tokens
            + span.gen_ai_usage_output_tokens
            + counted_beside_input
            + counted_beside_output,
    );

    // Usage details (remaining gen_ai.usage.* fields)
    let mut details = serde_json::Map::new();
    for (key, value) in attrs {
        if let Some(field) = key.strip_prefix("gen_ai.usage.")
            && !KNOWN_USAGE_FIELDS.contains(&field)
        {
            let json_val = value
                .parse::<i64>()
                .map(|n| json!(n))
                .or_else(|_| value.parse::<f64>().map(|n| json!(n)))
                .unwrap_or_else(|_| json!(value));
            details.insert(field.to_string(), json_val);
        }
    }
    span.gen_ai_usage_details = if details.is_empty() {
        JsonValue::Null
    } else {
        JsonValue::Object(details)
    };

    // Pre-calculated costs (OpenInference llm.cost.* attributes)
    span.extracted_cost_total = parse_opt(attrs, keys::LLM_COST_TOTAL);
    span.extracted_cost_input = parse_opt(attrs, keys::LLM_COST_PROMPT);
    span.extracted_cost_output = parse_opt(attrs, keys::LLM_COST_COMPLETION);
}

#[cfg(test)]
#[path = "attributes_tests.rs"]
mod tests;
