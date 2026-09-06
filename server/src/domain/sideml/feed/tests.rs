//! Tests for the feed pipeline.
//!
//! These tests verify the correctness of the message processing pipeline
//! using the new flattened block structure.

use chrono::Utc;
use serde_json::json;

use super::*;
use crate::data::types::MessageSpanRow;
use crate::domain::sideml::types::{ChatRole, ContentBlock, FinishReason};

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Fixed timestamp for tests (2025-01-01T00:00:00Z)
fn fixed_time() -> chrono::DateTime<Utc> {
    chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

fn make_span_row(
    trace_id: &str,
    span_id: &str,
    parent_span_id: Option<&str>,
    messages_json: &str,
    tool_definitions_json: &str,
    tool_names_json: &str,
) -> MessageSpanRow {
    // Use fixed_time() to match the timestamps in test JSON messages
    let ts = fixed_time();
    MessageSpanRow {
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        parent_span_id: parent_span_id.map(String::from),
        span_timestamp: ts,
        span_end_timestamp: None,
        messages_json: messages_json.to_string(),
        tool_definitions_json: tool_definitions_json.to_string(),
        tool_names_json: tool_names_json.to_string(),
        model: Some("gpt-4".to_string()),
        provider: Some("openai".to_string()),
        status_code: None,
        exception_type: None,
        exception_message: None,
        exception_stacktrace: None,
        input_tokens: 100,
        output_tokens: 50,
        total_tokens: 150,
        cost_total: 0.01,
        observation_type: None,
        session_id: None,
        ingested_at: ts,
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

/// Create a span row with explicit timestamps for dedup-aware tests
fn make_span_row_with_timestamps(
    trace_id: &str,
    span_id: &str,
    parent_span_id: Option<&str>,
    messages_json: &str,
    span_start: chrono::DateTime<Utc>,
    span_end: Option<chrono::DateTime<Utc>>,
) -> MessageSpanRow {
    // Default to "generation" for LLM spans to enable history detection
    make_span_row_full(
        trace_id,
        span_id,
        parent_span_id,
        messages_json,
        span_start,
        span_end,
        Some("generation"),
    )
}

/// Create a span row with full control over all fields
fn make_span_row_full(
    trace_id: &str,
    span_id: &str,
    parent_span_id: Option<&str>,
    messages_json: &str,
    span_start: chrono::DateTime<Utc>,
    span_end: Option<chrono::DateTime<Utc>>,
    observation_type: Option<&str>,
) -> MessageSpanRow {
    MessageSpanRow {
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        parent_span_id: parent_span_id.map(String::from),
        span_timestamp: span_start,
        span_end_timestamp: span_end,
        messages_json: messages_json.to_string(),
        tool_definitions_json: "[]".to_string(),
        tool_names_json: "[]".to_string(),
        model: Some("gpt-4".to_string()),
        provider: Some("openai".to_string()),
        status_code: None,
        exception_type: None,
        exception_message: None,
        exception_stacktrace: None,
        input_tokens: 100,
        output_tokens: 50,
        total_tokens: 150,
        cost_total: 0.01,
        observation_type: observation_type.map(String::from),
        session_id: None,
        ingested_at: span_start,
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

#[allow(dead_code)]
fn get_text(block: &ContentBlock) -> Option<&str> {
    match block {
        ContentBlock::Text { text } => Some(text.as_str()),
        _ => None,
    }
}

// ============================================================================
// BASIC TESTS
// ============================================================================

#[test]
fn test_process_spans_empty() {
    let rows = vec![];
    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    assert!(result.messages.is_empty());
    assert!(result.tool_definitions.is_empty());
    assert!(result.tool_names.is_empty());
}

#[test]
fn test_process_spans_simple_message() {
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {"role": "user", "content": "Hello"}
    }]);

    let row = make_span_row("trace1", "span1", None, &msg.to_string(), "[]", "[]");
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    assert_eq!(result.messages.len(), 1);
    assert_eq!(result.messages[0].role, ChatRole::User);
    // Content is now a single block
    assert!(matches!(&result.messages[0].content, ContentBlock::Text { text } if text == "Hello"));
}

#[test]
fn test_process_spans_flattening() {
    // Test that multiple content blocks in one message become multiple BlockEntries
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.assistant.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "First"},
                {"type": "text", "text": "Second"}
            ]
        }
    }]);

    let row = make_span_row("trace1", "span1", None, &msg.to_string(), "[]", "[]");
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    // Should have 2 blocks (one per content block)
    assert_eq!(result.messages.len(), 2);
    assert_eq!(result.messages[0].role, ChatRole::Assistant);
    assert_eq!(result.messages[1].role, ChatRole::Assistant);
    assert_eq!(result.messages[0].entry_index, 0);
    assert_eq!(result.messages[1].entry_index, 1);

    // Verify content
    assert!(matches!(&result.messages[0].content, ContentBlock::Text { text } if text == "First"));
    assert!(matches!(&result.messages[1].content, ContentBlock::Text { text } if text == "Second"));
}

#[test]
fn test_deduplicate_tools() {
    let tools = vec![
        json!({"type": "function", "function": {"name": "tool_a", "description": "A"}}),
        json!({"type": "function", "function": {"name": "tool_b", "description": "B"}}),
        json!({"type": "function", "function": {"name": "tool_a", "description": "A again"}}),
    ];

    let deduped = deduplicate_tools(tools);
    assert_eq!(deduped.len(), 2);

    let names: Vec<_> = deduped
        .iter()
        .filter_map(|t| t.get("function")?.get("name")?.as_str())
        .collect();
    assert_eq!(names, vec!["tool_a", "tool_b"]);
}

#[test]
fn test_deduplicate_tools_prefers_richer_definition() {
    let tools = vec![
        json!({"type": "function", "function": {"name": "tool_a"}}),
        json!({
            "type": "function",
            "function": {
                "name": "tool_a",
                "description": "Richer definition",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string"}
                    }
                }
            }
        }),
    ];

    let deduped = deduplicate_tools(tools);
    assert_eq!(deduped.len(), 1);

    let func = &deduped[0]["function"];
    assert_eq!(func["name"].as_str(), Some("tool_a"));
    assert_eq!(func["description"].as_str(), Some("Richer definition"));
    assert!(func.get("parameters").is_some());
}

#[test]
fn test_deduplicate_tools_merges_complementary_fields() {
    let tools = vec![
        json!({
            "type": "function",
            "function": {
                "name": "tool_a",
                "description": "Weather tool"
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "tool_a",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string"}
                    }
                }
            }
        }),
    ];

    let deduped = deduplicate_tools(tools);
    assert_eq!(deduped.len(), 1);

    let func = &deduped[0]["function"];
    assert_eq!(func["name"].as_str(), Some("tool_a"));
    assert_eq!(func["description"].as_str(), Some("Weather tool"));
    assert_eq!(
        func["parameters"]["properties"]["city"]["type"].as_str(),
        Some("string")
    );
}

#[test]
fn test_deduplicate_tools_merges_parameter_properties_and_required() {
    let tools = vec![
        json!({
            "type": "function",
            "function": {
                "name": "tool_a",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string"}
                    },
                    "required": ["city"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "tool_a",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "days": {"type": "integer"}
                    },
                    "required": ["days"]
                }
            }
        }),
    ];

    let deduped = deduplicate_tools(tools);
    assert_eq!(deduped.len(), 1);

    let params = &deduped[0]["function"]["parameters"];
    assert_eq!(
        params["properties"]["city"]["type"].as_str(),
        Some("string")
    );
    assert_eq!(
        params["properties"]["days"]["type"].as_str(),
        Some("integer")
    );

    let required = params["required"].as_array().unwrap();
    assert!(required.contains(&json!("city")));
    assert!(required.contains(&json!("days")));
}

#[test]
fn test_deduplicate_names() {
    let names = vec![
        "tool_b".to_string(),
        "tool_a".to_string(),
        "tool_b".to_string(),
        "tool_c".to_string(),
    ];

    let deduped = deduplicate_names(names);
    assert_eq!(deduped, vec!["tool_a", "tool_b", "tool_c"]);
}

#[test]
fn test_role_filter() {
    let msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
            "content": {"role": "user", "content": "User message"}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": "2025-01-01T00:00:01Z"}},
            "content": {"role": "assistant", "content": "Assistant message"}
        }
    ]);

    let row = make_span_row("trace1", "span1", None, &msg.to_string(), "[]", "[]");

    // Filter for user messages only
    let options = FeedOptions::new().with_role(Some("user".to_string()));
    let result = process_spans(vec![row], &options);

    assert_eq!(result.messages.len(), 1);
    assert_eq!(result.messages[0].role, ChatRole::User);
}

/// The role filter must be a view over the finished feed, not a stage inside it.
///
/// Filtering during flattening removed blocks the later stages read. `role=tool` deleted the
/// assistant tool calls that correlation uses to give an id-less result its call's id; the two
/// results then fell back to content identity, which is the same for both, and dedup collapsed
/// them - so asking for the tool messages returned *fewer* than the unfiltered feed contains.
///
/// Stated as a property rather than a fixed count: for every role, filtering must return exactly
/// the blocks of that role the unfiltered feed returns.
#[test]
fn role_filter_returns_exactly_the_unfiltered_blocks_of_that_role() {
    // Two sequential calls to the same tool whose results are byte-identical and carry no id -
    // the Gemini/ADK shape. Distinguishable only through their calls.
    let msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
            "content": {"role": "user", "content": "Weather in Paris and Lyon?"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:01Z"}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call-paris", "name": "get_weather",
                             "input": {"city": "Paris"}}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": "2025-01-01T00:00:02Z"}},
            "content": {
                "role": "user",
                "content": [{"type": "tool_result", "name": "get_weather", "content": "sunny"}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:03Z"}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call-lyon", "name": "get_weather",
                             "input": {"city": "Lyon"}}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": "2025-01-01T00:00:04Z"}},
            "content": {
                "role": "user",
                "content": [{"type": "tool_result", "name": "get_weather", "content": "sunny"}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:05Z"}},
            "content": {"role": "assistant", "content": "Both are sunny."}
        }
    ]);

    let row = || make_span_row("trace1", "span1", None, &msg.to_string(), "[]", "[]");

    let unfiltered = process_spans(vec![row()], &FeedOptions::new());
    assert_eq!(
        unfiltered
            .messages
            .iter()
            .filter(|b| b.role == ChatRole::Tool)
            .count(),
        2,
        "the fixture must produce two distinct tool results, or it cannot detect the collapse"
    );

    for role in ["user", "assistant", "tool", "system"] {
        let expected: Vec<String> = unfiltered
            .messages
            .iter()
            .filter(|b| b.role.as_str() == role)
            .map(|b| format!("{:?}", b.content))
            .collect();

        let filtered = process_spans(
            vec![row()],
            &FeedOptions::new().with_role(Some(role.to_string())),
        );
        let actual: Vec<String> = filtered
            .messages
            .iter()
            .map(|b| format!("{:?}", b.content))
            .collect();

        assert_eq!(
            actual, expected,
            "?role={role} must return exactly the {role} blocks of the unfiltered feed"
        );
        assert_eq!(
            filtered.metadata.block_count,
            filtered.messages.len(),
            "?role={role} reported a block count that does not match what it returned"
        );
    }
}

#[test]
fn test_block_entry_metadata() {
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.assistant.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {
            "role": "assistant",
            "content": "Test content"
        }
    }]);

    let mut row = make_span_row("trace1", "span1", None, &msg.to_string(), "[]", "[]");
    row.session_id = Some("session1".to_string());
    row.status_code = Some("OK".to_string());

    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    assert_eq!(result.messages.len(), 1);
    let block = &result.messages[0];

    assert_eq!(block.trace_id, "trace1");
    assert_eq!(block.span_id, "span1");
    assert_eq!(block.session_id, Some("session1".to_string()));
    assert_eq!(block.model, Some("gpt-4".to_string()));
    assert_eq!(block.provider, Some("openai".to_string()));
    assert_eq!(block.status_code, Some("OK".to_string()));
    assert!(!block.is_error);
    assert_eq!(block.entry_type, "text");
    assert!(!block.content_hash.is_empty());
}

#[test]
fn test_span_path_computation() {
    // Create a hierarchy: root -> child -> grandchild
    // Each span has a DIFFERENT message to avoid deduplication
    let msg_root = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {"role": "user", "content": "Root message"}
    }]);
    let msg_child = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:01Z"}},
        "content": {"role": "user", "content": "Child message"}
    }]);
    let msg_grandchild = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:02Z"}},
        "content": {"role": "user", "content": "Grandchild message"}
    }]);

    let rows = vec![
        make_span_row("trace1", "root", None, &msg_root.to_string(), "[]", "[]"),
        make_span_row(
            "trace1",
            "child",
            Some("root"),
            &msg_child.to_string(),
            "[]",
            "[]",
        ),
        make_span_row(
            "trace1",
            "grandchild",
            Some("child"),
            &msg_grandchild.to_string(),
            "[]",
            "[]",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Find blocks by span_id
    let root_block = result
        .messages
        .iter()
        .find(|b| b.span_id == "root")
        .unwrap();
    let child_block = result
        .messages
        .iter()
        .find(|b| b.span_id == "child")
        .unwrap();
    let grandchild_block = result
        .messages
        .iter()
        .find(|b| b.span_id == "grandchild")
        .unwrap();

    // Verify span_path
    assert_eq!(root_block.span_path, vec!["root"]);
    assert_eq!(child_block.span_path, vec!["root", "child"]);
    assert_eq!(
        grandchild_block.span_path,
        vec!["root", "child", "grandchild"]
    );
}

#[test]
fn test_tool_use_extraction() {
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.assistant.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {
            "role": "assistant",
            "content": [{
                "type": "tool_use",
                "id": "call_123",
                "name": "search",
                "input": {"query": "test"}
            }]
        }
    }]);

    let row = make_span_row("trace1", "span1", None, &msg.to_string(), "[]", "[]");
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    assert_eq!(result.messages.len(), 1);
    let block = &result.messages[0];

    assert_eq!(block.entry_type, "tool_use");
    assert_eq!(block.tool_use_id, Some("call_123".to_string()));
    assert_eq!(block.tool_name, Some("search".to_string()));

    match &block.content {
        ContentBlock::ToolUse { id, name, input } => {
            assert_eq!(id, &Some("call_123".to_string()));
            assert_eq!(name, "search");
            assert_eq!(input.get("query").unwrap().as_str(), Some("test"));
        }
        _ => panic!("Expected ToolUse content block"),
    }
}

#[test]
fn test_tool_result_extraction() {
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {
            "role": "tool",
            "tool_use_id": "call_123",
            "content": "Tool output"
        }
    }]);

    let row = make_span_row("trace1", "span1", None, &msg.to_string(), "[]", "[]");
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    assert_eq!(result.messages.len(), 1);
    let block = &result.messages[0];

    assert_eq!(block.role, ChatRole::Tool);
    assert_eq!(block.tool_use_id, Some("call_123".to_string()));
}

#[test]
fn test_sorting_by_timestamp_message_entry() {
    // Test that blocks are sorted by (timestamp, message_index, entry_index)
    let msg1 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {"role": "user", "content": "First"}
    }]);
    let msg2 = json!([{
        "source": {"event": {"name": "gen_ai.assistant.message", "time": "2025-01-01T00:00:01Z"}},
        "content": {"role": "assistant", "content": "Second"}
    }]);

    let row1 = make_span_row("trace1", "span1", None, &msg1.to_string(), "[]", "[]");
    let row2 = make_span_row("trace1", "span2", None, &msg2.to_string(), "[]", "[]");

    let options = FeedOptions::default();
    let result = process_spans(vec![row2, row1], &options); // Note: reversed order

    assert_eq!(result.messages.len(), 2);
    // Should be sorted by timestamp ASC
    assert!(matches!(&result.messages[0].content, ContentBlock::Text { text } if text == "First"));
    assert!(matches!(&result.messages[1].content, ContentBlock::Text { text } if text == "Second"));
}

#[test]
fn test_metadata() {
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {"role": "user", "content": "Test"}
    }]);

    let row = make_span_row("trace1", "span1", None, &msg.to_string(), "[]", "[]");
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    assert_eq!(result.metadata.block_count, 1);
    assert_eq!(result.metadata.span_count, 1);
    assert_eq!(result.metadata.total_tokens, 150);
    assert!((result.metadata.total_cost - 0.01).abs() < 0.001);
}

// ============================================================================
// DEDUPLICATION INTEGRATION TESTS
// ============================================================================
// Unit tests for dedup logic are in dedup.rs. These tests verify pipeline integration.

#[test]
fn test_thinking_blocks_preserved() {
    // Test that thinking blocks (enrichment content) are preserved
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:00Z"}},
        "content": {
            "role": "assistant",
            "content": [
                {"type": "thinking", "text": "Let me think about this..."},
                {"type": "text", "text": "Here is my answer"}
            ],
            "finish_reason": "stop"
        }
    }]);

    let row = make_span_row("trace1", "span1", None, &msg.to_string(), "[]", "[]");
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    // Should have 2 blocks: thinking + text
    assert_eq!(result.messages.len(), 2);
    assert_eq!(result.messages[0].entry_type, "thinking");
    assert_eq!(result.messages[1].entry_type, "text");
}

#[test]
fn test_history_deduplication() {
    // Test that duplicate history messages are automatically deduplicated
    // Child span has history (user message) that should be filtered as a duplicate
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    let root_msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Original question"}
    }]);

    let child_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Original question"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "The answer", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "root", None, &root_msg.to_string(), t0, Some(t0)),
        make_span_row_with_timestamps(
            "trace1",
            "child",
            Some("root"),
            &child_msg.to_string(),
            t0,
            Some(t1),
        ),
    ];

    // History is automatically detected and deduplicated
    let options = FeedOptions::new();
    let result = process_spans(rows, &options);

    // Root's user message + child's assistant message (duplicate user message filtered)
    assert_eq!(result.messages.len(), 2);
    assert_eq!(result.messages[0].role, ChatRole::User);
    assert_eq!(result.messages[1].role, ChatRole::Assistant);
}

#[test]
fn test_process_feed_multiple_sessions() {
    // Test process_feed with multiple sessions
    let msg1 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {"role": "user", "content": "Session 1 message"}
    }]);

    let msg2 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:01Z"}},
        "content": {"role": "user", "content": "Session 2 message"}
    }]);

    let mut row1 = make_span_row("trace1", "span1", None, &msg1.to_string(), "[]", "[]");
    row1.session_id = Some("session1".to_string());

    let mut row2 = make_span_row("trace2", "span2", None, &msg2.to_string(), "[]", "[]");
    row2.session_id = Some("session2".to_string());

    let options = FeedOptions::default();
    let result = process_feed(vec![row1, row2], &options);

    // Both sessions should be processed
    assert_eq!(result.messages.len(), 2);
}

#[test]
fn test_process_feed_same_batch_ordering() {
    // Feed uses DESC order (newest first), but within same-batch blocks
    // (same span + same timestamp), text should still come before tool_use
    let t0 = "2025-01-01T00:00:00Z";

    let messages = json!([
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t0}},
            "content": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "I'll search for that"},
                    {"type": "tool_use", "id": "call_1", "name": "search", "input": {"q": "test"}}
                ],
                "finish_reason": "tool_use"
            }
        }
    ]);

    let mut row = make_span_row("trace1", "span1", None, &messages.to_string(), "[]", "[]");
    row.session_id = Some("session1".to_string());

    let options = FeedOptions::default();
    let result = process_feed(vec![row], &options);

    // Should have text and tool_use
    assert_eq!(result.messages.len(), 2);

    // Text should come before tool_use (same-batch ordering preserved)
    assert_eq!(result.messages[0].entry_type, "text");
    assert_eq!(result.messages[1].entry_type, "tool_use");
}

#[test]
fn test_span_end_timestamp_used_for_output_ordering() {
    // Test that span_end_timestamp is used for OUTPUT message ordering
    // even when event time is earlier
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:00Z"}},
        "content": {"role": "assistant", "content": "Response", "finish_reason": "stop"}
    }]);

    let mut row = make_span_row("trace1", "span1", None, &msg.to_string(), "[]", "[]");
    // Set span_end_timestamp to later time
    row.span_end_timestamp = Some(
        chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:05Z")
            .unwrap()
            .with_timezone(&Utc),
    );

    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    assert_eq!(result.messages.len(), 1);
    // The block should exist and be processed correctly
    assert_eq!(result.messages[0].role, ChatRole::Assistant);
}

// ============================================================================
// REGRESSION TESTS FOR DEDUPLICATION ISSUES
// ============================================================================

/// Helper to create a span row with observation_type for tool spans
fn make_tool_span_row(
    trace_id: &str,
    span_id: &str,
    parent_span_id: Option<&str>,
    messages_json: &str,
    span_start: chrono::DateTime<Utc>,
    span_end: Option<chrono::DateTime<Utc>>,
) -> MessageSpanRow {
    let mut row = make_span_row_with_timestamps(
        trace_id,
        span_id,
        parent_span_id,
        messages_json,
        span_start,
        span_end,
    );
    row.observation_type = Some("tool".to_string());
    row
}

// ----------------------------------------------------------------------------
// ISSUE 1: Historical Context Leaking as New Messages
// ----------------------------------------------------------------------------
// When an LLM span includes conversation history from previous turns,
// those messages appear as separate entries in the feed.

#[test]
fn test_regression_historical_context_not_leaked() {
    // Scenario: Agent trace where user asks about LA, but history includes NYC data
    // The NYC messages should NOT appear in the feed for the LA request
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);

    // Root span: user asks about LA
    let root_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.system.message", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "You are a weather assistant."}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "What's the weather in Los Angeles?"}
        }
    ]);

    // Child LLM span includes history from previous turn (NYC) as context
    // This is what the LLM received, but shouldn't be in final feed
    let child_msg = json!([
        // Historical context (previous turn about NYC)
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "What's the weather in New York?"}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "New York is sunny today."}
        },
        // Current turn
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "What's the weather in Los Angeles?"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Los Angeles is warm and sunny.", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "root", None, &root_msg.to_string(), t0, Some(t2)),
        make_span_row_with_timestamps(
            "trace1",
            "child",
            Some("root"),
            &child_msg.to_string(),
            t1,
            Some(t2),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Should NOT include NYC messages - they're historical context
    let texts: Vec<_> = result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        !texts.iter().any(|t| t.contains("New York")),
        "Historical NYC messages should not appear in feed. Found: {:?}",
        texts
    );

    // Should only have: system, user (LA question), assistant (LA answer)
    assert_eq!(result.messages.len(), 3);
}

// ----------------------------------------------------------------------------
// ISSUE 2: Tool Results Not Deduplicating Due to Structure Differences
// ----------------------------------------------------------------------------
// Same tool result appears with different JSON structures in different spans,
// causing hash mismatch and duplicate entries.

#[test]
fn test_regression_tool_result_structure_dedup() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);

    // Tool execution span: result as direct object
    let tool_span_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "tool",
                "tool_use_id": "call_123",
                "content": {"result": "sunny", "temp": 25}
            }
        }
    ]);

    // Chat span receives tool result as array with type wrapper
    let chat_span_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "tool",
                "tool_use_id": "call_123",
                "content": [{"type": "json", "data": {"json": {"result": "sunny", "temp": 25}}}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
            "content": {"role": "assistant", "content": "The weather is sunny.", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_tool_span_row(
            "trace1",
            "tool_span",
            Some("chat_span"),
            &tool_span_msg.to_string(),
            t1,
            Some(t1),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "chat_span",
            Some("root"),
            &chat_span_msg.to_string(),
            t0,
            Some(t2),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Count tool results
    let tool_results: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.entry_type == "tool_result")
        .collect();

    // Should be 1 tool result, not 2
    assert_eq!(
        tool_results.len(),
        1,
        "Same tool result with different structure should deduplicate. Found {} instead of 1",
        tool_results.len()
    );
}

// ----------------------------------------------------------------------------
// ISSUE 3: Text Messages Not Deduplicating Due to Whitespace
// ----------------------------------------------------------------------------
// Same text with trailing newline hashes differently, causing duplicates.

#[test]
fn test_regression_text_whitespace_dedup() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Child span: response without trailing newline
    let child_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
        "content": {"role": "assistant", "content": "Hello, world!", "finish_reason": "stop"}
    }]);

    // Root span: aggregated response with trailing newline
    let root_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
        "content": {"role": "assistant", "content": "Hello, world!\n", "finish_reason": "stop"}
    }]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "root", None, &root_msg.to_string(), t0, Some(t1)),
        make_span_row_with_timestamps(
            "trace1",
            "child",
            Some("root"),
            &child_msg.to_string(),
            t0,
            Some(t1),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Count assistant responses
    let assistant_msgs: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();

    // Should be 1 response, not 2
    assert_eq!(
        assistant_msgs.len(),
        1,
        "Same text with/without trailing newline should deduplicate. Found {}",
        assistant_msgs.len()
    );
}

// ----------------------------------------------------------------------------
// ISSUE 4: Wrong Message Ordering (Tool Use After Tool Result)
// ----------------------------------------------------------------------------
// Tool use blocks appear after tool result blocks in the output.

#[test]
fn test_regression_tool_ordering() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100);
    let t2 = t0 + chrono::Duration::milliseconds(200);
    let t3 = t0 + chrono::Duration::milliseconds(300);

    // LLM span with tool use output
    let llm_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Search for cats"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_1", "name": "search", "input": {"q": "cats"}}],
                "finish_reason": "tool_use"
            }
        }
    ]);

    // Second LLM span: receives tool result as history, outputs final response
    // The tool result is recorded here even though tool execution happened elsewhere
    let llm2_msg = json!([
        // History includes the tool result
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t2.to_rfc3339()}},
            "content": {"role": "tool", "tool_use_id": "call_1", "content": "Found cats!"}
        },
        // Final output
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t3.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Here are the cats!", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_span_row_with_timestamps(
            "trace1",
            "llm1",
            Some("root"),
            &llm_msg.to_string(),
            t0,
            Some(t1),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "llm2",
            Some("root"),
            &llm2_msg.to_string(),
            t2,
            Some(t3),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Find positions
    let tool_use_pos = result
        .messages
        .iter()
        .position(|m| m.entry_type == "tool_use");
    let tool_result_pos = result
        .messages
        .iter()
        .position(|m| m.entry_type == "tool_result");

    assert!(
        tool_use_pos.is_some() && tool_result_pos.is_some(),
        "Should have both tool_use and tool_result. Types found: {:?}",
        result
            .messages
            .iter()
            .map(|m| &m.entry_type)
            .collect::<Vec<_>>()
    );

    assert!(
        tool_use_pos.unwrap() < tool_result_pos.unwrap(),
        "tool_use (pos {}) should come before tool_result (pos {})",
        tool_use_pos.unwrap(),
        tool_result_pos.unwrap()
    );
}

// ----------------------------------------------------------------------------
// ISSUE 5: Spurious Json Block from Tool Input
// ----------------------------------------------------------------------------
// Tool input appears as a separate Json message entry.
// This matches the pattern seen in trace 959d2590050265486b5f3a55ae3e2b71
// where span 2983bb7075c0d081 has a Json block with {"city": "Los Angeles", "days": 7}

#[test]
fn test_regression_no_spurious_tool_input_block() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Tool span with Strands-style events:
    // - tool_handler.invoke event with input params
    // - tool_handler.result event with output
    let tool_msg = json!([
        {
            "source": {"event": {"name": "tool_handler.invoke", "time": t0.to_rfc3339()}},
            "content": {"city": "Los Angeles", "days": 7}
        },
        {
            "source": {"event": {"name": "tool_handler.result", "time": t1.to_rfc3339()}},
            "content": {
                "role": "tool",
                "tool_use_id": "tooluse_ABC",
                "content": {"json": {"result": "sunny"}}
            }
        }
    ]);

    let rows = vec![make_tool_span_row(
        "trace1",
        "tool1",
        Some("root"),
        &tool_msg.to_string(),
        t0,
        Some(t1),
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Collect all block types
    let block_types: Vec<_> = result
        .messages
        .iter()
        .map(|m| m.entry_type.as_str())
        .collect();

    // Should NOT have json or text blocks for tool input params
    let non_tool_blocks: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.entry_type != "tool_result" && m.role != ChatRole::Tool)
        .collect();

    assert!(
        non_tool_blocks.is_empty(),
        "Tool span should only produce tool_result, not extra blocks. Found: {:?}",
        block_types
    );
}

// ----------------------------------------------------------------------------
// ISSUE 6: Tool Results with Same tool_use_id but Different Content Hash
// ----------------------------------------------------------------------------
// Tool results referencing same tool_use_id should deduplicate even if structure differs.

#[test]
fn test_regression_tool_result_same_id_dedup() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);

    // Tool span result
    let tool_msg = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t1.to_rfc3339()}},
        "content": {
            "role": "tool",
            "tool_use_id": "tooluse_ABC123",
            "content": "The forecast shows sunny weather."
        }
    }]);

    // Chat span receives same result (recorded again as input history)
    let chat_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "tool",
                "tool_use_id": "tooluse_ABC123",
                "content": [{"type": "text", "text": "The forecast shows sunny weather."}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
            "content": {"role": "assistant", "content": "It will be sunny!", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_tool_span_row(
            "trace1",
            "tool1",
            Some("chat1"),
            &tool_msg.to_string(),
            t0,
            Some(t1),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "chat1",
            Some("root"),
            &chat_msg.to_string(),
            t0,
            Some(t2),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Count tool results for this tool_use_id
    let tool_results: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.tool_use_id.as_deref() == Some("tooluse_ABC123") && m.role == ChatRole::Tool)
        .collect();

    assert_eq!(
        tool_results.len(),
        1,
        "Same tool result (same tool_use_id) should appear once. Found {}",
        tool_results.len()
    );
}

// ----------------------------------------------------------------------------
// ISSUE 7: Intermediate Agent Loop Outputs Appearing in Feed
// ----------------------------------------------------------------------------
// Outputs from intermediate agent loop iterations shouldn't appear.

#[test]
fn test_regression_no_intermediate_loop_outputs() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);
    let t3 = t0 + chrono::Duration::seconds(3);

    // First loop iteration (intermediate, not final)
    let loop1_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Get weather for LA"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_1", "name": "weather", "input": {}}],
                "finish_reason": "tool_use"
            }
        }
    ]);

    // Second loop iteration (final)
    let loop2_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t2.to_rfc3339()}},
            "content": {"role": "tool", "tool_use_id": "call_1", "content": "Sunny"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t3.to_rfc3339()}},
            "content": {"role": "assistant", "content": "The weather in LA is sunny!", "finish_reason": "stop"}
        }
    ]);

    // Root span with final output only
    let root_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Get weather for LA"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t3.to_rfc3339()}},
            "content": {"role": "assistant", "content": "The weather in LA is sunny!", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "root", None, &root_msg.to_string(), t0, Some(t3)),
        make_span_row_with_timestamps(
            "trace1",
            "loop1",
            Some("root"),
            &loop1_msg.to_string(),
            t0,
            Some(t1),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "loop2",
            Some("root"),
            &loop2_msg.to_string(),
            t1,
            Some(t3),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Count final assistant text responses (not tool_use)
    let final_responses: Vec<_> = result
        .messages
        .iter()
        .filter(|m| {
            m.role == ChatRole::Assistant && m.entry_type == "text" && m.finish_reason.is_some()
        })
        .collect();

    // Should have exactly 1 final response
    assert_eq!(
        final_responses.len(),
        1,
        "Should have exactly 1 final response. Found {}",
        final_responses.len()
    );
}

// ----------------------------------------------------------------------------
// ISSUE 8: History Assistant Messages Without finish_reason
// ----------------------------------------------------------------------------
// Historical assistant messages lack finish_reason, affecting birth time computation.

#[test]
fn test_regression_history_assistant_detection() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);

    // First span: original conversation
    let first_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi there!", "finish_reason": "stop"}
        }
    ]);

    // Second span: includes history WITHOUT finish_reason
    let second_msg = json!([
        // History - note: NO finish_reason (stripped when re-sent)
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi there!"}
        },
        // New turn
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "How are you?"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
            "content": {"role": "assistant", "content": "I'm doing well!", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_span_row_with_timestamps(
            "trace1",
            "span1",
            None,
            &first_msg.to_string(),
            t0,
            Some(t1),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "span2",
            Some("span1"),
            &second_msg.to_string(),
            t1,
            Some(t2),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Count "Hi there!" assistant messages
    let hi_messages: Vec<_> = result
        .messages
        .iter()
        .filter(|m| {
            m.role == ChatRole::Assistant
                && matches!(&m.content, ContentBlock::Text { text } if text == "Hi there!")
        })
        .collect();

    // Should deduplicate to 1
    assert_eq!(
        hi_messages.len(),
        1,
        "History assistant message should deduplicate. Found {}",
        hi_messages.len()
    );
}

// ----------------------------------------------------------------------------
// ISSUE 9: Cross-Span Duplicate Tool Use Events
// ----------------------------------------------------------------------------
// Same tool use appears in multiple spans with different timing.

#[test]
fn test_regression_cross_span_tool_use_dedup() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);

    // LLM span decides to use tool
    let llm_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [{"type": "tool_use", "id": "call_xyz", "name": "search", "input": {"q": "test"}}],
            "finish_reason": "tool_use"
        }
    }]);

    // Tool span receives the same tool use as input context
    let tool_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_xyz", "name": "search", "input": {"q": "test"}}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t2.to_rfc3339()}},
            "content": {"role": "tool", "tool_use_id": "call_xyz", "content": "Results"}
        }
    ]);

    let rows = vec![
        make_span_row_with_timestamps(
            "trace1",
            "llm",
            Some("root"),
            &llm_msg.to_string(),
            t0,
            Some(t1),
        ),
        make_tool_span_row(
            "trace1",
            "tool",
            Some("llm"),
            &tool_msg.to_string(),
            t1,
            Some(t2),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Count tool_use blocks with this ID
    let tool_uses: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.entry_type == "tool_use" && m.tool_use_id.as_deref() == Some("call_xyz"))
        .collect();

    assert_eq!(
        tool_uses.len(),
        1,
        "Same tool_use across spans should deduplicate. Found {}",
        tool_uses.len()
    );
}

// ----------------------------------------------------------------------------
// ISSUE 10: Full Agent Trace Integration Test
// ----------------------------------------------------------------------------
// Simulates the exact scenario from trace 959d2590050265486b5f3a55ae3e2b71

#[test]
fn test_regression_full_agent_trace() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100);
    let t2 = t0 + chrono::Duration::milliseconds(200);
    let t4 = t0 + chrono::Duration::milliseconds(400);

    // Root agent span
    let root_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.system.message", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "You are a weather assistant."}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Weather in LA?"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t4.to_rfc3339()}},
            "content": {"role": "assistant", "content": "LA is sunny!\n", "finish_reason": "stop"}
        }
    ]);

    // First LLM call - decides to use tool
    let llm1_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Weather in LA?"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_1", "name": "get_weather", "input": {"city": "LA"}}],
                "finish_reason": "tool_use"
            }
        }
    ]);

    // Tool execution
    // Note: In tool spans, gen_ai.choice is used for tool OUTPUT (result)
    // gen_ai.tool.message would be INPUT which is not what we want here
    let tool_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.tool.input", "time": t2.to_rfc3339()}},
            "content": {"city": "LA"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
            "content": {"role": "tool", "tool_use_id": "call_1", "content": "Sunny, 25C"}
        }
    ]);

    // Second LLM call - produces final response
    let llm2_msg = json!([
        // History
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Weather in LA?"}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_1", "name": "get_weather", "input": {"city": "LA"}}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t2.to_rfc3339()}},
            "content": {
                "role": "tool",
                "tool_use_id": "call_1",
                "content": [{"type": "text", "text": "Sunny, 25C"}]
            }
        },
        // New output
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t4.to_rfc3339()}},
            "content": {"role": "assistant", "content": "LA is sunny!", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "root", None, &root_msg.to_string(), t0, Some(t4)),
        make_span_row_with_timestamps(
            "trace1",
            "llm1",
            Some("root"),
            &llm1_msg.to_string(),
            t0,
            Some(t1),
        ),
        make_tool_span_row(
            "trace1",
            "tool",
            Some("llm1"),
            &tool_msg.to_string(),
            t1,
            Some(t2),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "llm2",
            Some("root"),
            &llm2_msg.to_string(),
            t2,
            Some(t4),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Expected conversation flow:
    // 1. System message
    // 2. User: "Weather in LA?"
    // 3. Assistant: [tool_use: get_weather]
    // 4. Tool: "Sunny, 25C"
    // 5. Assistant: "LA is sunny!"

    // Verify no duplicates
    assert_eq!(
        result.messages.len(),
        5,
        "Should have exactly 5 messages. Found {}:\n{:?}",
        result.messages.len(),
        result
            .messages
            .iter()
            .map(|m| format!("{}: {:?}", m.entry_type, m.role))
            .collect::<Vec<_>>()
    );

    // Verify correct ordering - note: system and user have same timestamp,
    // so order between them may vary. Important is semantic ordering:
    // user/system → tool_use → tool_result → assistant_text
    let types: Vec<_> = result
        .messages
        .iter()
        .map(|m| (m.role, m.entry_type.as_str()))
        .collect();

    // First two should be user and system (order may vary as they have same timestamp)
    let first_two: std::collections::HashSet<_> = types[0..2].iter().collect();
    assert!(
        first_two.contains(&(ChatRole::User, "text"))
            && first_two.contains(&(ChatRole::System, "text")),
        "First two should be user and system"
    );
    assert_eq!(
        types[2],
        (ChatRole::Assistant, "tool_use"),
        "Third should be tool_use"
    );
    assert_eq!(
        types[3],
        (ChatRole::Tool, "tool_result"),
        "Fourth should be tool_result"
    );
    assert_eq!(
        types[4],
        (ChatRole::Assistant, "text"),
        "Fifth should be assistant text"
    );

    // No spurious json blocks
    let json_blocks = result
        .messages
        .iter()
        .filter(|m| m.entry_type == "json")
        .count();
    assert_eq!(json_blocks, 0, "Should have no spurious json blocks");
}

// ----------------------------------------------------------------------------
// ISSUE 11: Content Hash Consistency Between Functions
// ----------------------------------------------------------------------------
// compute_block_hash and compute_semantic_hash should produce same results.

#[test]
fn test_regression_hash_function_consistency() {
    use super::dedup::MessageIdentity;

    let t0 = fixed_time();

    // Create a text block
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Hello world"}
    }]);

    let row =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), t0, Some(t0));
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    assert_eq!(result.messages.len(), 1);
    let block = &result.messages[0];

    // Get the content_hash from the block (computed by compute_block_hash)
    let display_hash = u64::from_str_radix(&block.content_hash, 16).unwrap();

    // Get the identity hash (computed by MessageIdentity::from_block -> compute_semantic_hash)
    let identity = MessageIdentity::from_block(block);
    let identity_hash = match identity {
        MessageIdentity::Regular { semantic_hash, .. } => semantic_hash,
        _ => panic!("Expected Regular identity"),
    };

    // These should match for consistent deduplication
    assert_eq!(
        display_hash, identity_hash,
        "content_hash ({:016x}) should match identity semantic_hash ({:016x})",
        display_hash, identity_hash
    );
}

// ============================================================================
// ADDITIONAL REGRESSION TESTS
// ============================================================================

// ----------------------------------------------------------------------------
// ISSUE 11b: JSON Key Order Should Not Affect Deduplication
// ----------------------------------------------------------------------------
// Same JSON content with different key orders should hash to the same value.

#[test]
fn test_regression_json_key_order_deduplication() {
    use super::compute_block_hash;
    use crate::domain::sideml::ContentBlock;

    // Same data, different key order
    let json1 = serde_json::json!({
        "name": "Jane",
        "age": 28,
        "city": "NYC"
    });

    let json2 = serde_json::json!({
        "city": "NYC",
        "name": "Jane",
        "age": 28
    });

    let block1 = ContentBlock::Json { data: json1 };
    let block2 = ContentBlock::Json { data: json2 };

    let hash1 = compute_block_hash(&block1);
    let hash2 = compute_block_hash(&block2);

    assert_eq!(
        hash1, hash2,
        "JSON blocks with same data but different key order should have same hash"
    );
}

#[test]
fn test_regression_nested_json_key_order_deduplication() {
    use super::compute_block_hash;
    use crate::domain::sideml::ContentBlock;

    // Nested JSON with different key orders at multiple levels
    let json1 = serde_json::json!({
        "person": {
            "name": "Jane",
            "address": {"city": "NYC", "street": "123 Main"}
        },
        "score": 95
    });

    let json2 = serde_json::json!({
        "score": 95,
        "person": {
            "address": {"street": "123 Main", "city": "NYC"},
            "name": "Jane"
        }
    });

    let block1 = ContentBlock::Json { data: json1 };
    let block2 = ContentBlock::Json { data: json2 };

    let hash1 = compute_block_hash(&block1);
    let hash2 = compute_block_hash(&block2);

    assert_eq!(
        hash1, hash2,
        "Nested JSON with different key order should have same hash"
    );
}

// ----------------------------------------------------------------------------
// ISSUE 12: Multiple Parallel Tool Calls in Single Response
// ----------------------------------------------------------------------------
// LLM responds with multiple tool_use blocks; all should be preserved.

#[test]
fn test_regression_parallel_tool_calls() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    let msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Compare weather in LA and NYC"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "call_la", "name": "get_weather", "input": {"city": "Los Angeles"}},
                    {"type": "tool_use", "id": "call_nyc", "name": "get_weather", "input": {"city": "New York"}}
                ],
                "finish_reason": "tool_use"
            }
        }
    ]);

    let row =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), t0, Some(t1));
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    // Should have 3 blocks: user + 2 tool_use
    let tool_uses: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.entry_type == "tool_use")
        .collect();
    assert_eq!(
        tool_uses.len(),
        2,
        "Parallel tool calls should both be preserved. Found {}",
        tool_uses.len()
    );

    // Verify different inputs preserved
    let inputs: std::collections::HashSet<_> = tool_uses
        .iter()
        .filter_map(|m| match &m.content {
            ContentBlock::ToolUse { input, .. } => input.get("city").and_then(|v| v.as_str()),
            _ => None,
        })
        .collect();
    assert!(inputs.contains("Los Angeles"));
    assert!(inputs.contains("New York"));
}

// ----------------------------------------------------------------------------
// ISSUE 13: Empty Content Blocks Filtered
// ----------------------------------------------------------------------------
// Messages with empty content arrays should not produce blocks.

#[test]
fn test_regression_empty_content_filtered() {
    let t0 = fixed_time();

    let msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": ""}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": []}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        }
    ]);

    let row =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), t0, Some(t0));
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    // Only the non-empty message should produce a block
    assert_eq!(
        result.messages.len(),
        1,
        "Empty content should be filtered. Found {} messages",
        result.messages.len()
    );
    assert!(matches!(&result.messages[0].content, ContentBlock::Text { text } if text == "Hello"));
}

// ----------------------------------------------------------------------------
// ISSUE 14: Unicode and Special Characters in Content
// ----------------------------------------------------------------------------
// Unicode text should hash consistently and deduplicate properly.

#[test]
fn test_regression_unicode_content_dedup() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Same unicode content in two spans
    let msg1 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Hello 你好 مرحبا 🌍"}
    }]);

    let msg2 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Hello 你好 مرحبا 🌍"}
    }]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "span1", None, &msg1.to_string(), t0, Some(t0)),
        make_span_row_with_timestamps(
            "trace1",
            "span2",
            Some("span1"),
            &msg2.to_string(),
            t1,
            Some(t1),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Should deduplicate to 1
    assert_eq!(
        result.messages.len(),
        1,
        "Unicode content should deduplicate. Found {}",
        result.messages.len()
    );
}

// ----------------------------------------------------------------------------
// ISSUE 15: System Messages in History
// ----------------------------------------------------------------------------
// System prompts duplicated across spans should deduplicate.

#[test]
fn test_regression_system_message_dedup() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    let span1_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.system.message", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "You are a helpful assistant."}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        }
    ]);

    let span2_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.system.message", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "You are a helpful assistant."}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi!", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_span_row_with_timestamps(
            "trace1",
            "span1",
            None,
            &span1_msg.to_string(),
            t0,
            Some(t0),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "span2",
            Some("span1"),
            &span2_msg.to_string(),
            t0,
            Some(t1),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Count system messages
    let system_count = result
        .messages
        .iter()
        .filter(|m| m.role == ChatRole::System)
        .count();
    assert_eq!(
        system_count, 1,
        "System message should deduplicate. Found {}",
        system_count
    );
}

// ----------------------------------------------------------------------------
// ISSUE 16: Cross-Trace Isolation
// ----------------------------------------------------------------------------
// Same content in different traces should NOT deduplicate.

#[test]
fn test_regression_cross_trace_isolation() {
    let t0 = fixed_time();

    let msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Hello"}
    }]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), t0, Some(t0)),
        make_span_row_with_timestamps("trace2", "span2", None, &msg.to_string(), t0, Some(t0)),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Event-based traces (no input attribute): guard prevents false marking
    // because input_source_count (1) <= accumulated.len() (1). Both preserved.
    assert_eq!(
        result.messages.len(),
        2,
        "Event-based traces with same content: both preserved (guard prevents marking). Found {}",
        result.messages.len()
    );
}

// ----------------------------------------------------------------------------
// ISSUE 17: Tool Result with Error Flag
// ----------------------------------------------------------------------------
// Tool results with is_error=true should be preserved and marked.

#[test]
fn test_regression_tool_result_error_preserved() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Tool result with error - using content array with explicit is_error
    let msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Run the command"}
        },
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "tool",
                "tool_use_id": "call_1",
                "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "Error: API rate limit exceeded", "is_error": true}]
            }
        }
    ]);

    let row =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), t0, Some(t1));

    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    // Should have user message and tool result
    assert_eq!(result.messages.len(), 2);

    // Find the tool result block
    let tool_block = result.messages.iter().find(|m| m.role == ChatRole::Tool);
    assert!(tool_block.is_some(), "Should have a tool result");

    let block = tool_block.unwrap();
    assert_eq!(block.entry_type, "tool_result");

    // Verify error info is preserved in the content
    match &block.content {
        ContentBlock::ToolResult { is_error, .. } => {
            assert!(*is_error, "is_error should be true");
        }
        _ => panic!("Expected ToolResult, got {:?}", block.entry_type),
    }
}

// ----------------------------------------------------------------------------
// ISSUE 18: Deep Span Hierarchy
// ----------------------------------------------------------------------------
// Messages in deeply nested spans should maintain correct span_path.

#[test]
fn test_regression_deep_hierarchy_span_path() {
    let t0 = fixed_time();

    // Create 5-level deep hierarchy
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Deep message"}
    }]);

    let rows = vec![
        make_span_row("trace1", "l1", None, "[]", "[]", "[]"),
        make_span_row("trace1", "l2", Some("l1"), "[]", "[]", "[]"),
        make_span_row("trace1", "l3", Some("l2"), "[]", "[]", "[]"),
        make_span_row("trace1", "l4", Some("l3"), "[]", "[]", "[]"),
        make_span_row("trace1", "l5", Some("l4"), &msg.to_string(), "[]", "[]"),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    assert_eq!(result.messages.len(), 1);
    assert_eq!(
        result.messages[0].span_path,
        vec!["l1", "l2", "l3", "l4", "l5"],
        "Deep hierarchy span_path should be correct"
    );
}

// ----------------------------------------------------------------------------
// ISSUE 19: Thinking and Text in Same Message
// ----------------------------------------------------------------------------
// Both thinking and text blocks should be preserved from same message.

#[test]
fn test_regression_thinking_with_text_preserved() {
    let t0 = fixed_time();

    let msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t0.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [
                {"type": "thinking", "text": "Let me reason through this..."},
                {"type": "text", "text": "The answer is 42."}
            ],
            "finish_reason": "stop"
        }
    }]);

    let row =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), t0, Some(t0));
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    // Should have 2 blocks: thinking + text
    assert_eq!(result.messages.len(), 2);

    let types: Vec<_> = result
        .messages
        .iter()
        .map(|m| m.entry_type.as_str())
        .collect();
    assert!(types.contains(&"thinking"), "Should contain thinking block");
    assert!(types.contains(&"text"), "Should contain text block");

    // Both should have same message_index but different entry_index
    assert_eq!(
        result.messages[0].message_index,
        result.messages[1].message_index
    );
    assert_ne!(
        result.messages[0].entry_index,
        result.messages[1].entry_index
    );
}

// ----------------------------------------------------------------------------
// ISSUE 20: Redacted Thinking Block Handling
// ----------------------------------------------------------------------------
// Redacted thinking blocks should be preserved.

#[test]
fn test_regression_redacted_thinking_preserved() {
    let t0 = fixed_time();

    let msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t0.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [
                {"type": "redacted_thinking", "data": "encrypted_data_here"},
                {"type": "text", "text": "Here is my answer."}
            ],
            "finish_reason": "stop"
        }
    }]);

    let row =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), t0, Some(t0));
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    let types: Vec<_> = result
        .messages
        .iter()
        .map(|m| m.entry_type.as_str())
        .collect();
    assert!(
        types.contains(&"redacted_thinking"),
        "Redacted thinking should be preserved. Found: {:?}",
        types
    );
}

// ----------------------------------------------------------------------------
// ISSUE 21: Same Timestamp Different Content
// ----------------------------------------------------------------------------
// Different content at exact same timestamp should both be preserved.

#[test]
fn test_regression_same_timestamp_different_content() {
    let t0 = fixed_time();

    let msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "First question"}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Second question"}
        }
    ]);

    let row =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), t0, Some(t0));
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    // Both should be preserved (different content)
    assert_eq!(
        result.messages.len(),
        2,
        "Different content at same timestamp should both be preserved"
    );
}

// ----------------------------------------------------------------------------
// ISSUE 22: Very Long Content Hashing
// ----------------------------------------------------------------------------
// Very long content should hash consistently without truncation issues.

#[test]
fn test_regression_long_content_hashing() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Create a long message (10KB)
    let long_text: String = "A".repeat(10_000);

    let msg1 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": &long_text}
    }]);

    let msg2 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": &long_text}
    }]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "span1", None, &msg1.to_string(), t0, Some(t0)),
        make_span_row_with_timestamps(
            "trace1",
            "span2",
            Some("span1"),
            &msg2.to_string(),
            t1,
            Some(t1),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Should deduplicate
    assert_eq!(
        result.messages.len(),
        1,
        "Long content should deduplicate correctly"
    );
}

// ----------------------------------------------------------------------------
// ISSUE 23: Tool Use Followed by Immediate Text Response
// ----------------------------------------------------------------------------
// When LLM outputs tool_use and text in same response, both should be preserved.

#[test]
fn test_regression_tool_use_with_text_response() {
    let t0 = fixed_time();

    let msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t0.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "Let me search for that."},
                {"type": "tool_use", "id": "call_1", "name": "search", "input": {"q": "test"}}
            ],
            "finish_reason": "tool_use"
        }
    }]);

    let row =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), t0, Some(t0));
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    // Should have both blocks
    assert_eq!(result.messages.len(), 2);
    let types: Vec<_> = result
        .messages
        .iter()
        .map(|m| m.entry_type.as_str())
        .collect();
    assert!(types.contains(&"text"));
    assert!(types.contains(&"tool_use"));
}

// ----------------------------------------------------------------------------
// ISSUE 24: Multiple Tool Results for Same Tool Use ID
// ----------------------------------------------------------------------------
// If somehow two different results reference same tool_use_id, both should be handled.

#[test]
fn test_regression_duplicate_tool_use_id_different_content() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // First tool result
    let msg1 = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t0.to_rfc3339()}},
        "content": {"role": "tool", "tool_use_id": "call_1", "content": "First result"}
    }]);

    // Second tool result (same ID, different content — anomalous but tool_use_id is identity)
    let msg2 = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t1.to_rfc3339()}},
        "content": {"role": "tool", "tool_use_id": "call_1", "content": "Second result"}
    }]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "span1", None, &msg1.to_string(), t0, Some(t0)),
        make_span_row_with_timestamps(
            "trace1",
            "span2",
            Some("span1"),
            &msg2.to_string(),
            t1,
            Some(t1),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Same tool_use_id = same logical tool execution → deduped to 1.
    // tool_use_id is the primary identity signal for tool results.
    assert_eq!(
        result.messages.len(),
        1,
        "Same tool_use_id should dedup regardless of content differences"
    );
}

// ----------------------------------------------------------------------------
// ISSUE 25: Context Block Handling
// ----------------------------------------------------------------------------
// Context blocks should be preserved as-is.

#[test]
fn test_regression_context_block_preserved() {
    let t0 = fixed_time();

    let msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {
            "role": "user",
            "content": [
                {"type": "context", "context_type": "file", "data": {"path": "/test.txt", "content": "test content"}}
            ]
        }
    }]);

    let row =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), t0, Some(t0));
    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    assert_eq!(result.messages.len(), 1);
    assert_eq!(result.messages[0].entry_type, "context");
}

// ----------------------------------------------------------------------------
// ISSUE 26: Event Source vs Attribute Source Both Handled
// ----------------------------------------------------------------------------
// Both event and attribute sources are valid message sources.
// When duplicates exist, quality scoring prefers event source.

#[test]
fn test_regression_event_and_attribute_sources_handled() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Attribute source - needs timestamp for processing
    let msg1 = json!([{
        "source": {"attribute": {"key": "llm.input_messages", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Hello from attribute"}
    }]);

    // Event source
    let msg2 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}},
        "content": {"role": "user", "content": "Hello from event"}
    }]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "span1", None, &msg1.to_string(), t0, Some(t0)),
        make_span_row_with_timestamps(
            "trace1",
            "span2",
            Some("span1"),
            &msg2.to_string(),
            t1,
            Some(t1),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Should have 2 messages (different content)
    assert_eq!(
        result.messages.len(),
        2,
        "Should have 2 messages (different content). Found: {:?}",
        result
            .messages
            .iter()
            .map(|m| (&m.source_type, &m.span_id))
            .collect::<Vec<_>>()
    );

    // Verify both source types are represented
    let source_types: std::collections::HashSet<_> = result
        .messages
        .iter()
        .map(|m| m.source_type.as_str())
        .collect();
    assert!(
        source_types.contains("attribute"),
        "Should have attribute source"
    );
    assert!(source_types.contains("event"), "Should have event source");
}

// ----------------------------------------------------------------------------
// ISSUE 27: Finish Reason Preservation in Dedup
// ----------------------------------------------------------------------------
// When deduplicating, the version with finish_reason should be kept.

#[test]
fn test_regression_finish_reason_preserved_in_dedup() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Without finish_reason (in later span - will be marked as history duplicate)
    let msg1 = json!([{
        "source": {"event": {"name": "gen_ai.assistant.message", "time": t0.to_rfc3339()}},
        "content": {"role": "assistant", "content": "The answer"}
    }]);

    // With finish_reason (in earlier span - original occurrence)
    let msg2 = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t0.to_rfc3339()}},
        "content": {"role": "assistant", "content": "The answer", "finish_reason": "stop"}
    }]);

    let rows = vec![
        // Span with finish_reason comes first (lower timestamp)
        make_span_row_with_timestamps("trace1", "span1", None, &msg2.to_string(), t0, Some(t0)),
        // Span without finish_reason comes later
        make_span_row_with_timestamps(
            "trace1",
            "span2",
            Some("span1"),
            &msg1.to_string(),
            t1,
            Some(t1),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Should deduplicate to 1 with finish_reason
    assert_eq!(
        result.messages.len(),
        1,
        "Should deduplicate to 1. Found: {}",
        result.messages.len()
    );
    assert!(
        result.messages[0].finish_reason.is_some(),
        "Version with finish_reason should be kept. finish_reason: {:?}",
        result.messages[0].finish_reason
    );
}

// ----------------------------------------------------------------------------
// ISSUE 28: Model Info Preservation in Dedup
// ----------------------------------------------------------------------------
// When deduplicating, the version with model info should be kept.

#[test]
fn test_regression_model_info_preserved_in_dedup() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    let msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Hello"}
    }]);

    // First span with model info
    let mut row1 =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), t0, Some(t0));
    row1.model = Some("claude-3-opus".to_string()); // Has model info

    // Second span without model info (duplicate message)
    let mut row2 = make_span_row_with_timestamps(
        "trace1",
        "span2",
        Some("span1"),
        &msg.to_string(),
        t1,
        Some(t1),
    );
    row2.model = None; // No model info

    let options = FeedOptions::default();
    let result = process_spans(vec![row1, row2], &options);

    // Should deduplicate to 1 with model info
    assert_eq!(
        result.messages.len(),
        1,
        "Should deduplicate to 1. Found: {}",
        result.messages.len()
    );
    assert!(
        result.messages[0].model.is_some(),
        "Version with model info should be kept. Model: {:?}",
        result.messages[0].model
    );
}

// ----------------------------------------------------------------------------
// Additional helper tests for specific edge cases
// ----------------------------------------------------------------------------

#[test]
fn test_tool_result_text_vs_array_normalization() {
    // Tool result content can be string, object, or array
    // All forms representing same data should produce same identity
    let t0 = fixed_time();

    // Form 1: plain string
    let msg1 = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t0.to_rfc3339()}},
        "content": {"role": "tool", "tool_use_id": "call_1", "content": "Result text"}
    }]);

    // Form 2: array with text block
    let msg2 = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t0.to_rfc3339()}},
        "content": {"role": "tool", "tool_use_id": "call_1", "content": [{"type": "text", "text": "Result text"}]}
    }]);

    let row1 =
        make_span_row_with_timestamps("trace1", "span1", None, &msg1.to_string(), t0, Some(t0));
    let row2 = make_span_row_with_timestamps(
        "trace1",
        "span2",
        Some("span1"),
        &msg2.to_string(),
        t0,
        Some(t0),
    );

    let options = FeedOptions::default();
    let result = process_spans(vec![row1, row2], &options);

    // Both forms should deduplicate to 1
    let tool_results: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.role == ChatRole::Tool)
        .collect();

    // This test documents current behavior - it may fail if normalization isn't implemented
    assert_eq!(
        tool_results.len(),
        1,
        "Tool results with same semantic content should deduplicate. Found {}:\n{:?}",
        tool_results.len(),
        tool_results
            .iter()
            .map(|m| format!("span={}, hash={}", m.span_id, m.content_hash))
            .collect::<Vec<_>>()
    );
}

// ============================================================================
// PHASE 3 HISTORY DETECTION REGRESSION TESTS
// ============================================================================
// Tests for GenAI input events from non-generation spans being marked as history.
// This catches cross-trace session history that Strands includes in event loop spans.

/// Helper to create a span row with specific observation_type
fn make_span_row_with_observation_type(
    trace_id: &str,
    span_id: &str,
    parent_span_id: Option<&str>,
    messages_json: &str,
    span_start: chrono::DateTime<Utc>,
    span_end: Option<chrono::DateTime<Utc>>,
    observation_type: &str,
) -> MessageSpanRow {
    let mut row = make_span_row_with_timestamps(
        trace_id,
        span_id,
        parent_span_id,
        messages_json,
        span_start,
        span_end,
    );
    row.observation_type = Some(observation_type.to_string());
    row
}

// ----------------------------------------------------------------------------
// ISSUE 29: Session History in Event Loop Spans
// ----------------------------------------------------------------------------
// Strands includes previous session turns in event loop spans (observation_type="span").
// These should be filtered as history.

#[test]
fn test_regression_session_history_in_event_loop_span() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);

    // Root agent span with current request
    let root_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.system.message", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "You are a weather assistant."}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "What's the weather in London?"}
        }
    ]);

    // Event loop span with session history (previous NYC request)
    // This is what Strands does - accumulates all previous turns
    let event_loop_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "What's the weather in NYC?"}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "NYC is sunny today."}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "What's the weather in London?"}
        }
    ]);

    // Generation span with actual LLM output
    let gen_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
        "content": {"role": "assistant", "content": "London is rainy today.", "finish_reason": "stop"}
    }]);

    let rows = vec![
        make_span_row_with_observation_type(
            "trace1",
            "root",
            None,
            &root_msg.to_string(),
            t0,
            Some(t2),
            "agent",
        ),
        make_span_row_with_observation_type(
            "trace1",
            "event_loop",
            Some("root"),
            &event_loop_msg.to_string(),
            t0,
            Some(t2),
            "span",
        ),
        make_span_row_with_observation_type(
            "trace1",
            "gen",
            Some("event_loop"),
            &gen_msg.to_string(),
            t1,
            Some(t2),
            "generation",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Should NOT contain NYC messages (session history)
    let texts: Vec<_> = result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        !texts.iter().any(|t| t.contains("NYC")),
        "Session history (NYC) should be filtered. Found: {:?}",
        texts
    );

    // Should only have: system, user (London), assistant (London response)
    assert_eq!(
        result.messages.len(),
        3,
        "Should have 3 messages (system, user, assistant). Found {}:\n{:?}",
        result.messages.len(),
        texts
    );
}

// ----------------------------------------------------------------------------
// ISSUE 30: Generation Span Messages Not Filtered
// ----------------------------------------------------------------------------
// Messages from generation spans should NOT be filtered, even if they have
// GenAI input event names.

#[test]
fn test_regression_generation_span_messages_preserved() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Generation span with user input and assistant output
    let gen_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi there!", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_span_row_with_observation_type("trace1", "root", None, "[]", t0, Some(t1), "agent"),
        make_span_row_with_observation_type(
            "trace1",
            "gen",
            Some("root"),
            &gen_msg.to_string(),
            t0,
            Some(t1),
            "generation",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Both messages from generation span should be preserved
    assert_eq!(
        result.messages.len(),
        2,
        "Messages from generation span should be preserved. Found {}",
        result.messages.len()
    );

    let roles: Vec<_> = result.messages.iter().map(|m| m.role).collect();
    assert!(roles.contains(&ChatRole::User));
    assert!(roles.contains(&ChatRole::Assistant));
}

// ----------------------------------------------------------------------------
// ISSUE 31: Root Span Messages Not Filtered
// ----------------------------------------------------------------------------
// Messages from root spans should NOT be filtered, regardless of event name.

#[test]
fn test_regression_root_span_messages_preserved() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Root span with user input (even though it has GenAI input event name)
    let root_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Root span user message"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Response", "finish_reason": "stop"}
        }
    ]);

    // Even with observation_type="span", root should not be filtered
    let rows = vec![make_span_row_with_observation_type(
        "trace1",
        "root",
        None,
        &root_msg.to_string(),
        t0,
        Some(t1),
        "span",
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Root span messages should be preserved
    assert_eq!(
        result.messages.len(),
        2,
        "Root span messages should be preserved. Found {}",
        result.messages.len()
    );
}

// ----------------------------------------------------------------------------
// ISSUE 32: Chain Span History Filtered
// ----------------------------------------------------------------------------
// GenAI input events from chain spans (observation_type="chain") should be filtered.

#[test]
fn test_regression_chain_span_history_filtered() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Root span
    let root_msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Current request"}
    }]);

    // Chain span with accumulated history
    let chain_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Previous request"}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Previous response"}
        }
    ]);

    // Generation span with output
    let gen_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
        "content": {"role": "assistant", "content": "Current response", "finish_reason": "stop"}
    }]);

    let rows = vec![
        make_span_row_with_observation_type(
            "trace1",
            "root",
            None,
            &root_msg.to_string(),
            t0,
            Some(t1),
            "agent",
        ),
        make_span_row_with_observation_type(
            "trace1",
            "chain",
            Some("root"),
            &chain_msg.to_string(),
            t0,
            Some(t1),
            "chain",
        ),
        make_span_row_with_observation_type(
            "trace1",
            "gen",
            Some("chain"),
            &gen_msg.to_string(),
            t0,
            Some(t1),
            "generation",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Should NOT contain "Previous" messages from chain span
    let texts: Vec<_> = result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        !texts.iter().any(|t| t.contains("Previous")),
        "Chain span history should be filtered. Found: {:?}",
        texts
    );

    // Should have: user (Current request), assistant (Current response)
    assert_eq!(result.messages.len(), 2);
}

// ----------------------------------------------------------------------------
// ISSUE 33: Agent Span History Filtered
// ----------------------------------------------------------------------------
// GenAI input events from agent spans (observation_type="agent") in non-root
// position should be filtered.

#[test]
fn test_regression_nested_agent_span_history_filtered() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Root agent span with current request
    let root_msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Main request"}
    }]);

    // Nested agent span (sub-agent) with history
    let sub_agent_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Sub-agent history"}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Sub-agent previous response"}
        }
    ]);

    // Generation span with final output
    let gen_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
        "content": {"role": "assistant", "content": "Final response", "finish_reason": "stop"}
    }]);

    let rows = vec![
        make_span_row_with_observation_type(
            "trace1",
            "root",
            None,
            &root_msg.to_string(),
            t0,
            Some(t1),
            "agent",
        ),
        make_span_row_with_observation_type(
            "trace1",
            "sub_agent",
            Some("root"),
            &sub_agent_msg.to_string(),
            t0,
            Some(t1),
            "agent",
        ),
        make_span_row_with_observation_type(
            "trace1",
            "gen",
            Some("sub_agent"),
            &gen_msg.to_string(),
            t0,
            Some(t1),
            "generation",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Should NOT contain sub-agent history
    let texts: Vec<_> = result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        !texts.iter().any(|t| t.contains("Sub-agent")),
        "Nested agent span history should be filtered. Found: {:?}",
        texts
    );

    // Should have: user (Main request), assistant (Final response)
    assert_eq!(result.messages.len(), 2);
}

// ----------------------------------------------------------------------------
// ISSUE 34: Output Events Not Filtered
// ----------------------------------------------------------------------------
// gen_ai.choice events should NOT be filtered even from non-generation spans.

#[test]
fn test_regression_output_events_preserved() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Span with both input and output events
    let span_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "History input"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Current output", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_span_row_with_observation_type("trace1", "root", None, "[]", t0, Some(t1), "agent"),
        make_span_row_with_observation_type(
            "trace1",
            "span",
            Some("root"),
            &span_msg.to_string(),
            t0,
            Some(t1),
            "span",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // gen_ai.choice output should be preserved (only input filtered)
    let texts: Vec<_> = result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        texts.iter().any(|t| t.contains("Current output")),
        "Output events should be preserved. Found: {:?}",
        texts
    );

    // Input should be filtered
    assert!(
        !texts.iter().any(|t| t.contains("History input")),
        "Input events from span should be filtered. Found: {:?}",
        texts
    );
}

// ----------------------------------------------------------------------------
// ISSUE 35: Multi-Turn Session History (Real-World Strands Pattern)
// ----------------------------------------------------------------------------
// Simulates a Strands trace with multiple previous session turns.

#[test]
fn test_regression_multi_turn_session_history() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100);
    let t2 = t0 + chrono::Duration::milliseconds(200);
    let t_end = t0 + chrono::Duration::seconds(3);

    // Root agent span with current request
    let root_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.system.message", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "You are helpful."}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Third question"}
        }
    ]);

    // Event loop span with ALL previous session turns
    let event_loop_msg = json!([
        // Turn 1
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "First question"}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "First answer"}
        },
        // Turn 2
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "Second question"}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t2.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Second answer"}
        },
        // Current turn (also appears here)
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "Third question"}
        }
    ]);

    // Generation span with current output
    let gen_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t_end.to_rfc3339()}},
        "content": {"role": "assistant", "content": "Third answer", "finish_reason": "stop"}
    }]);

    let rows = vec![
        make_span_row_with_observation_type(
            "trace1",
            "root",
            None,
            &root_msg.to_string(),
            t0,
            Some(t_end),
            "agent",
        ),
        make_span_row_with_observation_type(
            "trace1",
            "event_loop",
            Some("root"),
            &event_loop_msg.to_string(),
            t0,
            Some(t_end),
            "span",
        ),
        make_span_row_with_observation_type(
            "trace1",
            "gen",
            Some("event_loop"),
            &gen_msg.to_string(),
            t2,
            Some(t_end),
            "generation",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Should NOT contain first/second turn history
    let texts: Vec<_> = result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        !texts.iter().any(|t| t.contains("First")),
        "First turn should be filtered. Found: {:?}",
        texts
    );
    assert!(
        !texts.iter().any(|t| t.contains("Second")),
        "Second turn should be filtered. Found: {:?}",
        texts
    );

    // Should have: system, user (Third question), assistant (Third answer)
    assert_eq!(
        result.messages.len(),
        3,
        "Should have 3 messages for current turn. Found {}:\n{:?}",
        result.messages.len(),
        texts
    );
}

/// Regression #36: System message should appear before user message when timestamps are equal.
///
/// Some frameworks (Strands) record system and user messages with the same timestamp.
/// Semantic ordering should ensure System comes first (sets context), then User (provides input).
#[test]
fn test_regression_system_before_user_same_timestamp() {
    let t0 = fixed_time();
    let t_end = t0 + chrono::Duration::seconds(1);

    // Both messages have the exact same timestamp
    // System comes first in the array (message_index 0)
    let messages = json!([
        {
            "source": {"event": {"name": "gen_ai.system.message", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "You are a helpful assistant."}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        }
    ]);

    let rows = vec![make_span_row_with_timestamps(
        "trace1",
        "span1",
        None,
        &messages.to_string(),
        t0,
        Some(t_end),
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    assert_eq!(result.messages.len(), 2, "Should have 2 messages");

    // System should be first (message_index 0), User second (message_index 1)
    assert_eq!(
        result.messages[0].role,
        ChatRole::System,
        "First message should be System, got {:?}",
        result.messages[0].role
    );
    assert_eq!(
        result.messages[1].role,
        ChatRole::User,
        "Second message should be User, got {:?}",
        result.messages[1].role
    );

    // Verify the content
    assert!(
        matches!(&result.messages[0].content, ContentBlock::Text { text } if text.contains("helpful assistant")),
        "System message content mismatch"
    );
    assert!(
        matches!(&result.messages[1].content, ContentBlock::Text { text } if text == "Hello"),
        "User message content mismatch"
    );
}

// ============================================================================
// OUTPUT CLASSIFICATION AND HISTORY PROTECTION TESTS
// ============================================================================

/// Regression #37: gen_ai.choice events are ALWAYS protected from history marking.
///
/// Even if a gen_ai.choice event appears in a non-generation span with a parent,
/// it should NOT be marked as history. This protects actual LLM outputs.
#[test]
fn test_regression_gen_ai_choice_never_history() {
    let t0 = fixed_time();
    let t_end = t0 + chrono::Duration::seconds(1);

    // A gen_ai.choice event in a span with parent - should NOT be marked as history
    let messages = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t_end.to_rfc3339()}},
        "content": {"role": "assistant", "content": "LLM response", "finish_reason": "stop"}
    }]);

    let rows = vec![make_span_row_with_observation_type(
        "trace1",
        "child_span",
        Some("parent"), // Has parent
        &messages.to_string(),
        t0,
        Some(t_end),
        "span", // Non-generation span type that normally triggers Phase 3
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // The gen_ai.choice event should be preserved (protected from history)
    assert_eq!(
        result.messages.len(),
        1,
        "gen_ai.choice should not be filtered"
    );
    assert!(
        matches!(&result.messages[0].content, ContentBlock::Text { text } if text == "LLM response"),
        "gen_ai.choice content should be preserved"
    );
}

/// Regression #38: gen_ai.assistant.message events CAN be marked as history.
///
/// Unlike gen_ai.choice (actual LLM output), gen_ai.assistant.message is used for
/// history re-sends. These SHOULD be marked as history when in non-generation spans.
#[test]
fn test_regression_gen_ai_assistant_message_can_be_history() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100);
    let t_end = t0 + chrono::Duration::seconds(1);

    // Root span with current request (has gen_ai.choice output)
    let root_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Current request"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t_end.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Current response", "finish_reason": "stop"}
        }
    ]);

    // Event loop span with history (gen_ai.assistant.message - NOT gen_ai.choice)
    let event_loop_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Previous response from history"}
        }
    ]);

    let rows = vec![
        make_span_row_with_observation_type(
            "trace1",
            "root",
            None,
            &root_msg.to_string(),
            t0,
            Some(t_end),
            "generation",
        ),
        make_span_row_with_observation_type(
            "trace1",
            "event_loop",
            Some("root"),
            &event_loop_msg.to_string(),
            t0,
            Some(t_end),
            "span", // Non-generation span
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Should have: user (Current request), assistant (Current response)
    // Should NOT have: "Previous response from history"
    let texts: Vec<_> = result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        !texts.iter().any(|t| t.contains("Previous response")),
        "gen_ai.assistant.message should be filtered as history. Found: {:?}",
        texts
    );
    assert!(
        texts.iter().any(|t| t.contains("Current response")),
        "gen_ai.choice should be preserved. Found: {:?}",
        texts
    );
}

/// Regression #39: uses_span_end field is correctly set for different block types.
///
/// Verifies the output classification rules:
/// - gen_ai.choice → uses_span_end = true
/// - Assistant text → uses_span_end = true
/// - ToolUse from non-tool span → uses_span_end = true
/// - User message → uses_span_end = false
/// - Tool result → uses_span_end = false
#[test]
fn test_regression_uses_span_end_classification() {
    let t0 = fixed_time();
    let t_end = t0 + chrono::Duration::seconds(1);

    // Mix of different message types
    let messages = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "User message"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t_end.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Response text"},
                    {"type": "tool_use", "id": "call_1", "name": "search", "input": {"q": "test"}}
                ],
                "finish_reason": "tool_use"
            }
        }
    ]);

    let rows = vec![make_span_row_with_timestamps(
        "trace1",
        "span1",
        None,
        &messages.to_string(),
        t0,
        Some(t_end),
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Find blocks by type and verify uses_span_end
    let user_block = result.messages.iter().find(|b| b.role == ChatRole::User);
    let assistant_text = result
        .messages
        .iter()
        .find(|b| b.role == ChatRole::Assistant && b.entry_type == "text");
    let tool_use = result.messages.iter().find(|b| b.entry_type == "tool_use");

    assert!(user_block.is_some(), "Should have user message");
    assert!(assistant_text.is_some(), "Should have assistant text");
    assert!(tool_use.is_some(), "Should have tool_use");

    // Verify uses_span_end flags
    assert!(
        !user_block.unwrap().uses_span_end,
        "User message should NOT be output"
    );
    assert!(
        assistant_text.unwrap().uses_span_end,
        "Assistant text from gen_ai.choice should be output"
    );
    // NOTE: ToolUse ALWAYS uses event_time (uses_span_end=false), even from gen_ai.choice events.
    // This is critical for correct ordering: ToolUse at event_time T=100 must sort BEFORE
    // ToolResult at span_end T=200. If ToolUse used span_end, it would sort AFTER ToolResult.
    assert!(
        !tool_use.unwrap().uses_span_end,
        "ToolUse should use event_time (not output) for correct ordering"
    );
}

/// Regression #40: ToolUse from tool spans is INPUT, not OUTPUT.
///
/// Tool spans log tool invocation (INPUT). The tool_use is output only if it
/// comes from a gen_ai.choice event (the LLM's completion marker).
#[test]
fn test_regression_tool_use_from_tool_span_is_input() {
    let t0 = fixed_time();
    let t_end = t0 + chrono::Duration::seconds(1);

    // Tool span with tool_use (logging the call)
    let tool_span_msg = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t0.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [{"type": "tool_use", "id": "call_1", "name": "search", "input": {"q": "test"}}]
        }
    }]);

    let rows = vec![make_span_row_with_observation_type(
        "trace1",
        "tool_span",
        Some("parent"),
        &tool_span_msg.to_string(),
        t0,
        Some(t_end),
        "tool", // Tool span
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // The tool_use should be present and marked as INPUT
    let tool_use = result.messages.iter().find(|b| b.entry_type == "tool_use");
    assert!(tool_use.is_some(), "Tool use should be preserved");
    assert!(
        !tool_use.unwrap().uses_span_end,
        "ToolUse from tool span should be INPUT (not OUTPUT)"
    );
}

/// Regression #41: ToolResult from tool spans is OUTPUT, uses span_end for ordering.
///
/// Tool results from tool spans represent the actual tool execution result.
/// They should use span_end for effective timestamp (when tool finished).
#[test]
fn test_regression_tool_result_from_tool_span_uses_span_end() {
    let t0 = fixed_time();
    let t_end = t0 + chrono::Duration::seconds(1);

    // Tool span with tool_result (actual execution)
    let tool_span_msg = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t0.to_rfc3339()}},
        "content": {
            "role": "tool",
            "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "result"}]
        }
    }]);

    let rows = vec![make_span_row_with_observation_type(
        "trace1",
        "tool_span",
        Some("parent"),
        &tool_span_msg.to_string(),
        t0,
        Some(t_end),
        "tool", // Tool span
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // The tool_result should be present and marked as OUTPUT
    let tool_result = result
        .messages
        .iter()
        .find(|b| b.entry_type == "tool_result");
    assert!(tool_result.is_some(), "Tool result should be preserved");
    assert!(
        tool_result.unwrap().uses_span_end,
        "ToolResult from tool span should be OUTPUT"
    );
}

/// Regression #42: Tool ordering - tool_use ALWAYS before tool_result.
///
/// Even when history copies of tool_results have earlier timestamps,
/// the ordering should still be: tool_use → tool_result.
/// This is achieved by:
/// 1. ToolResult from tool spans uses span_end (when tool finished)
/// 2. History copies are filtered and don't affect birth_time
#[test]
fn test_regression_tool_use_before_tool_result_with_history() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100);
    let t2 = t0 + chrono::Duration::milliseconds(200);
    let t3 = t0 + chrono::Duration::milliseconds(300);
    let t4 = t0 + chrono::Duration::milliseconds(400);
    let t5 = t0 + chrono::Duration::milliseconds(500);

    // Generation span with tool_use (LLM decision)
    let gen_span_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [{"type": "tool_use", "id": "call_1", "name": "search", "input": {"q": "test"}}],
            "finish_reason": "tool_use"
        }
    }]);

    // Tool span with tool_result (actual execution)
    let tool_span_msg = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t4.to_rfc3339()}},
        "content": {
            "role": "tool",
            "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "result data"}]
        }
    }]);

    // Event loop span with history copy of tool_result (misleading early timestamp!)
    let t_early = t0 + chrono::Duration::milliseconds(50); // Very early!
    let history_span_msg = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t_early.to_rfc3339()}},
        "content": {
            "role": "tool",
            "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "result data"}]
        }
    }]);

    let rows = vec![
        // Generation span (tool_use)
        make_span_row_with_observation_type(
            "trace1",
            "gen_span",
            Some("root"),
            &gen_span_msg.to_string(),
            t0,
            Some(t2), // span_end = T2
            "generation",
        ),
        // Tool span (actual tool_result)
        make_span_row_with_observation_type(
            "trace1",
            "tool_span",
            Some("gen_span"),
            &tool_span_msg.to_string(),
            t3,
            Some(t5), // span_end = T5
            "tool",
        ),
        // Event loop span (history copy with early timestamp)
        make_span_row_with_observation_type(
            "trace1",
            "event_loop",
            Some("root"),
            &history_span_msg.to_string(),
            t0,
            Some(t0 + chrono::Duration::seconds(10)),
            "span", // Non-generation span
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Find the final tool_use and tool_result blocks
    let tool_use_idx = result
        .messages
        .iter()
        .position(|b| b.entry_type == "tool_use");
    let tool_result_idx = result
        .messages
        .iter()
        .position(|b| b.entry_type == "tool_result");

    assert!(tool_use_idx.is_some(), "Should have tool_use");
    assert!(tool_result_idx.is_some(), "Should have tool_result");

    // CRITICAL: tool_use must come BEFORE tool_result
    assert!(
        tool_use_idx.unwrap() < tool_result_idx.unwrap(),
        "tool_use (index {}) should come before tool_result (index {})",
        tool_use_idx.unwrap(),
        tool_result_idx.unwrap()
    );
}

/// Regression #43: Tool ordering in same span (Strands scenario).
///
/// In Strands and similar frameworks, tool_use and tool_result can appear
/// in the SAME generation span with different event timestamps:
/// - tool_use at T=100 (LLM decided to call tool)
/// - tool_result at T=200 (tool returned)
/// - final_response at T=300 (LLM finished with response)
/// - span_end at T=300
///
/// If tool_use incorrectly used span_end (T=300), it would sort AFTER
/// tool_result (T=200), breaking the conversation flow.
///
/// This test ensures tool_use uses event_time, not span_end.
#[test]
fn test_regression_tool_use_and_tool_result_same_span() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100); // tool_use event
    let t2 = t0 + chrono::Duration::milliseconds(200); // tool_result event
    let t3 = t0 + chrono::Duration::milliseconds(300); // final_response event
    let t_end = t3; // span_end

    // Single generation span with tool_use, tool_result, and final response
    // This mimics Strands behavior where all messages are in one span
    let messages = json!([
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_1", "name": "search", "input": {"q": "test"}}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t2.to_rfc3339()}},
            "content": {
                "role": "tool",
                "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "result data"}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t3.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "text", "text": "Here is your result"}],
                "finish_reason": "stop"
            }
        }
    ]);

    let rows = vec![make_span_row_with_timestamps(
        "trace1",
        "gen_span",
        None,
        &messages.to_string(),
        t0,
        Some(t_end),
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Should have 3 blocks: tool_use, tool_result, final_response
    assert_eq!(result.messages.len(), 3, "Should have 3 blocks");

    // Find indices
    let tool_use_idx = result
        .messages
        .iter()
        .position(|b| b.entry_type == "tool_use");
    let tool_result_idx = result
        .messages
        .iter()
        .position(|b| b.entry_type == "tool_result");
    let text_idx = result
        .messages
        .iter()
        .position(|b| b.entry_type == "text" && b.finish_reason.is_some());

    assert!(tool_use_idx.is_some(), "Should have tool_use");
    assert!(tool_result_idx.is_some(), "Should have tool_result");
    assert!(text_idx.is_some(), "Should have final text");

    // CRITICAL: Correct ordering must be: tool_use < tool_result < final_response
    assert!(
        tool_use_idx.unwrap() < tool_result_idx.unwrap(),
        "tool_use (idx {}) must come before tool_result (idx {})",
        tool_use_idx.unwrap(),
        tool_result_idx.unwrap()
    );
    assert!(
        tool_result_idx.unwrap() < text_idx.unwrap(),
        "tool_result (idx {}) must come before final text (idx {})",
        tool_result_idx.unwrap(),
        text_idx.unwrap()
    );

    // Verify uses_span_end classification
    let tool_use = &result.messages[tool_use_idx.unwrap()];
    let tool_result = &result.messages[tool_result_idx.unwrap()];
    let final_text = &result.messages[text_idx.unwrap()];

    // tool_use from gen_ai.assistant.message (not gen_ai.choice) should NOT be uses_span_end
    // because it doesn't have finish_reason and isn't a completion marker
    assert!(
        !tool_use.uses_span_end,
        "tool_use from gen_ai.assistant.message should NOT be uses_span_end"
    );

    // tool_result from generation span should NOT be uses_span_end
    // (only tool_result from tool spans is uses_span_end)
    assert!(
        !tool_result.uses_span_end,
        "tool_result from generation span should NOT be uses_span_end"
    );

    // final_text from gen_ai.choice with finish_reason should be uses_span_end
    assert!(
        final_text.uses_span_end,
        "final_text from gen_ai.choice should be uses_span_end"
    );
}

/// Regression #44: Parallel tool calls ordering (multiple tools same timestamp).
///
/// When LLM calls multiple tools in parallel, all tool_use blocks have
/// the same event_time. They should maintain their message_index order
/// and all come before their corresponding tool_results.
#[test]
fn test_regression_parallel_tools_same_span_ordering() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100); // both tool_use events
    let t2 = t0 + chrono::Duration::milliseconds(200); // both tool_result events
    let t3 = t0 + chrono::Duration::milliseconds(300); // final response
    let t_end = t3;

    let messages = json!([
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_1", "name": "temperature", "input": {"city": "NYC"}}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_2", "name": "precipitation", "input": {"city": "NYC"}}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t2.to_rfc3339()}},
            "content": {
                "role": "tool",
                "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "72F"}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t2.to_rfc3339()}},
            "content": {
                "role": "tool",
                "content": [{"type": "tool_result", "tool_use_id": "call_2", "content": "20%"}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t3.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "text", "text": "Temperature: 72F, Precipitation: 20%"}],
                "finish_reason": "stop"
            }
        }
    ]);

    let rows = vec![make_span_row_with_timestamps(
        "trace1",
        "gen_span",
        None,
        &messages.to_string(),
        t0,
        Some(t_end),
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Collect indices by type
    let tool_uses: Vec<_> = result
        .messages
        .iter()
        .enumerate()
        .filter(|(_, b)| b.entry_type == "tool_use")
        .collect();
    let tool_results: Vec<_> = result
        .messages
        .iter()
        .enumerate()
        .filter(|(_, b)| b.entry_type == "tool_result")
        .collect();
    let final_text: Vec<_> = result
        .messages
        .iter()
        .enumerate()
        .filter(|(_, b)| b.entry_type == "text" && b.finish_reason.is_some())
        .collect();

    assert_eq!(tool_uses.len(), 2, "Should have 2 tool_use blocks");
    assert_eq!(tool_results.len(), 2, "Should have 2 tool_result blocks");
    assert_eq!(final_text.len(), 1, "Should have 1 final text block");

    // All tool_uses should come before all tool_results
    let max_tool_use_idx = tool_uses.iter().map(|(i, _)| *i).max().unwrap();
    let min_tool_result_idx = tool_results.iter().map(|(i, _)| *i).min().unwrap();
    assert!(
        max_tool_use_idx < min_tool_result_idx,
        "All tool_uses (max idx {}) must come before all tool_results (min idx {})",
        max_tool_use_idx,
        min_tool_result_idx
    );

    // All tool_results should come before final text
    let max_tool_result_idx = tool_results.iter().map(|(i, _)| *i).max().unwrap();
    let final_text_idx = final_text[0].0;
    assert!(
        max_tool_result_idx < final_text_idx,
        "All tool_results (max idx {}) must come before final text (idx {})",
        max_tool_result_idx,
        final_text_idx
    );
}

/// Regression #45: ToolUse without explicit completion marker uses event_time.
///
/// When tool_use comes from gen_ai.assistant.message (not gen_ai.choice),
/// it should use event_time for ordering, not span_end.
/// This is critical for correct tool ordering within a span.
#[test]
fn test_regression_tool_use_from_assistant_message_uses_event_time() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100);
    let t_end = t0 + chrono::Duration::milliseconds(500); // Much later span_end

    // tool_use from gen_ai.assistant.message (not gen_ai.choice)
    let messages = json!([{
        "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [{"type": "tool_use", "id": "call_1", "name": "search", "input": {}}]
        }
    }]);

    let rows = vec![make_span_row_with_timestamps(
        "trace1",
        "gen_span",
        None,
        &messages.to_string(),
        t0,
        Some(t_end),
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    assert_eq!(result.messages.len(), 1);
    let block = &result.messages[0];

    // Should NOT be uses_span_end (no gen_ai.choice event, no finish_reason)
    assert!(
        !block.uses_span_end,
        "tool_use from gen_ai.assistant.message should NOT be uses_span_end"
    );

    // Category should be GenAIAssistantMessage, not GenAIChoice
    assert_eq!(
        block.category,
        crate::data::types::MessageCategory::GenAIAssistantMessage
    );
}

/// Regression #46: Intermediate assistant text from generation spans is filtered.
///
/// In Strands tool-use loops:
/// - Generation span produces intermediate text (gen_ai.assistant.message)
/// - Agent span produces final response (gen_ai.choice)
///
/// The intermediate text should be filtered to show only the final response.
/// This prevents duplicate/intermediate outputs during tool-use cycles.
#[test]
fn test_regression_intermediate_assistant_text_filtered() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100);
    let t2 = t0 + chrono::Duration::milliseconds(200);
    let t3 = t0 + chrono::Duration::milliseconds(300);
    let t_end = t0 + chrono::Duration::seconds(1);

    // Generation span: intermediate assistant text (NOT the final response)
    let gen_span_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "text", "text": "Intermediate output during tool use"}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t2.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_1", "name": "search", "input": {}}]
            }
        }
    ]);

    // Agent span: final response via gen_ai.choice
    let agent_span_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t3.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [{"type": "text", "text": "Final response after tools"}],
            "finish_reason": "stop"
        }
    }]);

    let rows = vec![
        // Agent span (root) with final choice
        make_span_row_full(
            "trace1",
            "agent_span",
            None,
            &agent_span_msg.to_string(),
            t0,
            Some(t_end),
            Some("agent"),
        ),
        // Generation span with intermediate output
        make_span_row_with_observation_type(
            "trace1",
            "gen_span",
            Some("agent_span"),
            &gen_span_msg.to_string(),
            t0,
            Some(t3),
            "generation",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Should have tool_use and final text, but NOT intermediate text
    let text_blocks: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "text" && b.role == ChatRole::Assistant)
        .collect();

    assert_eq!(
        text_blocks.len(),
        1,
        "Should have exactly 1 assistant text (final only)"
    );

    let final_text = text_blocks[0];
    assert_eq!(
        final_text.category,
        crate::data::types::MessageCategory::GenAIChoice,
        "Should be from GenAIChoice (final response)"
    );
    assert!(
        matches!(&final_text.content, ContentBlock::Text { text } if text.contains("Final response")),
        "Should be the final response text"
    );

    // Tool use should still be present
    let tool_uses: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_use")
        .collect();
    assert_eq!(tool_uses.len(), 1, "Should have tool_use");
}

/// Regression #47: Multi-turn session history filtered from generation spans.
///
/// In Strands multi-turn sessions, generation spans contain FULL conversation history
/// including tool calls and results from previous turns. These should be filtered
/// so only the current turn's messages appear.
///
/// Key insight: Tool results in generation spans indicate session history is present.
/// Current turn output uses gen_ai.choice (GenAIChoice category) which is protected.
#[test]
fn test_regression_multi_turn_session_history_in_generation_span() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100);
    let t2 = t0 + chrono::Duration::milliseconds(200);
    let t3 = t0 + chrono::Duration::milliseconds(300);
    let t_end = t0 + chrono::Duration::seconds(1);

    // Generation span contains:
    // 1. Previous turn tool calls (GenAIAssistantMessage) - HISTORY
    // 2. Previous turn tool results (GenAIToolMessage) - HISTORY
    // 3. Current turn tool calls (GenAIChoice) - CURRENT
    let gen_span_msg = json!([
        // Previous turn tool call (history)
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "old_call_1", "name": "search", "input": {"query": "NYC"}}]
            }
        },
        // Previous turn tool result (history) - KEY SIGNAL for session history detection
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "tool",
                "tool_call_id": "old_call_1",
                "content": "NYC weather: sunny"
            }
        },
        // Current turn tool call (gen_ai.choice = protected)
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "new_call_1", "name": "search", "input": {"query": "LA"}}],
                "finish_reason": "tool_use"
            }
        }
    ]);

    // Agent span: current turn user message and final response
    let agent_span_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "LA weather"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t3.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "text", "text": "LA is sunny"}],
                "finish_reason": "stop"
            }
        }
    ]);

    // Event loop span: current turn tool result (execution output)
    let event_loop_msg = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t2.to_rfc3339()}},
        "content": {
            "role": "tool",
            "tool_call_id": "new_call_1",
            "content": "LA weather: sunny"
        }
    }]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "agent_span",
            None,
            &agent_span_msg.to_string(),
            t0,
            Some(t_end),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace1",
            "event_loop",
            Some("agent_span"),
            &event_loop_msg.to_string(),
            t0,
            Some(t3),
            "span",
        ),
        make_span_row_with_observation_type(
            "trace1",
            "gen_span",
            Some("event_loop"),
            &gen_span_msg.to_string(),
            t0,
            Some(t2),
            "generation",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Count tool_use blocks - should only have LA (current turn), not NYC (history)
    let tool_uses: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_use")
        .collect();

    assert_eq!(
        tool_uses.len(),
        1,
        "Should have exactly 1 tool_use (current turn only). Found: {:?}",
        tool_uses.iter().map(|b| &b.content).collect::<Vec<_>>()
    );

    // Verify it's the LA tool call (current turn)
    if let ContentBlock::ToolUse { input, .. } = &tool_uses[0].content {
        assert_eq!(
            input.get("query").and_then(|v| v.as_str()),
            Some("LA"),
            "Should be LA tool call (current turn)"
        );
    } else {
        panic!("Expected tool_use content block");
    }

    // Count tool_result blocks - should only have LA (current turn), not NYC (history)
    let tool_results: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_result")
        .collect();

    assert_eq!(
        tool_results.len(),
        1,
        "Should have exactly 1 tool_result (current turn only)"
    );

    // Should have user message for current turn
    let user_msgs: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.role == ChatRole::User)
        .collect();
    assert_eq!(user_msgs.len(), 1, "Should have 1 user message");

    // Should have final assistant text
    let assistant_text: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.role == ChatRole::Assistant && b.entry_type == "text")
        .collect();
    assert_eq!(
        assistant_text.len(),
        1,
        "Should have 1 final assistant text"
    );
}

// ============================================================================
// REGRESSION TESTS FOR TIMESTAMP-BASED HISTORY DETECTION
// ============================================================================

/// Regression #48: Timestamp-based history detection.
///
/// Messages with timestamp < span_start in child generation spans should be
/// marked as history. This is the fundamental signal for detecting historical
/// context that was passed to the LLM.
#[test]
fn test_regression_timestamp_based_history_detection() {
    let t0 = fixed_time();
    let t_history = t0 - chrono::Duration::seconds(10); // Before span start
    let t_current = t0 + chrono::Duration::seconds(1); // After span start
    let t_end = t0 + chrono::Duration::seconds(2);

    // Generation span with both historical and current content
    let gen_span_msg = json!([
        // Historical message (timestamp before span start) - should be filtered
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t_history.to_rfc3339()}},
            "content": {"role": "user", "content": "Old question from history"}
        },
        // Current message (timestamp after span start) - should be kept
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t_current.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": "Current response",
                "finish_reason": "stop"
            }
        }
    ]);

    let rows = vec![make_span_row_with_observation_type(
        "trace1",
        "gen_span",
        Some("parent"),
        &gen_span_msg.to_string(),
        t0, // Span starts at t0
        Some(t_end),
        "generation",
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Historical message should be filtered (timestamp < span_start)
    let user_msgs: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.role == ChatRole::User)
        .collect();
    assert!(
        user_msgs.is_empty(),
        "Historical user message (timestamp < span_start) should be filtered"
    );

    // Current response should be preserved (protected by gen_ai.choice)
    let assistant_msgs: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.role == ChatRole::Assistant)
        .collect();
    assert_eq!(
        assistant_msgs.len(),
        1,
        "Current response should be preserved"
    );
}

/// Regression #49: Child spans with different content preserved.
///
/// When parent and child spans have genuinely different content,
/// both should be preserved. The history detection should NOT filter
/// new content just because it doesn't exist in parent spans.
#[test]
fn test_regression_child_span_new_content_preserved() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);

    // Parent span with one message
    let parent_msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Question in parent"}
    }]);

    // Child span with different (new) content - NOT a history copy
    let child_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": "Response in child",
            "finish_reason": "stop"
        }
    }]);

    let rows = vec![
        make_span_row_with_timestamps(
            "trace1",
            "parent",
            None,
            &parent_msg.to_string(),
            t0,
            Some(t1),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "child",
            Some("parent"),
            &child_msg.to_string(),
            t1,
            Some(t2),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Both messages should be preserved (different content)
    assert_eq!(
        result.messages.len(),
        2,
        "Both parent and child content should be preserved when different"
    );

    let roles: Vec<_> = result.messages.iter().map(|m| m.role).collect();
    assert!(roles.contains(&ChatRole::User), "User message should exist");
    assert!(
        roles.contains(&ChatRole::Assistant),
        "Assistant message should exist"
    );
}

/// Regression #50: Tool_use preserved when intermediate text is filtered.
///
/// In generation spans, intermediate assistant text should be filtered
/// but tool_use blocks should be preserved. This is because tool_use
/// represents actual LLM output (a decision to call a tool).
#[test]
fn test_regression_tool_use_preserved_text_filtered() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100);
    let t2 = t0 + chrono::Duration::milliseconds(200);
    let t3 = t0 + chrono::Duration::milliseconds(300);
    let t_end = t0 + chrono::Duration::seconds(1);

    // Generation span with intermediate text AND tool_use
    let gen_span_msg = json!([
        // Intermediate text (should be filtered)
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "text", "text": "Let me search for that"}]
            }
        },
        // Tool use (should be preserved - it's actual output)
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t2.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_1", "name": "search", "input": {"query": "test"}}]
            }
        }
    ]);

    // Agent span with final response (triggers has_agent_spans and provides final output)
    let agent_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t3.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": "Here are the results",
            "finish_reason": "stop"
        }
    }]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "agent",
            None,
            &agent_msg.to_string(),
            t0,
            Some(t_end),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace1",
            "gen",
            Some("agent"),
            &gen_span_msg.to_string(),
            t0,
            Some(t_end),
            "generation",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Tool_use should be preserved
    let tool_uses: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_use")
        .collect();
    assert_eq!(
        tool_uses.len(),
        1,
        "Tool_use should be preserved even when intermediate text is filtered"
    );

    // Intermediate text should be filtered, but final response (gen_ai.choice) preserved
    let texts: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "text" && b.role == ChatRole::Assistant)
        .collect();
    assert_eq!(
        texts.len(),
        1,
        "Should have 1 text (final response), intermediate filtered"
    );
    assert!(
        matches!(&texts[0].content, ContentBlock::Text { text } if text.contains("results")),
        "Should be final response, not intermediate text"
    );
}

/// Regression #51: Multi-turn session history with tool operations.
///
/// In multi-turn sessions (Strands-like), generation spans contain full
/// conversation history including tool calls and results from previous turns.
/// These should be filtered, keeping only current turn output.
///
/// This test specifically covers the original bug where trace
/// e91ae2156c0bb2242e34b507c68374e1 (LA weather) was showing NYC tool
/// calls/results from previous turns.
#[test]
fn test_regression_multi_turn_tool_history_filtered() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100);
    let t2 = t0 + chrono::Duration::milliseconds(200);
    let t3 = t0 + chrono::Duration::milliseconds(300);
    let t_end = t0 + chrono::Duration::seconds(1);

    // Generation span contains:
    // 1. Previous turn: user question about NYC
    // 2. Previous turn: tool call for NYC
    // 3. Previous turn: tool result for NYC
    // 4. Previous turn: assistant response about NYC
    // 5. Current turn: user question about LA
    // 6. Current turn: tool call for LA (gen_ai.choice - protected)
    let gen_span_msg = json!([
        // PREVIOUS TURN (all should be filtered)
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "What's the weather in NYC?"}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t0.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "old_call", "name": "weather", "input": {"city": "NYC"}}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t0.to_rfc3339()}},
            "content": {"role": "tool", "tool_call_id": "old_call", "content": "NYC: Rainy"}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "NYC is rainy today."}
        },
        // CURRENT TURN
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "What's the weather in LA?"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t3.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "new_call", "name": "weather", "input": {"city": "LA"}}],
                "finish_reason": "tool_use"
            }
        }
    ]);

    // Agent span with current turn user message
    let agent_msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}},
        "content": {"role": "user", "content": "What's the weather in LA?"}
    }]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "agent",
            None,
            &agent_msg.to_string(),
            t0,
            Some(t_end),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace1",
            "gen",
            Some("agent"),
            &gen_span_msg.to_string(),
            t1, // Span starts AFTER previous turn timestamps (t0)
            Some(t_end),
            "generation",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Check that NYC content is NOT present
    let all_text: Vec<_> = result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        !all_text.iter().any(|t| t.contains("NYC")),
        "NYC messages from previous turn should NOT appear. Found: {:?}",
        all_text
    );

    // Check that LA tool_use IS present
    let tool_uses: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_use")
        .collect();

    assert_eq!(
        tool_uses.len(),
        1,
        "Should have exactly 1 tool_use (LA, current turn)"
    );

    if let ContentBlock::ToolUse { input, .. } = &tool_uses[0].content {
        assert_eq!(
            input.get("city").and_then(|v| v.as_str()),
            Some("LA"),
            "Tool_use should be for LA (current turn), not NYC (history)"
        );
    }

    // Should have only LA user message (from agent span)
    let user_msgs: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.role == ChatRole::User)
        .filter_map(|b| match &b.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(user_msgs.len(), 1, "Should have exactly 1 user message");
    assert!(
        user_msgs[0].contains("LA"),
        "User message should be about LA"
    );
}

/// Regression #52: Cross-trace tool_use_id contamination in sessions.
///
/// When processing a session with multiple traces, the orphan tool_result
/// detection should work per-trace. Tool_use_ids from previous traces should
/// NOT be considered "current" for subsequent traces.
///
/// This test simulates a session where:
/// - Trace 1: Has tool_use with ID "call_old"
/// - Trace 2: Has tool_result with ID "call_old" (session history from trace 1),
///   AND tool_use/result with ID "call_new" (current turn)
///
/// The "call_old" tool_result in trace 2 should be filtered as orphan.
#[test]
fn test_regression_cross_trace_tool_id_contamination() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(5);
    let t2 = t1 + chrono::Duration::seconds(1);
    let t_end1 = t0 + chrono::Duration::seconds(4);
    let t_end2 = t1 + chrono::Duration::seconds(2);

    // TRACE 1: Has tool_use with ID "call_old"
    let trace1_agent_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t0.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [{"type": "tool_use", "id": "call_old", "name": "search", "input": {"q": "NYC"}}],
            "finish_reason": "tool_use"
        }
    }]);

    // TRACE 2: Generation span with session history (call_old) and current turn (call_new)
    let trace2_gen_msg = json!([
        // Session history from trace 1 (should be filtered as orphan)
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t1.to_rfc3339()}},
            "content": {"role": "tool", "tool_call_id": "call_old", "content": "NYC weather"}
        },
        // Current turn (protected by gen_ai.choice)
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_new", "name": "search", "input": {"q": "LA"}}],
                "finish_reason": "tool_use"
            }
        }
    ]);

    let trace2_agent_msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}},
        "content": {"role": "user", "content": "What's the weather in LA?"}
    }]);

    let rows = vec![
        // Trace 1
        make_span_row_full(
            "trace1",
            "agent1",
            None,
            &trace1_agent_msg.to_string(),
            t0,
            Some(t_end1),
            Some("agent"),
        ),
        // Trace 2
        make_span_row_full(
            "trace2",
            "agent2",
            None,
            &trace2_agent_msg.to_string(),
            t1,
            Some(t_end2),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace2",
            "gen2",
            Some("agent2"),
            &trace2_gen_msg.to_string(),
            t1,
            Some(t_end2),
            "generation",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Filter to trace2 messages only
    let trace2_msgs: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.trace_id == "trace2")
        .collect();

    // Should have: user message + current tool_use (call_new)
    // Should NOT have: old tool_result (call_old)
    let tool_results: Vec<_> = trace2_msgs
        .iter()
        .filter(|m| m.entry_type == "tool_result")
        .collect();

    assert!(
        tool_results.is_empty(),
        "Session history tool_result (call_old) should be filtered. Found: {:?}",
        tool_results
            .iter()
            .map(|m| &m.tool_use_id)
            .collect::<Vec<_>>()
    );

    // Current turn tool_use should be present
    let tool_uses: Vec<_> = trace2_msgs
        .iter()
        .filter(|m| m.entry_type == "tool_use")
        .collect();

    assert_eq!(
        tool_uses.len(),
        1,
        "Current turn tool_use (call_new) should be present"
    );

    if let ContentBlock::ToolUse { id, .. } = &tool_uses[0].content {
        assert_eq!(
            id.as_deref(),
            Some("call_new"),
            "Should be current turn tool_use"
        );
    }
}

/// Regression #53: Tool_use before tool_result ordering in multi-trace sessions.
///
/// When processing a session with multiple traces, tool_uses must appear
/// before their corresponding tool_results. This tests that orphan tool_results
/// (from session history) are filtered, maintaining correct ordering.
#[test]
fn test_regression_session_tool_ordering_across_traces() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);
    let t3 = t0 + chrono::Duration::seconds(3);
    let t4 = t0 + chrono::Duration::seconds(4);
    let t5 = t0 + chrono::Duration::seconds(5);

    // TRACE 1: Complete tool cycle
    let trace1_gen_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_1", "name": "weather", "input": {"city": "NYC"}}],
                "finish_reason": "tool_use"
            }
        }
    ]);
    let trace1_tool_msg = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t2.to_rfc3339()}},
        "content": {"role": "tool", "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "NYC: Sunny"}]}
    }]);

    // TRACE 2: Has session history (call_1 result) + new tool cycle (call_2)
    let trace2_gen_msg = json!([
        // Session history - old tool_result (should be filtered)
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": t3.to_rfc3339()}},
            "content": {"role": "tool", "content": [{"type": "tool_result", "tool_use_id": "call_1", "content": "NYC: Sunny"}]}
        },
        // Current turn - new tool_use
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t4.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_2", "name": "weather", "input": {"city": "LA"}}],
                "finish_reason": "tool_use"
            }
        }
    ]);
    let trace2_tool_msg = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t5.to_rfc3339()}},
        "content": {"role": "tool", "content": [{"type": "tool_result", "tool_use_id": "call_2", "content": "LA: Warm"}]}
    }]);

    // Agent spans need content for has_agent_spans detection
    let trace1_agent_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
        "content": {"role": "assistant", "content": "NYC weather ready.", "finish_reason": "stop"}
    }]);
    let trace2_agent_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t5.to_rfc3339()}},
        "content": {"role": "assistant", "content": "LA weather ready.", "finish_reason": "stop"}
    }]);

    let rows = vec![
        // Trace 1
        make_span_row_full(
            "trace1",
            "agent1",
            None,
            &trace1_agent_msg.to_string(),
            t0,
            Some(t2),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace1",
            "gen1",
            Some("agent1"),
            &trace1_gen_msg.to_string(),
            t0,
            Some(t2),
            "generation",
        ),
        make_span_row_full(
            "trace1",
            "tool1",
            Some("agent1"),
            &trace1_tool_msg.to_string(),
            t1,
            Some(t2),
            Some("tool"),
        ),
        // Trace 2
        make_span_row_full(
            "trace2",
            "agent2",
            None,
            &trace2_agent_msg.to_string(),
            t3,
            Some(t5),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace2",
            "gen2",
            Some("agent2"),
            &trace2_gen_msg.to_string(),
            t3,
            Some(t5),
            "generation",
        ),
        make_span_row_full(
            "trace2",
            "tool2",
            Some("agent2"),
            &trace2_tool_msg.to_string(),
            t4,
            Some(t5),
            Some("tool"),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Check trace 2 specifically - should have call_2 tool_use before call_2 tool_result
    let trace2_msgs: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.trace_id == "trace2")
        .collect();

    let tool_uses: Vec<_> = trace2_msgs
        .iter()
        .filter(|m| m.entry_type == "tool_use")
        .collect();
    let tool_results: Vec<_> = trace2_msgs
        .iter()
        .filter(|m| m.entry_type == "tool_result")
        .collect();

    // Should have exactly 1 tool_use (call_2) and 1 tool_result (call_2)
    assert_eq!(
        tool_uses.len(),
        1,
        "Trace 2 should have 1 tool_use (call_2)"
    );
    assert_eq!(
        tool_results.len(),
        1,
        "Trace 2 should have 1 tool_result (call_2, not call_1)"
    );

    // Verify it's call_2, not call_1 (session history)
    assert_eq!(
        tool_results[0].tool_use_id.as_deref(),
        Some("call_2"),
        "Tool result should be call_2 (current turn), not call_1 (session history)"
    );

    // Verify ordering: tool_use before tool_result in the full result
    let trace2_tool_positions: Vec<_> = result
        .messages
        .iter()
        .enumerate()
        .filter(|(_, m)| {
            m.trace_id == "trace2" && (m.entry_type == "tool_use" || m.entry_type == "tool_result")
        })
        .map(|(i, m)| (i, &m.entry_type))
        .collect();

    assert!(
        trace2_tool_positions.len() >= 2,
        "Should have both tool_use and tool_result"
    );

    // Find positions
    let tool_use_pos = trace2_tool_positions
        .iter()
        .find(|(_, t)| *t == "tool_use")
        .map(|(i, _)| i);
    let tool_result_pos = trace2_tool_positions
        .iter()
        .find(|(_, t)| *t == "tool_result")
        .map(|(i, _)| i);

    assert!(
        tool_use_pos < tool_result_pos,
        "Tool_use should come before tool_result. Positions: use={:?}, result={:?}",
        tool_use_pos,
        tool_result_pos
    );
}

/// Regression #54: Session with multiple traces - each trace isolated.
///
/// When processing a session, history detection must work per-trace.
/// Tool operations from trace N should not affect filtering in trace N+1.
#[test]
fn test_regression_session_per_trace_isolation() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(5);
    let t2 = t0 + chrono::Duration::seconds(10);
    let t_end0 = t0 + chrono::Duration::seconds(4);
    let t_end1 = t1 + chrono::Duration::seconds(4);
    let t_end2 = t2 + chrono::Duration::seconds(4);

    // Three traces, each with their own tool cycle
    // The tool_use_ids are different in each trace
    let make_trace_msgs = |call_id: &str, city: &str, time: chrono::DateTime<Utc>| {
        let gen_msg = json!([{
            "source": {"event": {"name": "gen_ai.choice", "time": time.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": call_id, "name": "weather", "input": {"city": city}}],
                "finish_reason": "tool_use"
            }
        }]);
        let tool_msg = json!([{
            "source": {"event": {"name": "gen_ai.tool.message", "time": (time + chrono::Duration::seconds(1)).to_rfc3339()}},
            "content": {"role": "tool", "content": [{"type": "tool_result", "tool_use_id": call_id, "content": format!("{}: Weather data", city)}]}
        }]);
        (gen_msg, tool_msg)
    };

    let (trace1_gen, trace1_tool) = make_trace_msgs("call_a", "NYC", t0);
    let (trace2_gen, trace2_tool) = make_trace_msgs("call_b", "LA", t1);
    let (trace3_gen, trace3_tool) = make_trace_msgs("call_c", "Chicago", t2);

    let rows = vec![
        // Trace 1
        make_span_row_full(
            "trace1",
            "agent1",
            None,
            "[]",
            t0,
            Some(t_end0),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace1",
            "gen1",
            Some("agent1"),
            &trace1_gen.to_string(),
            t0,
            Some(t_end0),
            "generation",
        ),
        make_span_row_full(
            "trace1",
            "tool1",
            Some("agent1"),
            &trace1_tool.to_string(),
            t0,
            Some(t_end0),
            Some("tool"),
        ),
        // Trace 2
        make_span_row_full(
            "trace2",
            "agent2",
            None,
            "[]",
            t1,
            Some(t_end1),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace2",
            "gen2",
            Some("agent2"),
            &trace2_gen.to_string(),
            t1,
            Some(t_end1),
            "generation",
        ),
        make_span_row_full(
            "trace2",
            "tool2",
            Some("agent2"),
            &trace2_tool.to_string(),
            t1,
            Some(t_end1),
            Some("tool"),
        ),
        // Trace 3
        make_span_row_full(
            "trace3",
            "agent3",
            None,
            "[]",
            t2,
            Some(t_end2),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace3",
            "gen3",
            Some("agent3"),
            &trace3_gen.to_string(),
            t2,
            Some(t_end2),
            "generation",
        ),
        make_span_row_full(
            "trace3",
            "tool3",
            Some("agent3"),
            &trace3_tool.to_string(),
            t2,
            Some(t_end2),
            Some("tool"),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Each trace should have its own tool_use and tool_result
    for (trace_id, expected_call_id) in [
        ("trace1", "call_a"),
        ("trace2", "call_b"),
        ("trace3", "call_c"),
    ] {
        let trace_msgs: Vec<_> = result
            .messages
            .iter()
            .filter(|m| m.trace_id == trace_id)
            .collect();

        let tool_uses: Vec<_> = trace_msgs
            .iter()
            .filter(|m| m.entry_type == "tool_use")
            .collect();
        let tool_results: Vec<_> = trace_msgs
            .iter()
            .filter(|m| m.entry_type == "tool_result")
            .collect();

        assert_eq!(
            tool_uses.len(),
            1,
            "{} should have exactly 1 tool_use",
            trace_id
        );
        assert_eq!(
            tool_results.len(),
            1,
            "{} should have exactly 1 tool_result",
            trace_id
        );

        // Verify correct call_id
        if let ContentBlock::ToolUse { id, .. } = &tool_uses[0].content {
            assert_eq!(
                id.as_deref(),
                Some(expected_call_id),
                "{} tool_use should have id {}",
                trace_id,
                expected_call_id
            );
        }
        assert_eq!(
            tool_results[0].tool_use_id.as_deref(),
            Some(expected_call_id),
            "{} tool_result should have id {}",
            trace_id,
            expected_call_id
        );
    }
}

/// Regression #55: Thinking blocks without protection are filtered as history.
///
/// In multi-turn sessions, thinking blocks from previous turns are re-sent as
/// history context. These have GenAIAssistantMessage category (not GenAIChoice)
/// and no finish_reason, so they should be filtered.
///
/// Only thinking blocks with protection markers (GenAIChoice, finish_reason)
/// should be preserved.
#[test]
fn test_regression_thinking_history_filtered() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100);
    let t2 = t0 + chrono::Duration::milliseconds(200);
    let t_end = t0 + chrono::Duration::seconds(1);

    // Generation span with:
    // - History thinking (GenAIAssistantMessage, no finish_reason) - should be FILTERED
    // - Current thinking (GenAIChoice, finish_reason) - should be KEPT
    let gen_span_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "thinking", "thinking": "History thinking from previous turn"}]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "thinking", "thinking": "Current turn thinking"}],
                "finish_reason": "stop"
            }
        }
    ]);

    // Agent span (root) to trigger history detection
    let agent_span_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [{"type": "text", "text": "Final response"}],
            "finish_reason": "stop"
        }
    }]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "agent_span",
            None,
            &agent_span_msg.to_string(),
            t0,
            Some(t_end),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace1",
            "gen_span",
            Some("agent_span"),
            &gen_span_msg.to_string(),
            t0,
            Some(t_end),
            "generation",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Count thinking blocks
    let thinking_blocks: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "thinking")
        .collect();

    assert_eq!(
        thinking_blocks.len(),
        1,
        "Should have exactly 1 thinking block (current turn only)"
    );

    // Verify it's the current turn thinking (protected)
    let thinking = thinking_blocks[0];
    assert!(
        thinking.finish_reason.is_some(),
        "Preserved thinking should have finish_reason (protected)"
    );
    assert!(
        matches!(&thinking.content, ContentBlock::Thinking { text, .. } if text.contains("Current turn")),
        "Should be the current turn thinking"
    );
}

/// Regression #56: User/System messages in non-root generation spans filtered.
///
/// In Strands-like traces with agent spans:
/// - Root agent span has authoritative current-turn messages
/// - Child generation spans receive full context (history + current) for LLM
///
/// User/system messages in child generation spans are history context copies
/// and should be filtered when agent spans exist.
#[test]
fn test_regression_generation_span_history_user_filtered() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::milliseconds(100);
    let t2 = t0 + chrono::Duration::milliseconds(200);
    let t_end = t0 + chrono::Duration::seconds(1);

    // Agent span (root) with current turn messages - use event-based format
    let agent_span_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.system.message", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "You are a helpful assistant"}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Current question: What is 2+2?"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
            "content": {"role": "assistant", "content": "The answer is 4", "finish_reason": "stop"}
        }
    ]);

    // Generation span with history context (copies of previous turns)
    let gen_span_msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "History question from turn 1"}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "History question from turn 2"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "thinking", "thinking": "Current thinking"}],
                "finish_reason": "stop"
            }
        }
    ]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "agent_span",
            None,
            &agent_span_msg.to_string(),
            t0,
            Some(t_end),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace1",
            "gen_span",
            Some("agent_span"),
            &gen_span_msg.to_string(),
            t0,
            Some(t_end),
            "generation",
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Count user messages
    let user_messages: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.role == ChatRole::User)
        .collect();

    assert_eq!(
        user_messages.len(),
        1,
        "Should have exactly 1 user message (from root agent span), got {} user messages, {} total blocks",
        user_messages.len(),
        result.messages.len()
    );

    // Verify it's the current turn question
    assert!(
        matches!(&user_messages[0].content, ContentBlock::Text { text } if text.contains("Current question")),
        "Should be the current turn user message"
    );

    // Verify history user messages were filtered
    let has_history = result.messages.iter().any(
        |b| matches!(&b.content, ContentBlock::Text { text } if text.contains("History question")),
    );
    assert!(
        !has_history,
        "History user messages should be filtered from generation span"
    );
}

/// Regression #57: Multi-turn session with thinking - history filtered per trace.
///
/// Each trace (turn) has an agent span and generation span. History in
/// generation spans should be filtered, keeping only protected content.
#[test]
fn test_regression_multi_turn_thinking_session() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);

    // Turn 1: Simple question - no history
    let turn1_agent = json!([
        {"source": {"event": {"name": "gen_ai.system.message", "time": t0.to_rfc3339()}}, "content": {"role": "system", "content": "System prompt"}},
        {"source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}}, "content": {"role": "user", "content": "Question 1"}},
        {"source": {"event": {"name": "gen_ai.choice", "time": t0.to_rfc3339()}}, "content": {"role": "assistant", "content": "Answer 1", "finish_reason": "stop"}}
    ]);
    let turn1_gen = json!([
        {"source": {"event": {"name": "gen_ai.choice", "time": t0.to_rfc3339()}}, "content": {"role": "assistant", "content": [{"type": "thinking", "thinking": "Thinking for Q1"}], "finish_reason": "stop"}}
    ]);

    // Turn 2: Has history from turn 1 in generation span
    let turn2_agent = json!([
        {"source": {"event": {"name": "gen_ai.system.message", "time": t1.to_rfc3339()}}, "content": {"role": "system", "content": "System prompt"}},
        {"source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}}, "content": {"role": "user", "content": "Question 2"}},
        {"source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}}, "content": {"role": "assistant", "content": "Answer 2", "finish_reason": "stop"}}
    ]);
    let turn2_gen = json!([
        // History thinking (no finish_reason) - should be filtered
        {"source": {"event": {"name": "gen_ai.assistant.message", "time": t1.to_rfc3339()}}, "content": {"role": "assistant", "content": [{"type": "thinking", "thinking": "Thinking for Q1"}]}},
        // History user - should be filtered
        {"source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}}, "content": {"role": "user", "content": "Question 1"}},
        // Current thinking (protected) - should be kept
        {"source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}}, "content": {"role": "assistant", "content": [{"type": "thinking", "thinking": "Thinking for Q2"}], "finish_reason": "stop"}}
    ]);

    // Turn 3: Has history from turns 1 and 2 in generation span
    let turn3_agent = json!([
        {"source": {"event": {"name": "gen_ai.system.message", "time": t2.to_rfc3339()}}, "content": {"role": "system", "content": "System prompt"}},
        {"source": {"event": {"name": "gen_ai.user.message", "time": t2.to_rfc3339()}}, "content": {"role": "user", "content": "Question 3"}},
        {"source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}}, "content": {"role": "assistant", "content": "Answer 3", "finish_reason": "stop"}}
    ]);
    let turn3_gen = json!([
        // History thinking from Q1 and Q2 - should be filtered
        {"source": {"event": {"name": "gen_ai.assistant.message", "time": t2.to_rfc3339()}}, "content": {"role": "assistant", "content": [{"type": "thinking", "thinking": "Thinking for Q1"}]}},
        {"source": {"event": {"name": "gen_ai.assistant.message", "time": t2.to_rfc3339()}}, "content": {"role": "assistant", "content": [{"type": "thinking", "thinking": "Thinking for Q2"}]}},
        // History users - should be filtered
        {"source": {"event": {"name": "gen_ai.user.message", "time": t2.to_rfc3339()}}, "content": {"role": "user", "content": "Question 1"}},
        {"source": {"event": {"name": "gen_ai.user.message", "time": t2.to_rfc3339()}}, "content": {"role": "user", "content": "Question 2"}},
        // Current thinking (protected) - should be kept
        {"source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}}, "content": {"role": "assistant", "content": [{"type": "thinking", "thinking": "Thinking for Q3"}], "finish_reason": "stop"}}
    ]);

    let mut rows = vec![
        // Turn 1
        make_span_row_full(
            "trace1",
            "agent1",
            None,
            &turn1_agent.to_string(),
            t0,
            Some(t0 + chrono::Duration::milliseconds(500)),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace1",
            "gen1",
            Some("agent1"),
            &turn1_gen.to_string(),
            t0,
            Some(t0 + chrono::Duration::milliseconds(500)),
            "generation",
        ),
        // Turn 2
        make_span_row_full(
            "trace2",
            "agent2",
            None,
            &turn2_agent.to_string(),
            t1,
            Some(t1 + chrono::Duration::milliseconds(500)),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace2",
            "gen2",
            Some("agent2"),
            &turn2_gen.to_string(),
            t1,
            Some(t1 + chrono::Duration::milliseconds(500)),
            "generation",
        ),
        // Turn 3
        make_span_row_full(
            "trace3",
            "agent3",
            None,
            &turn3_agent.to_string(),
            t2,
            Some(t2 + chrono::Duration::milliseconds(500)),
            Some("agent"),
        ),
        make_span_row_with_observation_type(
            "trace3",
            "gen3",
            Some("agent3"),
            &turn3_gen.to_string(),
            t2,
            Some(t2 + chrono::Duration::milliseconds(500)),
            "generation",
        ),
    ];

    // Add session_id to group all traces as one conversation
    for row in &mut rows {
        row.session_id = Some("test-session".to_string());
    }

    let options = FeedOptions::default();
    let result = process_feed(rows, &options);

    // Should have 3 thinking blocks (one per turn)
    let thinking_blocks: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "thinking")
        .collect();

    assert_eq!(
        thinking_blocks.len(),
        3,
        "Should have exactly 3 thinking blocks (one per turn), got {}",
        thinking_blocks.len()
    );

    // Should have 3 user messages (one per turn)
    let user_messages: Vec<_> = result
        .messages
        .iter()
        .filter(|b| b.role == ChatRole::User)
        .collect();

    assert_eq!(
        user_messages.len(),
        3,
        "Should have exactly 3 user messages (one per turn), got {}",
        user_messages.len()
    );

    // Total should be 12 blocks: 3 * (system + user + thinking + response)
    assert_eq!(
        result.messages.len(),
        12,
        "Should have 12 total blocks (4 per turn), got {}",
        result.messages.len()
    );
}

/// Regression #58: Protected content never filtered regardless of timestamp.
///
/// Even if a message has timestamp < span_start, if it's protected
/// (gen_ai.choice, finish_reason), it should NOT be filtered.
#[test]
fn test_regression_protected_ignores_timestamp() {
    let t0 = fixed_time();
    let t_before = t0 - chrono::Duration::seconds(10);
    let t_end = t0 + chrono::Duration::seconds(1);

    // Protected message with timestamp before span start
    // Should NOT be filtered because it's protected
    let gen_span_msg = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t_before.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": "Response from LLM",
            "finish_reason": "stop"
        }
    }]);

    let rows = vec![make_span_row_with_observation_type(
        "trace1",
        "gen",
        Some("parent"),
        &gen_span_msg.to_string(),
        t0, // Span starts AFTER message timestamp
        Some(t_end),
        "generation",
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Protected message should be preserved despite timestamp
    assert_eq!(
        result.messages.len(),
        1,
        "Protected message should be preserved even with timestamp < span_start"
    );
    assert!(
        result.messages[0].finish_reason.is_some(),
        "Should have finish_reason (protected)"
    );
}

// ----------------------------------------------------------------------------
// ADK Thinking Block Recognition
// ----------------------------------------------------------------------------
// ADK sends thinking content as {"text": "...", "thought": true} which should
// be normalized to ContentBlock::Thinking, not ContentBlock::Unknown.

#[test]
fn test_regression_adk_thinking_blocks_recognized() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t_end = t0 + chrono::Duration::seconds(5);

    // ADK-style messages with thought blocks using event-based format
    let messages = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {
            "role": "user",
            "content": [{"type": "text", "text": "Solve this logic puzzle"}]
        }
    }, {
        "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [
                {"text": "Let me think step by step...", "thought": true},
                {"type": "text", "text": "The answer is Alice=Green, Bob=Blue, Carol=Red."}
            ],
            "finish_reason": "stop"
        }
    }]);

    let rows = vec![make_span_row_with_observation_type(
        "trace1",
        "gen1",
        None,
        &messages.to_string(),
        t0,
        Some(t_end),
        "generation",
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    assert!(
        result.messages.len() >= 3,
        "Expected user + thinking + assistant text, got {}",
        result.messages.len()
    );

    // Find the thinking block
    let thinking_blocks: Vec<_> = result
        .messages
        .iter()
        .filter(|m| matches!(m.content, ContentBlock::Thinking { .. }))
        .collect();

    assert_eq!(
        thinking_blocks.len(),
        1,
        "Should have exactly one thinking block"
    );

    if let ContentBlock::Thinking { ref text, .. } = thinking_blocks[0].content {
        assert_eq!(text, "Let me think step by step...");
    } else {
        panic!("Expected Thinking content block");
    }

    // Verify no unknown blocks (the thought block should not be unknown)
    let unknown_blocks: Vec<_> = result
        .messages
        .iter()
        .filter(|m| matches!(m.content, ContentBlock::Unknown { .. }))
        .collect();

    assert_eq!(
        unknown_blocks.len(),
        0,
        "ADK thought blocks should not produce Unknown content blocks"
    );
}

#[test]
fn test_regression_tool_use_flow_complete() {
    // Verifies the basic tool_use -> tool_result -> assistant flow
    // across all framework patterns
    let t0 = fixed_time();
    let t_end = t0 + chrono::Duration::seconds(5);

    let messages = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {
            "role": "user",
            "content": [{"type": "text", "text": "What is the weather in NYC?"}]
        }
    }, {
        "source": {"event": {"name": "gen_ai.choice", "time": (t0 + chrono::Duration::seconds(1)).to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [{
                "type": "tool_use",
                "id": "call_123",
                "name": "get_weather",
                "input": {"city": "NYC"}
            }],
            "finish_reason": "tool_use"
        }
    }, {
        "source": {"event": {"name": "gen_ai.tool.message", "time": (t0 + chrono::Duration::seconds(2)).to_rfc3339()}},
        "content": {
            "role": "tool",
            "tool_use_id": "call_123",
            "content": [{"type": "tool_result", "tool_use_id": "call_123", "content": "Sunny, 75F"}]
        }
    }, {
        "source": {"event": {"name": "gen_ai.choice", "time": (t0 + chrono::Duration::seconds(3)).to_rfc3339()}},
        "content": {
            "role": "assistant",
            "content": [{"type": "text", "text": "The weather in NYC is sunny and 75F!"}],
            "finish_reason": "stop"
        }
    }]);

    let rows = vec![make_span_row_with_observation_type(
        "trace1",
        "gen1",
        None,
        &messages.to_string(),
        t0,
        Some(t_end),
        "generation",
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Verify message count and order
    assert!(
        result.messages.len() >= 4,
        "Expected at least 4 messages (user, tool_use, tool_result, assistant), got {}",
        result.messages.len()
    );

    // Verify roles in order
    let roles: Vec<ChatRole> = result.messages.iter().map(|m| m.role).collect();
    assert_eq!(roles[0], ChatRole::User, "First should be user");

    // Find tool_use and tool_result
    let has_tool_use = result
        .messages
        .iter()
        .any(|m| matches!(m.content, ContentBlock::ToolUse { .. }));
    let has_tool_result = result
        .messages
        .iter()
        .any(|m| matches!(m.content, ContentBlock::ToolResult { .. }));

    assert!(has_tool_use, "Should have tool_use block");
    assert!(has_tool_result, "Should have tool_result block");

    // Verify tool_use comes before tool_result
    let tool_use_idx = result
        .messages
        .iter()
        .position(|m| matches!(m.content, ContentBlock::ToolUse { .. }))
        .unwrap();
    let tool_result_idx = result
        .messages
        .iter()
        .position(|m| matches!(m.content, ContentBlock::ToolResult { .. }))
        .unwrap();
    assert!(
        tool_use_idx < tool_result_idx,
        "tool_use should come before tool_result"
    );

    // Verify last message is assistant text
    let last = result.messages.last().unwrap();
    assert_eq!(last.role, ChatRole::Assistant, "Last should be assistant");
    if let ContentBlock::Text { ref text } = last.content {
        assert!(text.contains("sunny"), "Should contain weather response");
    } else {
        panic!("Last message should be text");
    }

    // No duplicates
    let hashes: Vec<_> = result.messages.iter().map(|m| &m.content_hash).collect();
    let unique_hashes: std::collections::HashSet<_> = hashes.iter().collect();
    assert_eq!(
        hashes.len(),
        unique_hashes.len(),
        "Should have no duplicate content hashes"
    );
}

// ============================================================================
// ERROR MESSAGE TESTS
// ============================================================================

#[test]
fn test_error_messages_from_status_message() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Error span with exception_message -> should get error ParsedMessage
    let mut error_row = make_span_row_full(
        "t1",
        "error-span",
        None,
        "[]",
        t0,
        Some(t1),
        Some("generation"),
    );
    error_row.status_code = Some("ERROR".to_string());
    error_row.exception_message = Some("Input is too long".into());

    // Normal span -> should NOT get error ParsedMessage
    let normal_row = make_span_row_full(
        "t1",
        "normal-span",
        None,
        "[]",
        t0,
        Some(t1),
        Some("generation"),
    );

    let rows = vec![error_row, normal_row];
    let options = FeedOptions::new();
    let result = process_spans(rows, &options);

    // Should have exactly one error block
    let error_blocks: Vec<_> = result.messages.iter().filter(|b| b.is_error).collect();
    assert_eq!(error_blocks.len(), 1);
    assert_eq!(
        error_blocks[0].finish_reason,
        Some(super::super::types::FinishReason::Error)
    );
    assert!(
        !error_blocks[0].is_history,
        "Error blocks should not be marked as history"
    );
}

#[test]
fn test_error_messages_leaf_only() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Root error span (has error child -> should NOT get error message)
    let mut root_row =
        make_span_row_full("t1", "root-span", None, "[]", t0, Some(t1), Some("agent"));
    root_row.status_code = Some("ERROR".to_string());
    root_row.exception_message = Some("Root error".to_string());

    // Leaf error span (no error children -> should get error message)
    let mut leaf_row = make_span_row_full(
        "t1",
        "leaf-span",
        Some("root-span"),
        "[]",
        t0,
        Some(t1),
        Some("generation"),
    );
    leaf_row.status_code = Some("ERROR".to_string());
    leaf_row.exception_message = Some("Leaf error".to_string());

    let rows = vec![root_row, leaf_row];
    let options = FeedOptions::new();
    let result = process_spans(rows, &options);

    let error_blocks: Vec<_> = result.messages.iter().filter(|b| b.is_error).collect();
    assert_eq!(
        error_blocks.len(),
        1,
        "Only leaf error should produce a block"
    );
    match &error_blocks[0].content {
        ContentBlock::Text { text } => assert_eq!(text, "Leaf error"),
        _ => panic!("Expected Text content block"),
    }
}

#[test]
fn test_error_messages_no_status_message() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Error span without exception fields -> should NOT get error block
    let mut error_row = make_span_row_full(
        "t1",
        "error-span",
        None,
        "[]",
        t0,
        Some(t1),
        Some("generation"),
    );
    error_row.status_code = Some("ERROR".to_string());
    // exception fields are all None

    let rows = vec![error_row];
    let options = FeedOptions::new();
    let result = process_spans(rows, &options);

    assert!(
        result.messages.is_empty(),
        "No error block when exception fields are empty"
    );
}

// ============================================================================
// COMPOSE ERROR TEXT TESTS
// ============================================================================

#[test]
fn test_compose_error_text_type_and_message() {
    let result = compose_error_text(Some("ValueError"), Some("bad input"), None);
    assert_eq!(result, Some("ValueError: bad input".to_string()));
}

#[test]
fn test_compose_error_text_message_only() {
    let result = compose_error_text(None, Some("bad input"), None);
    assert_eq!(result, Some("bad input".to_string()));
}

#[test]
fn test_compose_error_text_type_only() {
    let result = compose_error_text(Some("ValueError"), None, None);
    assert_eq!(result, Some("ValueError".to_string()));
}

#[test]
fn test_compose_error_text_all_none() {
    let result = compose_error_text(None, None, None);
    assert_eq!(result, None);
}

#[test]
fn test_compose_error_text_all_empty() {
    let result = compose_error_text(Some(""), Some(""), Some(""));
    assert_eq!(result, None);
}

#[test]
fn test_compose_error_text_stacktrace_only() {
    let result = compose_error_text(None, None, Some("at main.py:1"));
    assert_eq!(result, Some("```\nat main.py:1\n```".to_string()));
}

#[test]
fn test_compose_error_text_message_and_stacktrace() {
    let result = compose_error_text(None, Some("bad input"), Some("at main.py:1"));
    assert_eq!(
        result,
        Some("bad input\n\n```\nat main.py:1\n```".to_string())
    );
}

#[test]
fn test_compose_error_text_all_fields() {
    let result = compose_error_text(Some("ValueError"), Some("bad input"), Some("at main.py:1"));
    assert_eq!(
        result,
        Some("ValueError: bad input\n\n```\nat main.py:1\n```".to_string())
    );
}

// ============================================================================
// EXCEPTION FIELD REGRESSION TESTS
// ============================================================================

/// Regression: error span with only status_message (no exception fields)
/// should NOT produce an error block. OTEL SDKs propagate status_message
/// up the span tree — only exception events carry real error details.
#[test]
fn test_error_span_with_only_status_code_no_exception_fields() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    let mut row = make_span_row_full("t1", "s1", None, "[]", t0, Some(t1), Some("agent"));
    row.status_code = Some("ERROR".to_string());
    // No exception fields set — simulates SDK-propagated error status

    let rows = vec![row];
    let result = process_spans(rows, &FeedOptions::new());

    assert!(
        result.messages.is_empty(),
        "No error block for status_code=ERROR without exception fields"
    );
}

/// Regression: exception_type + exception_message produce "Type: Message" header
#[test]
fn test_error_block_with_exception_type_and_message() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    let mut row = make_span_row_full("t1", "s1", None, "[]", t0, Some(t1), Some("generation"));
    row.status_code = Some("ERROR".to_string());
    row.exception_type = Some("ValueError".to_string());
    row.exception_message = Some("bad input".to_string());

    let rows = vec![row];
    let result = process_spans(rows, &FeedOptions::new());

    let error_blocks: Vec<_> = result.messages.iter().filter(|b| b.is_error).collect();
    assert_eq!(error_blocks.len(), 1);
    match &error_blocks[0].content {
        ContentBlock::Text { text } => assert_eq!(text, "ValueError: bad input"),
        _ => panic!("Expected Text content block"),
    }
}

/// Regression: exception with stacktrace renders markdown code block
#[test]
fn test_error_block_with_stacktrace() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    let mut row = make_span_row_full("t1", "s1", None, "[]", t0, Some(t1), Some("generation"));
    row.status_code = Some("ERROR".to_string());
    row.exception_type = Some("RuntimeError".to_string());
    row.exception_message = Some("crash".to_string());
    row.exception_stacktrace = Some("Traceback:\n  File main.py".to_string());

    let rows = vec![row];
    let result = process_spans(rows, &FeedOptions::new());

    let error_blocks: Vec<_> = result.messages.iter().filter(|b| b.is_error).collect();
    assert_eq!(error_blocks.len(), 1);
    match &error_blocks[0].content {
        ContentBlock::Text { text } => {
            assert!(text.starts_with("RuntimeError: crash"));
            assert!(text.contains("```\nTraceback:\n  File main.py\n```"));
        }
        _ => panic!("Expected Text content block"),
    }
}

/// Regression: parent error span with error child should NOT produce error block
/// (leaf detection) even when parent has exception fields.
#[test]
fn test_error_block_only_on_leaf_with_exception_fields() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    let mut parent = make_span_row_full("t1", "parent", None, "[]", t0, Some(t1), Some("agent"));
    parent.status_code = Some("ERROR".to_string());
    parent.exception_type = Some("ValueError".to_string());
    parent.exception_message = Some("parent error".to_string());

    let mut child = make_span_row_full(
        "t1",
        "child",
        Some("parent"),
        "[]",
        t0,
        Some(t1),
        Some("generation"),
    );
    child.status_code = Some("ERROR".to_string());
    child.exception_type = Some("ValueError".to_string());
    child.exception_message = Some("child error".to_string());

    let rows = vec![parent, child];
    let result = process_spans(rows, &FeedOptions::new());

    let error_blocks: Vec<_> = result.messages.iter().filter(|b| b.is_error).collect();
    assert_eq!(
        error_blocks.len(),
        1,
        "Only leaf error should produce block"
    );
    match &error_blocks[0].content {
        ContentBlock::Text { text } => assert!(text.contains("child error")),
        _ => panic!("Expected Text content block"),
    }
}

/// Regression: two independent error leaf spans (broken hierarchy) should
/// both produce error blocks when both have exception fields.
#[test]
fn test_error_blocks_for_independent_leaf_spans() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    // Root agent span with exception event
    let mut root = make_span_row_full("t1", "root", None, "[]", t0, Some(t1), Some("agent"));
    root.status_code = Some("ERROR".to_string());
    root.exception_type = Some("RuntimeError".to_string());
    root.exception_message = Some("root exception".to_string());

    // Detached leaf span (parent not in trace) — only SDK-propagated status
    let mut detached = make_span_row_full(
        "t1",
        "detached",
        Some("missing-parent"),
        "[]",
        t0,
        Some(t1),
        Some("span"),
    );
    detached.status_code = Some("ERROR".to_string());
    // No exception fields — just propagated error status

    let rows = vec![root, detached];
    let result = process_spans(rows, &FeedOptions::new());

    let error_blocks: Vec<_> = result.messages.iter().filter(|b| b.is_error).collect();
    assert_eq!(
        error_blocks.len(),
        1,
        "Only the span with exception fields should produce an error block"
    );
    match &error_blocks[0].content {
        ContentBlock::Text { text } => assert!(text.contains("root exception")),
        _ => panic!("Expected Text content block"),
    }
}

// ============================================================================
// VERCEL AI SDK toModelOutput DEDUPLICATION
// ============================================================================
// Vercel AI SDK's toModelOutput transforms tool result content before re-sending
// to the model in the next generation span. The raw execute() output appears in
// the tool span, while the transformed format appears in the generation span's
// ai.prompt.messages. These have different content hashes but the same tool_use_id.
//
// Trace 74045ce6aae9e6765f8017028a23ecfe demonstrates this pattern:
//   - Tool spans: raw {path, base64, mimeType}
//   - Generation span: {type: "content", value: [{type: "text"}, {type: "image-data"}]}

#[test]
fn test_regression_vercel_to_model_output_tool_result_dedup() {
    // Vercel AI SDK: toModelOutput transforms content before re-sending to the model.
    // The raw tool result in the tool span differs from the transformed version in the
    // next generation span's ai.prompt.messages attribute.
    //
    // Trace 74045ce6aae9e6765f8017028a23ecfe demonstrates this pattern.
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);
    let t3 = t0 + chrono::Duration::seconds(3);

    // Root span (ai.generateText): wraps the whole multi-step flow
    let root_msg = json!([]);

    // First generation span (ai.generateText.doGenerate): produces tool_use
    let gen1_msg = json!([
        {
            "source": {"attribute": {"key": "ai.prompt.messages", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Read the image"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "tooluse_ABC", "name": "image_reader", "input": {"path": "/tmp/image.png"}}],
                "finish_reason": "tool_use"
            }
        }
    ]);

    // Tool span (ai.toolCall): raw execute() result as attribute
    let tool_msg = json!([{
        "source": {"attribute": {"key": "ai.toolCall.result", "time": t2.to_rfc3339()}},
        "content": {
            "role": "tool",
            "tool_use_id": "tooluse_ABC",
            "content": {"path": "/tmp/image.png", "base64": "iVBOR...", "mimeType": "image/png"}
        }
    }]);

    // Second generation span: tool result re-sent (toModelOutput format) + final answer
    let gen2_msg = json!([
        {
            "source": {"attribute": {"key": "ai.prompt.messages", "time": t2.to_rfc3339()}},
            "content": {
                "role": "tool",
                "tool_use_id": "tooluse_ABC",
                "content": {"type": "content", "value": [
                    {"type": "text", "text": "Image: /tmp/image.png"},
                    {"type": "image-data", "data": "iVBOR...", "mediaType": "image/png"}
                ]}
            }
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t3.to_rfc3339()}},
            "content": {"role": "assistant", "content": "The image shows a dog.", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_span_row_with_observation_type(
            "trace1",
            "root",
            None,
            &root_msg.to_string(),
            t0,
            Some(t3),
            "generation",
        ),
        make_span_row_with_timestamps(
            "trace1",
            "gen1",
            Some("root"),
            &gen1_msg.to_string(),
            t0,
            Some(t1),
        ),
        make_tool_span_row(
            "trace1",
            "tool_span",
            Some("root"),
            &tool_msg.to_string(),
            t2,
            Some(t2),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "gen2",
            Some("root"),
            &gen2_msg.to_string(),
            t2,
            Some(t3),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    let tool_results: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.entry_type == "tool_result")
        .collect();

    assert_eq!(
        tool_results.len(),
        1,
        "toModelOutput-transformed tool result should dedup with raw version. Found {}",
        tool_results.len()
    );

    // Should keep the tool span version (actual execution)
    assert_eq!(
        tool_results[0].observation_type.as_deref(),
        Some("tool"),
        "Should prefer tool span version over generation span copy"
    );
}

#[test]
fn test_regression_vercel_to_model_output_multiple_tools_dedup() {
    // 3 parallel tool calls: each produces a raw result in its tool span and
    // a toModelOutput-transformed copy in the next generation span.
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);
    let t3 = t0 + chrono::Duration::seconds(3);

    let root_msg = json!([]);

    // First generation span: 3 parallel tool_use calls
    let gen1_msg = json!([
        {
            "source": {"attribute": {"key": "ai.prompt.messages", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Generate 3 images of a dog"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "call_1", "name": "image_reader", "input": {"path": "/img1.png"}},
                    {"type": "tool_use", "id": "call_2", "name": "image_reader", "input": {"path": "/img2.png"}},
                    {"type": "tool_use", "id": "call_3", "name": "image_reader", "input": {"path": "/img3.png"}}
                ],
                "finish_reason": "tool_use"
            }
        }
    ]);

    // 3 tool spans with raw results (attribute source, matching real Vercel data)
    let tool1_msg = json!([{
        "source": {"attribute": {"key": "ai.toolCall.result", "time": t2.to_rfc3339()}},
        "content": {"role": "tool", "tool_use_id": "call_1", "content": {"path": "/img1.png", "data": "AAA"}}
    }]);
    let tool2_msg = json!([{
        "source": {"attribute": {"key": "ai.toolCall.result", "time": t2.to_rfc3339()}},
        "content": {"role": "tool", "tool_use_id": "call_2", "content": {"path": "/img2.png", "data": "BBB"}}
    }]);
    let tool3_msg = json!([{
        "source": {"attribute": {"key": "ai.toolCall.result", "time": t2.to_rfc3339()}},
        "content": {"role": "tool", "tool_use_id": "call_3", "content": {"path": "/img3.png", "data": "CCC"}}
    }]);

    // Second generation span: all 3 re-sent with toModelOutput format + answer
    let gen2_msg = json!([
        {
            "source": {"attribute": {"key": "ai.prompt.messages", "time": t2.to_rfc3339()}},
            "content": {"role": "tool", "tool_use_id": "call_1",
                "content": {"type": "content", "value": [{"type": "text", "text": "Image: /img1.png"}]}}
        },
        {
            "source": {"attribute": {"key": "ai.prompt.messages", "time": t2.to_rfc3339()}},
            "content": {"role": "tool", "tool_use_id": "call_2",
                "content": {"type": "content", "value": [{"type": "text", "text": "Image: /img2.png"}]}}
        },
        {
            "source": {"attribute": {"key": "ai.prompt.messages", "time": t2.to_rfc3339()}},
            "content": {"role": "tool", "tool_use_id": "call_3",
                "content": {"type": "content", "value": [{"type": "text", "text": "Image: /img3.png"}]}}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t3.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Image 1 is best.", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_span_row_with_observation_type(
            "trace1",
            "root",
            None,
            &root_msg.to_string(),
            t0,
            Some(t3),
            "generation",
        ),
        make_span_row_with_timestamps(
            "trace1",
            "gen1",
            Some("root"),
            &gen1_msg.to_string(),
            t0,
            Some(t1),
        ),
        make_tool_span_row(
            "trace1",
            "t1",
            Some("root"),
            &tool1_msg.to_string(),
            t2,
            Some(t2),
        ),
        make_tool_span_row(
            "trace1",
            "t2",
            Some("root"),
            &tool2_msg.to_string(),
            t2,
            Some(t2),
        ),
        make_tool_span_row(
            "trace1",
            "t3",
            Some("root"),
            &tool3_msg.to_string(),
            t2,
            Some(t2),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "gen2",
            Some("root"),
            &gen2_msg.to_string(),
            t2,
            Some(t3),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    let tool_results: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.entry_type == "tool_result")
        .collect();

    assert_eq!(
        tool_results.len(),
        3,
        "3 tool calls should produce 3 results (not 6). Found {}",
        tool_results.len()
    );

    // All should be from tool spans
    for tr in &tool_results {
        assert_eq!(
            tr.observation_type.as_deref(),
            Some("tool"),
            "All tool results should come from tool spans, not generation span"
        );
    }
}

// ============================================================================
// TIMESTAMP MATERIALIZATION
// ============================================================================
// Attribute-sourced messages inherit span start time as their raw timestamp,
// but their actual production time is span_end (for output blocks).
// After feed processing, block.timestamp must reflect the birth/effective time.
//
// Trace 74045ce6aae9e6765f8017028a23ecfe: final assistant response on root span
// had timestamp = span start (10:03:08) instead of span end (10:03:17).

#[test]
fn test_regression_timestamp_materialized_for_root_span_output() {
    // Vercel AI SDK multi-step: root span wraps gen1 → tools → gen2.
    // The final assistant response appears as an attribute on the root span,
    // inheriting span_start as its timestamp. After processing, its timestamp
    // must be materialized to span_end (when the response was actually produced).
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(3);
    let t2 = t0 + chrono::Duration::seconds(5);
    let t3 = t0 + chrono::Duration::seconds(10);

    // Root span (ai.generateText): final response as attribute
    let root_msg = json!([{
        "source": {"attribute": {"key": "ai.response.text", "time": t0.to_rfc3339()}},
        "content": {"role": "assistant", "content": "The image shows a dog.", "finish_reason": "stop"}
    }]);

    // First gen span: user prompt + tool_use output
    let gen1_msg = json!([
        {
            "source": {"attribute": {"key": "ai.prompt.messages", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Describe the image"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_1", "name": "image_reader", "input": {"path": "/img.png"}}],
                "finish_reason": "tool_use"
            }
        }
    ]);

    // Tool span: raw result
    let tool_msg = json!([{
        "source": {"attribute": {"key": "ai.toolCall.result", "time": t2.to_rfc3339()}},
        "content": {"role": "tool", "tool_use_id": "call_1", "content": {"path": "/img.png", "data": "AAA"}}
    }]);

    // Second gen span: transformed tool result + final answer (gen_ai.choice)
    let gen2_msg = json!([
        {
            "source": {"attribute": {"key": "ai.prompt.messages", "time": t2.to_rfc3339()}},
            "content": {"role": "tool", "tool_use_id": "call_1",
                "content": {"type": "content", "value": [{"type": "text", "text": "Image: /img.png"}]}}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t3.to_rfc3339()}},
            "content": {"role": "assistant", "content": "The image shows a dog.", "finish_reason": "stop"}
        }
    ]);

    let rows = vec![
        make_span_row_with_observation_type(
            "trace1",
            "root",
            None,
            &root_msg.to_string(),
            t0,
            Some(t3),
            "generation",
        ),
        make_span_row_with_timestamps(
            "trace1",
            "gen1",
            Some("root"),
            &gen1_msg.to_string(),
            t0,
            Some(t1),
        ),
        make_tool_span_row(
            "trace1",
            "tool1",
            Some("root"),
            &tool_msg.to_string(),
            t2,
            Some(t2),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "gen2",
            Some("root"),
            &gen2_msg.to_string(),
            t2,
            Some(t3),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Find the final assistant response (finish_reason=stop)
    let final_assistant: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.role == ChatRole::Assistant && m.finish_reason == Some(FinishReason::Stop))
        .collect();

    assert_eq!(
        final_assistant.len(),
        1,
        "Should have exactly one final assistant response"
    );

    // The key assertion: timestamp must NOT be span start (t0).
    // It must be materialized to the effective time (span_end = t3).
    assert_ne!(
        final_assistant[0].timestamp, t0,
        "Final assistant timestamp must not be raw span start time"
    );
    assert_eq!(
        final_assistant[0].timestamp, t3,
        "Final assistant timestamp should be span_end (when response was produced)"
    );

    // Tool results should have span_end timestamps too (tool execution completion)
    let tool_results: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.entry_type == "tool_result")
        .collect();

    assert_eq!(tool_results.len(), 1);
    assert_eq!(
        tool_results[0].timestamp, t2,
        "Tool result timestamp should be tool span_end (execution completion)"
    );

    // Verify chronological ordering: user → tool_use → tool_result → final assistant
    let timestamps: Vec<_> = result.messages.iter().map(|m| m.timestamp).collect();
    for window in timestamps.windows(2) {
        assert!(
            window[0] <= window[1],
            "Messages must be in chronological order: {:?} should be <= {:?}",
            window[0],
            window[1]
        );
    }
}

#[test]
fn test_regression_different_tool_use_ids_preserved() {
    // Different tool_use_ids = different logical tool executions → both preserved.
    // This guards against over-merging: only same tool_use_id triggers dedup.
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(1);

    let msg1 = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t0.to_rfc3339()}},
        "content": {"role": "tool", "tool_use_id": "call_1", "content": "First result"}
    }]);

    let msg2 = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t1.to_rfc3339()}},
        "content": {"role": "tool", "tool_use_id": "call_2", "content": "Second result"}
    }]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "span1", None, &msg1.to_string(), t0, Some(t0)),
        make_span_row_with_timestamps(
            "trace1",
            "span2",
            Some("span1"),
            &msg2.to_string(),
            t1,
            Some(t1),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    let tool_results: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.entry_type == "tool_result")
        .collect();

    assert_eq!(
        tool_results.len(),
        2,
        "Different tool_use_ids should both be preserved. Found {}",
        tool_results.len()
    );
}

// ============================================================================
// PR 1: WITHIN-TRACE ADK SUPPORT TESTS
// ============================================================================

/// ADK multi-span trace: Phase 4b marks assistant from input, output-source survives.
///
/// Agent root + 2 generation children.
/// span1 input (llm_request): [sys, userA, asstB_old, userC]
/// span1 output (gen_ai.choice): [toolD(tool_use)]
/// span2 input (llm_request): [sys, userA, asstB_old, userC, toolD, resultE]
/// span2 output (gen_ai.choice): [asstG(stop)]
///
/// Key assertion: Phase 4b marks asstB_old (input-source, assistant) as history.
/// Protected gen_ai.choice output (toolD, asstG) survives.
#[test]
fn test_adk_multi_span_phase4b_and_dedup() {
    let t0 = fixed_time();
    let dur = chrono::Duration::seconds;

    let agent_root = make_span_row_full(
        "trace1",
        "agent_root",
        None,
        "[]",
        t0,
        Some(t0 + dur(10)),
        Some("agent"),
    );

    let span1_msgs = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:01Z"}},
            "content": {"role": "system", "content": "You are helpful"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:01Z"}},
            "content": {"role": "user", "content": "What is 2+2?"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:01Z"}},
            "content": {"role": "assistant", "content": "Previous answer from history"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:01Z"}},
            "content": {"role": "user", "content": "Now do something"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:05Z"}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_1", "name": "calculator", "input": {"op": "add"}}]
            }
        }
    ]);

    let span1 = make_span_row_full(
        "trace1",
        "gen_span1",
        Some("agent_root"),
        &span1_msgs.to_string(),
        t0 + dur(1),
        Some(t0 + dur(5)),
        Some("generation"),
    );

    let span2_msgs = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:06Z"}},
            "content": {"role": "system", "content": "You are helpful"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:06Z"}},
            "content": {"role": "user", "content": "What is 2+2?"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:06Z"}},
            "content": {"role": "assistant", "content": "Previous answer from history"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:06Z"}},
            "content": {"role": "user", "content": "Now do something"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:06Z"}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_1", "name": "calculator", "input": {"op": "add"}}]
            }
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:06Z"}},
            "content": {"role": "tool", "tool_use_id": "call_1", "content": "4"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:09Z"}},
            "content": {
                "role": "assistant",
                "content": "The answer is 4",
                "finish_reason": "stop"
            }
        }
    ]);

    let span2 = make_span_row_full(
        "trace1",
        "gen_span2",
        Some("agent_root"),
        &span2_msgs.to_string(),
        t0 + dur(6),
        Some(t0 + dur(9)),
        Some("generation"),
    );

    let rows = vec![agent_root, span1, span2];
    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // "Previous answer from history" should be filtered by Phase 4b
    let has_old_assistant = result.messages.iter().any(|m| {
        matches!(&m.content, ContentBlock::Text { text } if text == "Previous answer from history")
    });
    assert!(
        !has_old_assistant,
        "Phase 4b should mark input-source assistant as history"
    );

    // toolD should survive (from span1 output via gen_ai.choice, protected)
    let tool_uses: Vec<_> = result.messages.iter().filter(|m| m.is_tool_use()).collect();
    assert_eq!(
        tool_uses.len(),
        1,
        "toolD should survive from output source"
    );

    // Final answer should be present (protected by gen_ai.choice + finish_reason)
    let final_answer = result
        .messages
        .iter()
        .any(|m| matches!(&m.content, ContentBlock::Text { text } if text == "The answer is 4"));
    assert!(final_answer, "Final assistant answer should be present");

    // Verify no empty result
    assert!(
        result.messages.len() >= 3,
        "At minimum: toolD, asstG, plus user context. Got {} blocks",
        result.messages.len()
    );
}

/// Phase 4b does not affect event-based frameworks (Strands).
/// Events have their own protection mechanism (gen_ai.choice is protected).
#[test]
fn test_phase4b_no_effect_on_strands_events() {
    let t0 = fixed_time();
    let dur = chrono::Duration::seconds;

    let agent_root = make_span_row_full(
        "trace1",
        "agent_root",
        None,
        "[]",
        t0,
        Some(t0 + dur(10)),
        Some("agent"),
    );

    // Strands pattern: events on generation span
    let gen_msgs = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:01Z"}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:03Z"}},
            "content": {
                "role": "assistant",
                "content": "Hi there!",
                "finish_reason": "stop"
            }
        }
    ]);

    let gen_span = make_span_row_full(
        "trace1",
        "gen_span",
        Some("agent_root"),
        &gen_msgs.to_string(),
        t0 + dur(1),
        Some(t0 + dur(3)),
        Some("generation"),
    );

    let rows = vec![agent_root, gen_span];
    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // gen_ai.choice assistant text is protected — Phase 4b should NOT touch it
    let assistant_blocks: Vec<_> = result
        .messages
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(
        assistant_blocks.len(),
        1,
        "Strands gen_ai.choice should survive (protected)"
    );
}

/// Output-source assistant from llm_response survives while input-source
/// assistant from llm_request is marked as history by Phase 4b.
#[test]
fn test_output_source_assistant_survives_input_source_marked() {
    let t0 = fixed_time();
    let dur = chrono::Duration::seconds;

    let agent_root = make_span_row_full(
        "trace1",
        "agent_root",
        None,
        "[]",
        t0,
        Some(t0 + dur(10)),
        Some("agent"),
    );

    // Non-root generation span with both input and output assistant text
    let gen_msgs = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:01Z"}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:01Z"}},
            "content": {"role": "assistant", "content": "Old response from history"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": "2025-01-01T00:00:05Z"}},
            "content": {
                "role": "assistant",
                "content": "New response from this turn",
                "finish_reason": "stop"
            }
        }
    ]);

    let gen_span = make_span_row_full(
        "trace1",
        "gen_span",
        Some("agent_root"),
        &gen_msgs.to_string(),
        t0 + dur(1),
        Some(t0 + dur(5)),
        Some("generation"),
    );

    let rows = vec![agent_root, gen_span];
    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // "Old response" should be gone (Phase 4b: input-source, assistant, non-root gen)
    let old = result.messages.iter().any(|m| {
        matches!(&m.content, ContentBlock::Text { text } if text == "Old response from history")
    });
    assert!(
        !old,
        "Input-source assistant should be marked as history and filtered"
    );

    // "New response" should survive (output-source, has finish_reason → protected)
    let new = result.messages.iter().any(|m| {
        matches!(&m.content, ContentBlock::Text { text } if text == "New response from this turn")
    });
    assert!(
        new,
        "Output-source assistant with finish_reason should survive"
    );
}

/// Phase 4b marks tool_use from input source as history, output-source tool_use wins dedup.
#[test]
fn test_tool_use_input_vs_output_source_quality() {
    let t0 = fixed_time();
    let dur = chrono::Duration::seconds;

    let agent_root = make_span_row_full(
        "trace1",
        "agent_root",
        None,
        "[]",
        t0,
        Some(t0 + dur(20)),
        Some("agent"),
    );

    // span1 output: gen_ai.choice with tool_use
    let span1_msgs = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:01Z"}},
            "content": {"role": "user", "content": "Do something"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:03Z"}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_abc", "name": "my_tool", "input": {"x": 1}}]
            }
        }
    ]);

    let span1 = make_span_row_full(
        "trace1",
        "gen_span1",
        Some("agent_root"),
        &span1_msgs.to_string(),
        t0 + dur(1),
        Some(t0 + dur(3)),
        Some("generation"),
    );

    // span2 input: llm_request re-sends the tool_use as history
    let span2_msgs = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:06Z"}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_abc", "name": "my_tool", "input": {"x": 1}}]
            }
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:06Z"}},
            "content": {"role": "tool", "tool_use_id": "call_abc", "content": "result_val"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:09Z"}},
            "content": {
                "role": "assistant",
                "content": "Done!",
                "finish_reason": "stop"
            }
        }
    ]);

    let span2 = make_span_row_full(
        "trace1",
        "gen_span2",
        Some("agent_root"),
        &span2_msgs.to_string(),
        t0 + dur(6),
        Some(t0 + dur(9)),
        Some("generation"),
    );

    let rows = vec![agent_root, span1, span2];
    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // tool_use should survive (from span1's gen_ai.choice, protected)
    let tool_uses: Vec<_> = result.messages.iter().filter(|m| m.is_tool_use()).collect();
    assert_eq!(tool_uses.len(), 1, "Exactly one tool_use should survive");

    // The surviving tool_use should be output-sourced (from gen_ai.choice event)
    assert!(
        tool_uses[0].is_output_event() || tool_uses[0].is_protected(),
        "Surviving tool_use should be from output/protected source"
    );
}

/// is_input_source and is_output_source classification propagates through pipeline.
/// Verify that blocks from ADK llm_request get is_input_source() = true.
#[test]
fn test_source_classification_propagates_to_blocks() {
    let t0 = fixed_time();
    let dur = chrono::Duration::seconds;

    let msgs = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": "2025-01-01T00:00:00Z"}},
            "content": {"role": "user", "content": "input msg"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": "2025-01-01T00:00:05Z"}},
            "content": {
                "role": "assistant",
                "content": "output msg",
                "finish_reason": "stop"
            }
        }
    ]);

    let row = make_span_row_full(
        "trace1",
        "span1",
        None, // root
        &msgs.to_string(),
        t0,
        Some(t0 + dur(5)),
        Some("generation"),
    );

    let options = FeedOptions::default();
    let result = process_spans(vec![row], &options);

    assert_eq!(result.messages.len(), 2);

    let user_block = &result.messages[0];
    assert_eq!(user_block.role, ChatRole::User);
    assert!(
        user_block.is_input_source(),
        "User block from llm_request should be input-source. source_attribute={:?}",
        user_block.source_attribute
    );

    let asst_block = &result.messages[1];
    assert_eq!(asst_block.role, ChatRole::Assistant);
    assert!(
        asst_block.is_output_source(),
        "Assistant block from llm_response should be output-source. source_attribute={:?}",
        asst_block.source_attribute
    );
}

// ============================================================================
// CROSS-TRACE SESSION DEDUP (PREFIX STRIP ENGINE)
// ============================================================================

// ----------------------------------------------------------------------------
// Test: 3 ADK traces with growing prefix → only new content from each
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_accumulated_history() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);
    let t2 = t0 + chrono::Duration::seconds(20);

    // Trace 1: user("NYC weather") → assistant("NYC sunny")
    let trace1_msg = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "NYC weather"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "NYC sunny"}
        }
    ]);

    // Trace 2: user("NYC weather") + assistant("NYC sunny") [history] + user("London") → assistant("London rain")
    let trace2_msg = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "NYC weather"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "NYC sunny"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "London weather"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "London rain"}
        }
    ]);

    // Trace 3: all history + user("Tokyo") → assistant("Tokyo cloudy")
    let trace3_msg = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "NYC weather"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "assistant", "content": "NYC sunny"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "London weather"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "assistant", "content": "London rain"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "Tokyo weather"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t2.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Tokyo cloudy"}
        }
    ]);

    let mut row1 = make_span_row_full(
        "trace1",
        "s1",
        None,
        &trace1_msg.to_string(),
        t0,
        Some(t0),
        Some("generation"),
    );
    let mut row2 = make_span_row_full(
        "trace2",
        "s2",
        None,
        &trace2_msg.to_string(),
        t1,
        Some(t1),
        Some("generation"),
    );
    let mut row3 = make_span_row_full(
        "trace3",
        "s3",
        None,
        &trace3_msg.to_string(),
        t2,
        Some(t2),
        Some("generation"),
    );
    row1.session_id = Some("session1".to_string());
    row2.session_id = Some("session1".to_string());
    row3.session_id = Some("session1".to_string());

    let options = FeedOptions::default();
    let result = process_spans(vec![row1, row2, row3], &options);

    // Within-trace: Phase 4b filters assistant blocks from llm_request (input-source).
    // Cross-trace prefix strip: removes user/tool blocks already seen in prior traces.
    // Trace 1: user("NYC") + asst("NYC sunny") from llm_response = 2
    // Trace 2: prefix [user("NYC")] stripped, asst("NYC sunny") filtered by 4b
    //   → user("London") + asst("London rain") = 2
    // Trace 3: prefix [user("NYC"), user("London")] stripped, assts filtered by 4b
    //   → user("Tokyo") + asst("Tokyo cloudy") = 2
    // Total: 6
    let texts: Vec<&str> = result
        .messages
        .iter()
        .filter_map(|b| match &b.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        result.messages.len(),
        6,
        "Expected 6 blocks (2 per trace after prefix strip + 4b). Got {} blocks: {:?}",
        result.messages.len(),
        texts
    );
}

// ----------------------------------------------------------------------------
// Test: Single trace is unchanged
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_single_trace_unchanged() {
    let t0 = fixed_time();

    let msg = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi there"}
        }
    ]);

    let row =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), t0, Some(t0));

    let options = FeedOptions::default();
    let result_via_process = process_spans(vec![row.clone()], &options);
    let result_via_trace = process_trace_spans(vec![row], &options);

    assert_eq!(
        result_via_process.messages.len(),
        result_via_trace.messages.len(),
        "Single trace: process_spans should match process_trace_spans"
    );
}

// ----------------------------------------------------------------------------
// Test: Two traces with no overlapping content → 0 stripped
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_no_overlap() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    let msg1 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Hello"}
    }]);
    let msg2 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}},
        "content": {"role": "user", "content": "Goodbye"}
    }]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "s1", None, &msg1.to_string(), t0, Some(t0)),
        make_span_row_with_timestamps("trace2", "s2", None, &msg2.to_string(), t1, Some(t1)),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    assert_eq!(
        result.messages.len(),
        2,
        "No overlap: both blocks should be preserved"
    );
}

// ----------------------------------------------------------------------------
// Test: Strands JS bundled gen_ai.input.messages — cross-trace history stripped
// ----------------------------------------------------------------------------
//
// Strands JS uses @opentelemetry/instrumentation-aws-sdk, which bundles all
// messages into a single gen_ai.input.messages event (shared timestamp). This
// means timestamp-based Phase 2 can't detect cross-trace history. The cross-
// trace prefix mechanism must handle it instead.

#[test]
fn test_cross_trace_strands_js_bundled_messages_deduped() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(60);

    // Trace 1: user1 → assistant1 (single turn)
    // Note: content values must be non-numeric strings to avoid normalize_content treating
    // them as JSON numbers (which produces empty content and drops the block).
    let trace1_msgs = json!([
        {
            // gen_ai.input.messages: just the user message (no history yet)
            "source": {"event": {"name": "gen_ai.input.messages", "time": t0.to_rfc3339()}},
            "content": [
                {"role": "user", "content": "What is 2+2?"}
            ]
        },
        {
            // gen_ai.choice: assistant response
            "source": {"event": {"name": "gen_ai.choice", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Two plus two is four"}
        }
    ]);

    // Trace 2: user1 + assistant1 replayed + new user2 → assistant2
    let trace2_msgs = json!([
        {
            // gen_ai.input.messages: includes full history from trace 1 + new user message
            // All share the SAME event timestamp (t1), so Phase 2 timestamp check won't help
            "source": {"event": {"name": "gen_ai.input.messages", "time": t1.to_rfc3339()}},
            "content": [
                {"role": "user", "content": "What is 2+2?"},                  // replayed from trace 1
                {"role": "assistant", "content": "Two plus two is four"},      // replayed from trace 1
                {"role": "user", "content": "And 3+3?"}                       // new user message
            ]
        },
        {
            // gen_ai.choice: new assistant response
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Three plus three is six"}
        }
    ]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "s1",
            None,
            &trace1_msgs.to_string(),
            t0,
            Some(t0),
            Some("generation"),
        ),
        make_span_row_full(
            "trace2",
            "s2",
            None,
            &trace2_msgs.to_string(),
            t1,
            Some(t1),
            Some("generation"),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Expected: user1, assistant1, user2, assistant2 — no duplicates
    assert_eq!(
        result.messages.len(),
        4,
        "Should have 4 unique messages (cross-trace history stripped). Got: {}",
        result.messages.len()
    );

    let texts: Vec<_> = result
        .messages
        .iter()
        .filter_map(|b| {
            if let crate::domain::sideml::types::ContentBlock::Text { text } = &b.content {
                Some(text.as_str())
            } else {
                None
            }
        })
        .collect();
    assert!(texts.contains(&"What is 2+2?"), "user1 should appear once");
    assert!(
        texts.contains(&"Two plus two is four"),
        "assistant1 should appear once"
    );
    assert!(texts.contains(&"And 3+3?"), "user2 should appear");
    assert!(
        texts.contains(&"Three plus three is six"),
        "assistant2 should appear"
    );
}

// ----------------------------------------------------------------------------
// Test: Event-based (Strands Python) per-message events stay trace-independent
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_strands_independent() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    // Strands: unique event per trace, different content
    let msg1 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Question 1"}
    }, {
        "source": {"event": {"name": "gen_ai.choice", "time": t0.to_rfc3339()}},
        "content": {"role": "assistant", "content": "Answer 1"}
    }]);
    let msg2 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}},
        "content": {"role": "user", "content": "Question 2"}
    }, {
        "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
        "content": {"role": "assistant", "content": "Answer 2"}
    }]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "s1", None, &msg1.to_string(), t0, Some(t0)),
        make_span_row_with_timestamps("trace2", "s2", None, &msg2.to_string(), t1, Some(t1)),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    assert_eq!(
        result.messages.len(),
        4,
        "Strands: unique events per trace, all preserved. Got {}",
        result.messages.len()
    );
}

// ----------------------------------------------------------------------------
// Test: Pure replay trace → 0 new blocks
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_replay_fully_deduped() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    let msg = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "What is 2+2?"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "4"}
        }
    ]);

    // Trace2 replays identical content (re-execution)
    let msg2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "What is 2+2?"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "4"}
        }
    ]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "s1",
            None,
            &msg.to_string(),
            t0,
            Some(t0),
            Some("generation"),
        ),
        make_span_row_full(
            "trace2",
            "s2",
            None,
            &msg2.to_string(),
            t1,
            Some(t1),
            Some("generation"),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Pure replay should contribute 0 new blocks from trace2.
    // Attribute-based replay is treated as history re-send and stripped.
    let trace1_count = result
        .messages
        .iter()
        .filter(|b| b.trace_id == "trace1")
        .count();
    let trace2_count = result
        .messages
        .iter()
        .filter(|b| b.trace_id == "trace2")
        .count();
    assert!(
        trace1_count > 0,
        "Trace1 should contribute blocks. Got {}",
        trace1_count
    );
    assert!(
        trace2_count == 0,
        "Trace2 (pure replay) should contribute 0 blocks. Got {}",
        trace2_count
    );
}

// ----------------------------------------------------------------------------
// Test: System messages preserved per contributing trace
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_system_per_trace() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    let msg1 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "You are helpful"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi"}
        }
    ]);

    // Trace2: same system + history prefix + new content
    let msg2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "system", "content": "You are helpful"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "Thanks"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Welcome"}
        }
    ]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "s1",
            None,
            &msg1.to_string(),
            t0,
            Some(t0),
            Some("generation"),
        ),
        make_span_row_full(
            "trace2",
            "s2",
            None,
            &msg2.to_string(),
            t1,
            Some(t1),
            Some("generation"),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Trace1: system + user + asst = 3
    // Trace2: system (preserved) + user("Thanks") + asst("Welcome") = 3
    // Phase 4b marks assistant from input-source as history within trace2,
    // so "Hi" from llm_request is already filtered by within-trace pipeline
    let system_count = result
        .messages
        .iter()
        .filter(|b| b.role == ChatRole::System)
        .count();
    assert!(
        system_count >= 1,
        "At least one system message should be preserved. Got {}",
        system_count
    );

    // Check that new content from trace2 is present
    let has_thanks = result
        .messages
        .iter()
        .any(|b| matches!(&b.content, ContentBlock::Text { text } if text == "Thanks"));
    assert!(has_thanks, "user('Thanks') should be present from trace2");

    let has_welcome = result
        .messages
        .iter()
        .any(|b| matches!(&b.content, ContentBlock::Text { text } if text == "Welcome"));
    assert!(has_welcome, "asst('Welcome') should be present from trace2");
}

// ----------------------------------------------------------------------------
// Test: ADK multi-span trace in session + Phase 4b
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_adk_multi_span() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    // Trace 1: single generation span
    let trace1_msg = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi there"}
        }
    ]);

    // Trace 2: history + new question
    let trace2_msg = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi there"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "How are you?"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "I am fine"}
        }
    ]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "s1",
            None,
            &trace1_msg.to_string(),
            t0,
            Some(t0),
            Some("generation"),
        ),
        make_span_row_full(
            "trace2",
            "s2",
            None,
            &trace2_msg.to_string(),
            t1,
            Some(t1),
            Some("generation"),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Trace 1: user("Hello") + asst("Hi there") = 2
    // Trace 2: prefix [user("Hello")] stripped (asst "Hi there" already filtered by 4b)
    //   → user("How are you?") + asst("I am fine") = 2
    // Total: 4
    let has_how = result
        .messages
        .iter()
        .any(|b| matches!(&b.content, ContentBlock::Text { text } if text == "How are you?"));
    assert!(has_how, "user('How are you?') from trace2 should survive");

    let has_fine = result
        .messages
        .iter()
        .any(|b| matches!(&b.content, ContentBlock::Text { text } if text == "I am fine"));
    assert!(has_fine, "asst('I am fine') from trace2 should survive");
}

// ----------------------------------------------------------------------------
// Test: retain by trace_id simulates the trace endpoint view
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_retain_trace_view() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    let msg1 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "First question"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "First answer"}
        }
    ]);

    let msg2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "First question"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "First answer"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "Second question"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Second answer"}
        }
    ]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "s1",
            None,
            &msg1.to_string(),
            t0,
            Some(t0),
            Some("generation"),
        ),
        make_span_row_full(
            "trace2",
            "s2",
            None,
            &msg2.to_string(),
            t1,
            Some(t1),
            Some("generation"),
        ),
    ];

    let options = FeedOptions::default();
    let mut result = process_spans(rows, &options);

    // Simulate trace endpoint: retain only trace2 blocks
    result.messages.retain(|b| b.trace_id == "trace2");

    // After prefix strip + retain: only trace2's NEW content
    let texts: Vec<&str> = result
        .messages
        .iter()
        .filter_map(|b| match &b.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        texts.contains(&"Second question"),
        "Should have 'Second question'. Got: {:?}",
        texts
    );
    assert!(
        texts.contains(&"Second answer"),
        "Should have 'Second answer'. Got: {:?}",
        texts
    );
    // History should NOT be present
    assert!(
        !texts.contains(&"First question"),
        "Should NOT have 'First question' (history). Got: {:?}",
        texts
    );
}

// ----------------------------------------------------------------------------
// Test: same-timestamp traces keep first-seen order for prefix strip
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_same_timestamp_trace_ordering() {
    let t0 = fixed_time();

    let msg1 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "First question"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "First answer"}
        }
    ]);

    let msg2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "First question"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "First answer"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Second question"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Second answer"}
        }
    ]);

    // Same timestamps and reverse lexical trace IDs: ordering must follow first-seen row
    // order (trace-z first), not trace_id sort (trace-a first).
    let mut row1 = make_span_row_full(
        "trace-z-older",
        "s1",
        None,
        &msg1.to_string(),
        t0,
        Some(t0),
        Some("generation"),
    );
    let mut row2 = make_span_row_full(
        "trace-a-newer",
        "s2",
        None,
        &msg2.to_string(),
        t0,
        Some(t0),
        Some("generation"),
    );
    row1.session_id = Some("session1".to_string());
    row2.session_id = Some("session1".to_string());

    let options = FeedOptions::default();
    let mut result = process_spans(vec![row1, row2], &options);

    // Simulate trace endpoint retain-by-trace behavior
    result.messages.retain(|b| b.trace_id == "trace-a-newer");
    let texts: Vec<&str> = result
        .messages
        .iter()
        .filter_map(|b| match &b.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        texts.contains(&"Second question"),
        "Expected 'Second question' in target trace. Got: {:?}",
        texts
    );
    assert!(
        texts.contains(&"Second answer"),
        "Expected 'Second answer' in target trace. Got: {:?}",
        texts
    );
    assert!(
        !texts.contains(&"First question"),
        "History prefix should be stripped from target trace. Got: {:?}",
        texts
    );
    assert!(
        !texts.contains(&"First answer"),
        "History prefix should be stripped from target trace. Got: {:?}",
        texts
    );
}

// ----------------------------------------------------------------------------
// Test: system blocks in prefix are transparent (do not break scan)
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_system_prefix_transparent() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    let msg1 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "You are helpful"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Q1"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "A1"}
        }
    ]);

    // Trace2 re-sends history (system + Q1 + A1 in llm_request) then adds new turn.
    let msg2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "system", "content": "You are helpful"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "Q1"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "A1"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "Q2"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "A2"}
        }
    ]);

    let mut row1 = make_span_row_full(
        "trace1",
        "s1",
        None,
        &msg1.to_string(),
        t0,
        Some(t0),
        Some("generation"),
    );
    let mut row2 = make_span_row_full(
        "trace2",
        "s2",
        None,
        &msg2.to_string(),
        t1,
        Some(t1),
        Some("generation"),
    );
    row1.session_id = Some("session1".to_string());
    row2.session_id = Some("session1".to_string());

    let options = FeedOptions::default();
    let mut result = process_spans(vec![row1, row2], &options);
    result.messages.retain(|b| b.trace_id == "trace2");

    let texts: Vec<&str> = result
        .messages
        .iter()
        .filter_map(|b| match &b.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        texts.contains(&"Q2"),
        "New user turn should remain. Got: {:?}",
        texts
    );
    assert!(
        texts.contains(&"A2"),
        "New assistant output should remain. Got: {:?}",
        texts
    );
    assert!(
        !texts.contains(&"Q1"),
        "History user message should be stripped despite leading system block. Got: {:?}",
        texts
    );
}

// ----------------------------------------------------------------------------
// Test: accumulated history matching allows gaps (subsequence, not strict)
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_prefix_subsequence_match() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    // Trace1 accumulated non-system sequence includes an output block ("B")
    // that won't be replayed in trace2 input prefix.
    let msg1 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "sys"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "A"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "B"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "C"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "D1"}
        }
    ]);

    // Trace2 replays A and C as history, but "B" is absent from prefix.
    // Strict matching would stop at C; subsequence matching should still strip C.
    let msg2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "system", "content": "sys"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "A"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "C"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "E"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "F"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "D2"}
        }
    ]);

    let mut row1 = make_span_row_full(
        "trace1",
        "s1",
        None,
        &msg1.to_string(),
        t0,
        Some(t0),
        Some("generation"),
    );
    let mut row2 = make_span_row_full(
        "trace2",
        "s2",
        None,
        &msg2.to_string(),
        t1,
        Some(t1),
        Some("generation"),
    );
    row1.session_id = Some("session1".to_string());
    row2.session_id = Some("session1".to_string());

    let options = FeedOptions::default();
    let mut result = process_spans(vec![row1, row2], &options);
    result.messages.retain(|b| b.trace_id == "trace2");

    let texts: Vec<&str> = result
        .messages
        .iter()
        .filter_map(|b| match &b.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        texts.contains(&"E"),
        "New content should remain. Got: {:?}",
        texts
    );
    assert!(
        texts.contains(&"F"),
        "New content should remain. Got: {:?}",
        texts
    );
    assert!(
        texts.contains(&"D2"),
        "New assistant response should remain. Got: {:?}",
        texts
    );
    assert!(
        !texts.contains(&"A"),
        "History A should be stripped. Got: {:?}",
        texts
    );
    assert!(
        !texts.contains(&"C"),
        "History C should be stripped even with accumulated gap. Got: {:?}",
        texts
    );
}

// ----------------------------------------------------------------------------
// Test: cross-trace prefix scan applies per span (not only trace start)
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_prefix_resets_per_span() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);
    let t2 = t0 + chrono::Duration::seconds(20);

    // Prior trace contributes A/B to accumulated history.
    let trace1_msg = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "sys"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "A"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "B"}
        }
    ]);

    // Target trace span1 starts with new content C.
    let trace2_span1 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "system", "content": "sys"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "C"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "D"}
        }
    ]);

    // Target trace span2 replays A/C, then adds E.
    // A should be stripped via cross-trace prefix even though it appears
    // in the second span, not at trace start.
    let trace2_span2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "system", "content": "sys"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "A"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "C"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "E"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t2.to_rfc3339()}},
            "content": {"role": "assistant", "content": "F"}
        }
    ]);

    let mut row1 = make_span_row_full(
        "trace1",
        "s1",
        None,
        &trace1_msg.to_string(),
        t0,
        Some(t0),
        Some("generation"),
    );
    let mut row2 = make_span_row_full(
        "trace2",
        "s2",
        None,
        &trace2_span1.to_string(),
        t1,
        Some(t1),
        Some("generation"),
    );
    let mut row3 = make_span_row_full(
        "trace2",
        "s3",
        None,
        &trace2_span2.to_string(),
        t2,
        Some(t2),
        Some("generation"),
    );
    row1.session_id = Some("session1".to_string());
    row2.session_id = Some("session1".to_string());
    row3.session_id = Some("session1".to_string());

    let options = FeedOptions::default();
    let mut result = process_spans(vec![row1, row2, row3], &options);
    result.messages.retain(|b| b.trace_id == "trace2");

    let texts: Vec<&str> = result
        .messages
        .iter()
        .filter_map(|b| match &b.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        texts.contains(&"C"),
        "New C should remain. Got: {:?}",
        texts
    );
    assert!(
        texts.contains(&"E"),
        "New E should remain. Got: {:?}",
        texts
    );
    assert!(
        texts.contains(&"F"),
        "Final F should remain. Got: {:?}",
        texts
    );
    assert!(
        !texts.contains(&"A"),
        "Cross-trace replay A should be stripped in span2 prefix. Got: {:?}",
        texts
    );
}

// ----------------------------------------------------------------------------
// Test: Replay trace contributes 0 tool defs
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_replay_no_tool_defs() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    let msg = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Use the tool"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Done"}
        }
    ]);

    let tool_defs =
        json!([{"type": "function", "function": {"name": "my_tool", "parameters": {}}}])
            .to_string();

    // Trace2 is pure replay
    let msg2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "Use the tool"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Done"}
        }
    ]);

    let mut row1 = make_span_row_full(
        "trace1",
        "s1",
        None,
        &msg.to_string(),
        t0,
        Some(t0),
        Some("generation"),
    );
    row1.tool_definitions_json = tool_defs.clone();

    let mut row2 = make_span_row_full(
        "trace2",
        "s2",
        None,
        &msg2.to_string(),
        t1,
        Some(t1),
        Some("generation"),
    );
    row2.tool_definitions_json = tool_defs;

    let options = FeedOptions::default();
    let result = process_spans(vec![row1, row2], &options);

    // Both traces contribute (guard prevents marking for pure replay).
    // Tool defs are deduplicated by name, so still 1 unique tool.
    assert_eq!(
        result.tool_definitions.len(),
        1,
        "Should have exactly 1 unique tool def after dedup. Got {}",
        result.tool_definitions.len()
    );
}

// ----------------------------------------------------------------------------
// Test: Repeated content AFTER prefix break is safe
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_repeated_content_safe() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    // Trace 1: user("yes") + asst("confirmed")
    let msg1 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "yes"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "confirmed"}
        }
    ]);

    // Trace 2: different prefix + user("yes") after break
    let msg2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "new question"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "yes"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "done"}
        }
    ]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "s1",
            None,
            &msg1.to_string(),
            t0,
            Some(t0),
            Some("generation"),
        ),
        make_span_row_full(
            "trace2",
            "s2",
            None,
            &msg2.to_string(),
            t1,
            Some(t1),
            Some("generation"),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Trace2 prefix: "new question" NOT in accumulated → STOP immediately → 0 stripped
    // So trace2 keeps: user("new question") + user("yes") + asst("done")
    let yes_count = result
        .messages
        .iter()
        .filter(|b| matches!(&b.content, ContentBlock::Text { text } if text == "yes"))
        .count();
    assert!(
        yes_count >= 2,
        "Both 'yes' should be preserved (different contexts). Found {}",
        yes_count
    );
}

// ----------------------------------------------------------------------------
// Test: cross-trace prefix matching is role-sensitive
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_prefix_role_sensitive() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    // Trace 1: assistant says "yes"
    let msg1 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Question 1"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "yes"}
        }
    ]);

    // Trace 2: user says "yes" as new input (same content, different role)
    let msg2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "yes"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Acknowledged"}
        }
    ]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "s1",
            None,
            &msg1.to_string(),
            t0,
            Some(t0),
            Some("generation"),
        ),
        make_span_row_full(
            "trace2",
            "s2",
            None,
            &msg2.to_string(),
            t1,
            Some(t1),
            Some("generation"),
        ),
    ];

    let options = FeedOptions::default();
    let mut result = process_spans(rows, &options);
    result.messages.retain(|b| b.trace_id == "trace2");

    let texts: Vec<&str> = result
        .messages
        .iter()
        .filter_map(|b| match &b.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        texts.contains(&"yes"),
        "User 'yes' in trace2 must not be stripped by assistant 'yes' from trace1. Got: {:?}",
        texts
    );
    assert!(
        texts.contains(&"Acknowledged"),
        "Trace2 assistant output should remain. Got: {:?}",
        texts
    );
}

// ----------------------------------------------------------------------------
// Test: mixed event+attribute duplicates keep event copy in target trace
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_prefix_mixed_source_event_survives() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    // Trace 1 contributes "repeat" to accumulated history.
    let msg1 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "repeat"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "first"}
        }
    ]);

    // Trace 2 has the same user text from both event and attribute sources.
    // Cross-trace prefix must only mark the attribute copy as history.
    let msg2 = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "repeat"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "repeat"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "fresh"}
        }
    ]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "s1",
            None,
            &msg1.to_string(),
            t0,
            Some(t0),
            Some("generation"),
        ),
        make_span_row_full(
            "trace2",
            "s2",
            None,
            &msg2.to_string(),
            t1,
            Some(t1),
            Some("generation"),
        ),
    ];

    let options = FeedOptions::default();
    let mut result = process_spans(rows, &options);
    result.messages.retain(|b| b.trace_id == "trace2");

    let repeat_blocks: Vec<_> = result
        .messages
        .iter()
        .filter(|b| matches!(&b.content, ContentBlock::Text { text } if text == "repeat"))
        .collect();

    assert_eq!(
        repeat_blocks.len(),
        1,
        "Exactly one 'repeat' should survive in trace2 after dedup. Got {:?}",
        result
            .messages
            .iter()
            .filter_map(|b| match &b.content {
                ContentBlock::Text { text } => Some((text.as_str(), b.source_type.as_str())),
                _ => None,
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        repeat_blocks[0].source_type,
        source_type::EVENT,
        "Event-sourced user message should win over attribute history copy"
    );
    assert!(
        result
            .messages
            .iter()
            .any(|b| matches!(&b.content, ContentBlock::Text { text } if text == "fresh")),
        "New assistant output should remain"
    );
}

// ----------------------------------------------------------------------------
// Test: repeated matches are bounded by accumulated occurrence count
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_prefix_occurrence_count_bounded() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);
    let t2 = t0 + chrono::Duration::seconds(20);

    // Trace 1 contributes one "ping".
    let msg1 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "ping"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "a1"}
        }
    ]);

    // Trace 2 contributes a second "ping" after a prefix break.
    let msg2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "barrier"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "ping"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "a2"}
        }
    ]);

    // Trace 3 starts with three "ping" entries.
    // Only first two should be stripped (bounded by accumulated count = 2).
    let msg3 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "ping"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "ping"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "ping"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t2.to_rfc3339()}},
            "content": {"role": "user", "content": "tail"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t2.to_rfc3339()}},
            "content": {"role": "assistant", "content": "a3"}
        }
    ]);

    let mut row1 = make_span_row_full(
        "trace1",
        "s1",
        None,
        &msg1.to_string(),
        t0,
        Some(t0),
        Some("generation"),
    );
    let mut row2 = make_span_row_full(
        "trace2",
        "s2",
        None,
        &msg2.to_string(),
        t1,
        Some(t1),
        Some("generation"),
    );
    let mut row3 = make_span_row_full(
        "trace3",
        "s3",
        None,
        &msg3.to_string(),
        t2,
        Some(t2),
        Some("generation"),
    );
    row1.session_id = Some("session1".to_string());
    row2.session_id = Some("session1".to_string());
    row3.session_id = Some("session1".to_string());

    let options = FeedOptions::default();
    let mut result = process_spans(vec![row1, row2, row3], &options);
    result.messages.retain(|b| b.trace_id == "trace3");

    let texts: Vec<&str> = result
        .messages
        .iter()
        .filter_map(|b| match &b.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    let ping_count = texts.iter().filter(|&&t| t == "ping").count();
    assert_eq!(
        ping_count, 1,
        "Only one non-history 'ping' should remain in trace3. Got {:?}",
        texts
    );
    assert!(
        texts.contains(&"tail"),
        "Content after prefix break should remain. Got {:?}",
        texts
    );
    assert!(
        texts.contains(&"a3"),
        "New assistant output should remain. Got {:?}",
        texts
    );
}

// ----------------------------------------------------------------------------
// Test: Strands traces with "yes" in both (different turns) → both preserved
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_strands_repeated_yes() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    // Strands: unique events per trace
    let msg1 = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Do you agree?"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "yes"}
        }
    ]);

    let msg2 = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "Confirm again?"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "yes"}
        }
    ]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "s1", None, &msg1.to_string(), t0, Some(t0)),
        make_span_row_with_timestamps("trace2", "s2", None, &msg2.to_string(), t1, Some(t1)),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // "Do you agree?" not in accumulated → STOP immediately → 0 stripped from trace2
    assert_eq!(
        result.messages.len(),
        4,
        "Strands: all 4 blocks preserved. Got {}",
        result.messages.len()
    );

    let yes_count = result
        .messages
        .iter()
        .filter(|b| matches!(&b.content, ContentBlock::Text { text } if text == "yes"))
        .count();
    assert_eq!(yes_count, 2, "Both 'yes' should be preserved");
}

// ----------------------------------------------------------------------------
// Test: Multi-trace detection routing check
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_multi_trace_detection() {
    let t0 = fixed_time();

    // Single trace: should go through process_trace_spans path
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
        "content": {"role": "user", "content": "Hello"}
    }]);

    let single_row =
        make_span_row_with_timestamps("trace1", "s1", None, &msg.to_string(), t0, Some(t0));
    let options = FeedOptions::default();

    // Single trace
    let r1 = process_spans(vec![single_row.clone()], &options);
    let r2 = process_trace_spans(vec![single_row], &options);
    assert_eq!(r1.messages.len(), r2.messages.len());

    // Two traces with same content
    let t1 = t0 + chrono::Duration::seconds(10);
    let msg2 = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": t1.to_rfc3339()}},
        "content": {"role": "user", "content": "Different"}
    }]);

    let rows = vec![
        make_span_row_with_timestamps("trace1", "s1", None, &msg.to_string(), t0, Some(t0)),
        make_span_row_with_timestamps("trace2", "s2", None, &msg2.to_string(), t1, Some(t1)),
    ];

    let r3 = process_spans(rows, &options);
    // Multi-trace path: both unique → both preserved
    assert_eq!(r3.messages.len(), 2);
}

// ----------------------------------------------------------------------------
// Test: Genuine repeated user message preserved (the reported bug)
// User asks the same question in trace 2 as in trace 1.
// The history re-send copy should be stripped but the genuine copy preserved.
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_genuine_repeat_preserved() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    // Trace 1: user("Hello") → assistant("Hi")
    let msg1 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi there"}
        }
    ]);

    // Trace 2: history re-send [user("Hello"), asst("Hi")] + genuine repeat user("Hello")
    // Framework re-sends full history as prefix of llm_request, then adds new message.
    // The new message happens to be "Hello" again (user asks the same question).
    let msg2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi there"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hello again!"}
        }
    ]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "s1",
            None,
            &msg1.to_string(),
            t0,
            Some(t0),
            Some("generation"),
        ),
        make_span_row_full(
            "trace2",
            "s2",
            None,
            &msg2.to_string(),
            t1,
            Some(t1),
            Some("generation"),
        ),
    ];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Trace 1: user("Hello") + asst("Hi there") = 2
    // Trace 2: Cross-trace prefix marks first user("Hello") and asst("Hi there") as history.
    //   Within-trace dedup: user("Hello") has history copy + genuine copy → non-history wins.
    //   asst("Hi there") from llm_request is history-only → dropped.
    //   Result: user("Hello") + asst("Hello again!") = 2
    // Total: 4
    let texts: Vec<&str> = result
        .messages
        .iter()
        .filter_map(|b| match &b.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    // The genuine "Hello" from trace 2 MUST be preserved
    let hello_count = texts.iter().filter(|&&t| t == "Hello").count();
    assert_eq!(
        hello_count, 2,
        "Both 'Hello' messages (trace1 + trace2 genuine) must be preserved. Got: {:?}",
        texts
    );

    // The new response from trace 2 MUST be present
    assert!(
        texts.contains(&"Hello again!"),
        "asst('Hello again!') from trace2 must be preserved. Got: {:?}",
        texts
    );

    // History re-send of "Hi there" from trace2's llm_request should be dropped
    let hi_count = texts.iter().filter(|&&t| t == "Hi there").count();
    assert_eq!(
        hi_count, 1,
        "Only trace1's 'Hi there' should survive (trace2's is history). Got: {:?}",
        texts
    );
}

// ----------------------------------------------------------------------------
// Test: Trace endpoint view with genuine repeated content
// Simulates the exact bug scenario: viewing a single trace via the trace endpoint
// where the trace has messages with same content as prior traces.
// ----------------------------------------------------------------------------

#[test]
fn test_cross_trace_retain_genuine_repeat_trace_view() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(10);

    let msg1 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi"}
        }
    ]);

    // Trace 2: re-sends history + user says "Hello" again
    let msg2 = json!([
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_request", "time": t1.to_rfc3339()}},
            "content": {"role": "user", "content": "Hello"}
        },
        {
            "source": {"attribute": {"key": "gcp.vertex.agent.llm_response", "time": t1.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Hi again"}
        }
    ]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "s1",
            None,
            &msg1.to_string(),
            t0,
            Some(t0),
            Some("generation"),
        ),
        make_span_row_full(
            "trace2",
            "s2",
            None,
            &msg2.to_string(),
            t1,
            Some(t1),
            Some("generation"),
        ),
    ];

    let options = FeedOptions::default();
    let mut result = process_spans(rows, &options);

    // Simulate trace endpoint: retain only trace2 blocks
    result.messages.retain(|b| b.trace_id == "trace2");

    let texts: Vec<&str> = result
        .messages
        .iter()
        .filter_map(|b| match &b.content {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();

    // The genuine "Hello" from trace2 MUST be present (this was the reported bug)
    assert!(
        texts.contains(&"Hello"),
        "Genuine 'Hello' from trace2 must be preserved in trace view. Got: {:?}",
        texts
    );
    assert!(
        texts.contains(&"Hi again"),
        "New response 'Hi again' from trace2 must be present. Got: {:?}",
        texts
    );
}

// ============================================================================
// LOGFIRE / OPENAI AGENTS: ASSISTANT PROMOTION IN CHOICELESS GENERATION SPANS
// ============================================================================
// Logfire stores LLM output as gen_ai.assistant.message (not gen_ai.choice).
// Without promotion, assistant messages sort by array index alongside inputs,
// causing incorrect ordering (system=0, assistant=1, user=2 → assistant before user).

#[test]
fn test_logfire_assistant_promoted_when_no_choice() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(2);

    // Generation span with parent, no gen_ai.choice — only gen_ai.*.message events
    let msgs = json!([
        {
            "source": {"event": {"name": "gen_ai.system.message", "time": t0.to_rfc3339()}},
            "content": {"role": "system", "content": "You are helpful."}
        },
        {
            "source": {"event": {"name": "gen_ai.assistant.message", "time": t0.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Here is the answer."}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": t0.to_rfc3339()}},
            "content": {"role": "user", "content": "What is 2+2?"}
        }
    ]);

    let rows = vec![make_span_row_with_timestamps(
        "trace1",
        "gen-span",
        Some("parent-span"),
        &msgs.to_string(),
        t0,
        Some(t1),
    )];

    let options = FeedOptions::default();
    let result = process_spans(rows, &options);

    // Collect roles in order
    let roles: Vec<ChatRole> = result.messages.iter().map(|b| b.role).collect();

    // Assistant should come AFTER user (promoted to span_end timestamp)
    assert_eq!(
        roles,
        vec![ChatRole::System, ChatRole::User, ChatRole::Assistant],
        "Expected system -> user -> assistant, got {:?}",
        roles
    );

    // Verify the promoted assistant block has correct flags
    let assistant_block = result
        .messages
        .iter()
        .find(|b| b.role == ChatRole::Assistant)
        .expect("Should have assistant block");
    assert!(
        assistant_block.uses_span_end,
        "Promoted assistant should use span_end"
    );
    assert!(
        assistant_block.is_protected(),
        "Promoted assistant should be protected from history marking"
    );
}

#[test]
fn test_no_promotion_when_choice_exists() {
    // Verify promotion is suppressed when gen_ai.choice is present.
    // Uses classify_blocks directly to test the classification logic
    // without dedup/history phases interfering.
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(2);

    let span_timestamps = std::collections::HashMap::from([(
        "gen-span".to_string(),
        super::dedup::SpanTimestamps {
            span_start: t0,
            span_end: Some(t1),
        },
    )]);

    // Build blocks manually: gen_ai.assistant.message + gen_ai.choice in same gen span
    let assistant_block = BlockEntry {
        position: PositionPath::default(),
        entry_type: "text".to_string(),
        content: ContentBlock::Text {
            text: "Previous response.".to_string(),
        },
        role: ChatRole::Assistant,
        trace_id: "trace1".to_string(),
        span_id: "gen-span".to_string(),
        session_id: None,
        message_index: 1,
        entry_index: 0,
        parent_span_id: Some("parent-span".to_string()),
        span_path: vec!["parent-span".to_string(), "gen-span".to_string()],
        timestamp: t0,
        order_time: t0,
        observation_type: Some("generation".to_string()),
        model: Some("gpt-4".to_string()),
        provider: Some("openai".to_string()),
        name: None,
        finish_reason: None,
        tool_use_id: None,
        tool_name: None,
        tokens: None,
        cost: None,
        status_code: None,
        is_error: false,
        source_type: "event".to_string(),
        event_name: Some("gen_ai.assistant.message".to_string()),
        source_attribute: None,
        category: crate::data::types::MessageCategory::GenAIAssistantMessage,
        content_hash: "hash_prev".to_string(),
        is_semantic: true,
        uses_span_end: false,
        is_history: false,
        tool_use_id_correlated: false,
        promoted_to_span_output: false,
    };

    let choice_block = BlockEntry {
        position: PositionPath::default(),
        entry_type: "text".to_string(),
        content: ContentBlock::Text {
            text: "4".to_string(),
        },
        role: ChatRole::Assistant,
        trace_id: "trace1".to_string(),
        span_id: "gen-span".to_string(),
        session_id: None,
        message_index: 3,
        entry_index: 0,
        parent_span_id: Some("parent-span".to_string()),
        span_path: vec!["parent-span".to_string(), "gen-span".to_string()],
        timestamp: t1,
        order_time: t1,
        observation_type: Some("generation".to_string()),
        model: Some("gpt-4".to_string()),
        provider: Some("openai".to_string()),
        name: None,
        finish_reason: Some(FinishReason::Stop),
        tool_use_id: None,
        tool_name: None,
        tokens: None,
        cost: None,
        status_code: None,
        is_error: false,
        source_type: "event".to_string(),
        event_name: Some("gen_ai.choice".to_string()),
        source_attribute: None,
        category: crate::data::types::MessageCategory::GenAIChoice,
        content_hash: "hash_4".to_string(),
        is_semantic: true,
        uses_span_end: false,
        is_history: false,
        tool_use_id_correlated: false,
        promoted_to_span_output: false,
    };

    let mut blocks = vec![assistant_block, choice_block];
    super::classify_blocks(&mut blocks, &span_timestamps);

    // gen_ai.choice should be classified normally (uses_span_end from is_protected)
    let choice = &blocks[1];
    assert!(choice.is_protected(), "gen_ai.choice should be protected");
    assert!(choice.uses_span_end, "gen_ai.choice should use span_end");

    // gen_ai.assistant.message should NOT be promoted (choice exists in this span)
    let asst = &blocks[0];
    assert_eq!(
        asst.category,
        crate::data::types::MessageCategory::GenAIAssistantMessage,
        "gen_ai.assistant.message should keep original category when choice exists"
    );
    assert!(
        !asst.uses_span_end,
        "gen_ai.assistant.message should NOT use span_end when choice exists"
    );
}

// ============================================================================
// Role filter
// ============================================================================

/// The Gemini/ADK shape: a tool result arrives inside a message whose raw role is `user`, and
/// the pipeline derives the block's role as `tool` from its content.
fn gemini_tool_result_row() -> MessageSpanRow {
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {
            "role": "user",
            "content": [
                {"functionResponse": {"name": "get_weather", "response": {"temp": 25}}}
            ]
        }
    }]);
    make_span_row(
        "trace-role",
        "span-role",
        None,
        &msg.to_string(),
        "[]",
        "[]",
    )
}

/// `?role=tool` must return a Gemini tool result.
///
/// The filter used to be applied to the raw message role before the per-block role was
/// derived, so `role=tool` dropped every Gemini and ADK tool result (raw role `user`) and
/// `role=user` returned blocks whose role is `tool`.
#[test]
fn role_filter_matches_the_derived_role_not_the_raw_one() {
    let rows = vec![gemini_tool_result_row()];

    let unfiltered = process_spans(rows.clone(), &FeedOptions::new());
    let derived: Vec<&str> = unfiltered
        .messages
        .iter()
        .map(|b| b.role.as_str())
        .collect();
    assert_eq!(
        derived,
        vec!["tool"],
        "precondition: the block's derived role is tool"
    );

    let as_tool = process_spans(
        rows.clone(),
        &FeedOptions::new().with_role(Some("tool".into())),
    );
    assert_eq!(
        as_tool.messages.len(),
        1,
        "role=tool must return the tool result; the raw message role is user"
    );

    let as_user = process_spans(rows, &FeedOptions::new().with_role(Some("user".into())));
    assert!(
        as_user.messages.is_empty(),
        "role=user must not return a block whose derived role is tool, got {:?}",
        as_user
            .messages
            .iter()
            .map(|b| b.role.as_str())
            .collect::<Vec<_>>()
    );
}

/// A time window must not change which messages are *history*.
///
/// The lower bound used to be passed to the message query, which removed the earlier spans that
/// history detection reads. With nothing to recognise a re-send against, the re-sent turns came
/// back as new messages - so narrowing a window could *increase* what a trace appeared to contain.
/// The window is a filter on the answer, applied after the pipeline has seen the context.
#[test]
fn a_time_window_only_removes_messages() {
    let earlier = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {"role": "user", "content": "first question"}
    }]);
    // The later span re-sends the first turn, as every framework does, plus a new one.
    let later = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
            "content": {"role": "user", "content": "first question"}
        },
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:05Z"}},
            "content": {"role": "user", "content": "second question"}
        }
    ]);

    let rows = vec![
        make_span_row_with_timestamps(
            "trace1",
            "span1",
            None,
            &earlier.to_string(),
            fixed_time(),
            Some(fixed_time() + chrono::Duration::seconds(1)),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "span2",
            None,
            &later.to_string(),
            fixed_time() + chrono::Duration::seconds(5),
            Some(fixed_time() + chrono::Duration::seconds(6)),
        ),
    ];

    let full = process_spans(rows.clone(), &FeedOptions::new());
    let windowed = apply_time_window(
        process_spans(rows, &FeedOptions::new()),
        Some(fixed_time() + chrono::Duration::seconds(2)),
        None,
    );

    assert!(
        windowed.messages.len() <= full.messages.len(),
        "a window returned more messages ({}) than the unwindowed feed ({})",
        windowed.messages.len(),
        full.messages.len()
    );
    assert_eq!(
        windowed.metadata.block_count,
        windowed.messages.len(),
        "the window reported a count that does not match what it returned"
    );
    for block in &windowed.messages {
        assert!(
            block.timestamp >= fixed_time() + chrono::Duration::seconds(2),
            "a block outside the window was returned: {}",
            block.timestamp
        );
    }
}

/// The project feed must not split a response.
///
/// `process_feed` recognises a response by its blocks sharing a timestamp. When each block carried
/// its own birth time, a response whose text was timestamped at span end and whose tool call was
/// timestamped at event time stopped being one response, and a block from another response could
/// land between them.
///
/// Note this endpoint is newest-first, so a later tool result appearing *before* the earlier call
/// it answers is correct here - the ordering to check is within a response, and that responses do
/// not interleave.
#[test]
fn the_feed_keeps_a_response_together() {
    let msg = json!([
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:01Z"}},
            "content": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "looking that up"},
                    {"type": "tool_use", "id": "call-1", "name": "lookup", "input": {"q": "a"}}
                ]
            }
        },
        {
            "source": {"event": {"name": "gen_ai.tool.message", "time": "2025-01-01T00:00:02Z"}},
            "content": {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "call-1", "name": "lookup",
                             "content": "answer"}]
            }
        }
    ]);

    // The span ends after the tool result, so the text's birth time falls after it.
    let start = "2025-01-01T00:00:00Z"
        .parse::<chrono::DateTime<Utc>>()
        .expect("valid timestamp");
    let mut row = make_span_row_with_timestamps(
        "trace1",
        "span1",
        None,
        &msg.to_string(),
        start,
        Some(start + chrono::Duration::seconds(4)),
    );
    row.ingested_at = start;

    let feed = process_feed(vec![row], &FeedOptions::new());
    let kinds: Vec<&str> = feed
        .messages
        .iter()
        .map(|b| b.entry_type.as_str())
        .collect();

    // The response's two blocks are adjacent and in the order the model produced them.
    let text_at = kinds
        .iter()
        .position(|k| *k == "text")
        .expect("the response's text");
    let call_at = kinds
        .iter()
        .position(|k| *k == "tool_use")
        .expect("the response's tool call");
    assert_eq!(
        call_at,
        text_at + 1,
        "the response was split, or its blocks were reordered: {kinds:?}"
    );

    // And every block of one response carries one timestamp, which is what keeps it together.
    let response_times: std::collections::BTreeSet<_> = feed
        .messages
        .iter()
        .filter(|b| b.entry_type == "text" || b.entry_type == "tool_use")
        .map(|b| b.timestamp)
        .collect();
    assert_eq!(
        response_times.len(),
        1,
        "the response's blocks reported {} different times",
        response_times.len()
    );
}

/// A span's input and its output are different responses, even at the same timestamp.
///
/// Attribute extraction gives every message of a span the span's start time, so keying a response
/// on time alone made `input.value` and `output.value` one unit. The earlier time was then
/// materialised onto both, and the completed output was reported as having happened when the span
/// started - which a time window could drop, and which is simply the wrong time to show.
#[test]
fn a_spans_input_and_output_are_not_one_response() {
    let start = fixed_time();
    let end = start + chrono::Duration::seconds(30);
    let msg = json!([
        {
            "source": {"attribute": {"key": "input.value", "time": start.to_rfc3339()}},
            "content": {"role": "user", "content": "what is the capital of France?"}
        },
        {
            // Structured output, which is what carries a span-end timestamp: a plain text
            // output.value is not classified as a completion and keeps the span's start time.
            "source": {"attribute": {"key": "output.value", "time": start.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "json", "data": {"json": {"capital": "Paris"}}}]
            }
        }
    ]);

    let row =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), start, Some(end));

    let result = process_spans(vec![row], &FeedOptions::new());
    let output = result
        .messages
        .iter()
        .find(|b| b.role == ChatRole::Assistant)
        .expect("the span's output");
    let input = result
        .messages
        .iter()
        .find(|b| b.role == ChatRole::User)
        .expect("the span's input");

    assert_eq!(
        input.timestamp, start,
        "the input belongs at the span's start"
    );
    assert_eq!(
        output.timestamp, end,
        "the output completed when the span did, and must not inherit the input's time"
    );
}

/// The bundled OTel event pair must be recognised as input and output.
///
/// The current conventions carry a turn as `gen_ai.input.messages` and `gen_ai.output.messages` on
/// one `gen_ai.client.inference.operation.details` event, so both arrive at the same instant. With
/// only the older event names classified, the output was not recognised as output: it shared a
/// response with the input, took the input's timestamp, and was not protected from history marking.
#[test]
fn the_bundled_otel_event_pair_is_classified() {
    let start = fixed_time();
    let end = start + chrono::Duration::seconds(20);
    let msg = json!([
        {
            "source": {"event": {"name": "gen_ai.input.messages", "time": start.to_rfc3339()}},
            "content": {"role": "user", "content": "what is the capital of France?"}
        },
        {
            "source": {"event": {"name": "gen_ai.output.messages", "time": start.to_rfc3339()}},
            "content": {"role": "assistant", "content": "Paris."}
        }
    ]);

    let row =
        make_span_row_with_timestamps("trace1", "span1", None, &msg.to_string(), start, Some(end));
    let result = process_spans(vec![row], &FeedOptions::new());

    let input = result
        .messages
        .iter()
        .find(|b| b.role == ChatRole::User)
        .expect("the turn's input");
    let output = result
        .messages
        .iter()
        .find(|b| b.role == ChatRole::Assistant)
        .expect("the turn's output");

    assert_eq!(
        input.timestamp, start,
        "the input is timestamped at the event"
    );
    assert_eq!(
        output.timestamp, end,
        "the output completed when the span did, and must not inherit the input's time"
    );
}

/// A tool result never precedes the call it answers, even when both spans report the same instant.
///
/// A message index restarts at zero in every span, so between spans it means nothing: a generation
/// span whose response opens with text gives its call index 1, the tool span carrying the result
/// starts at 0, and ordering the tie by index put the answer before the question.
///
/// Settling it *at comparison time* by role was tried three ways and each broke a framework - per
/// pair it is cyclic, per response it merges ADK's turns, per span it interleaves Vercel's parallel
/// calls with their results; all three are recorded in `dedup.rs`. Settling it beforehand does work:
/// the result takes its position from its own call, which is a property of the block rather than of
/// the pair, so the key stays a set of values and the order stays total. Only a cross-span tie is
/// adjusted, which is why the ADK and Vercel shapes are unaffected - their calls and results share a
/// span.
#[test]
fn a_cross_span_tie_keeps_a_result_after_its_call() {
    let t = fixed_time();
    // The generation span: introductory text, then the call. The call is index 1.
    let generation = json!([
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t.to_rfc3339()}},
            "content": {"role": "assistant", "content": "let me look that up"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": t.to_rfc3339()}},
            "content": {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call-1", "name": "lookup",
                             "input": {"q": "a"}}]
            }
        }
    ]);
    // The tool span: the result, index 0, at the same instant.
    let tool = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": t.to_rfc3339()}},
        "content": {
            "role": "tool",
            "content": [{"type": "tool_result", "tool_use_id": "call-1", "name": "lookup",
                         "content": "answer"}]
        }
    }]);

    let mut tool_row = make_span_row_with_timestamps(
        "trace1",
        "span2",
        Some("span1"),
        &tool.to_string(),
        t,
        Some(t),
    );
    tool_row.observation_type = Some("tool".to_string());

    let rows = vec![
        make_span_row_with_timestamps("trace1", "span1", None, &generation.to_string(), t, Some(t)),
        tool_row,
    ];

    let result = process_spans(rows, &FeedOptions::new());
    let kinds: Vec<&str> = result
        .messages
        .iter()
        .map(|b| b.entry_type.as_str())
        .collect();
    assert_eq!(
        kinds,
        vec!["text", "tool_use", "tool_result"],
        "the result took its position from the call it answers, so it follows it"
    );
}

/// A span delivered twice must be billed once.
///
/// The DuckDB message query reads the raw span table, where a re-ingested span appears twice, while
/// ClickHouse reads it with FINAL. Summing rows therefore doubled a conversation's tokens and cost
/// on one backend, even though the messages themselves are deduplicated and appear once.
#[test]
fn a_span_delivered_twice_is_counted_once() {
    let msg = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {"role": "user", "content": "hello"}
    }]);
    let row = make_span_row("trace1", "span1", None, &msg.to_string(), "[]", "[]");

    let once = process_spans(vec![row.clone()], &FeedOptions::new());
    let twice = process_spans(vec![row.clone(), row], &FeedOptions::new());

    assert_eq!(
        twice.messages.len(),
        once.messages.len(),
        "the duplicate delivery added a message"
    );
    assert_eq!(
        twice.metadata.total_tokens, once.metadata.total_tokens,
        "the duplicate delivery was billed twice"
    );
    assert_eq!(twice.metadata.total_cost, once.metadata.total_cost);
    assert_eq!(twice.metadata.span_count, 1);
}

/// The feed must keep a trace whole when only its root span names the session.
///
/// Several frameworks record the session id on the root span alone. Grouping by each row's own id
/// then split a conversation: the root joined the session group and its children a trace group, so
/// history detection ran on the halves separately and a re-sent turn survived in one of them.
#[test]
fn the_feed_groups_a_trace_by_its_root_session_id() {
    let first = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {"role": "user", "content": "first question"}
    }]);
    // The child re-sends the first turn, as a generation span does, and adds nothing new.
    let child = json!([{
        "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {"role": "user", "content": "first question"}
    }]);

    let mut root = make_span_row("trace1", "root", None, &first.to_string(), "[]", "[]");
    root.session_id = Some("session-1".to_string());
    // No session id on the child, which is what the frameworks in question emit.
    let child_row = make_span_row(
        "trace1",
        "child",
        Some("root"),
        &child.to_string(),
        "[]",
        "[]",
    );

    let feed = process_feed(vec![root, child_row], &FeedOptions::new());
    let users: Vec<&str> = feed
        .messages
        .iter()
        .filter(|b| b.role == ChatRole::User)
        .map(|b| b.span_id.as_str())
        .collect();
    assert_eq!(
        users.len(),
        1,
        "the re-sent turn survived, so the trace was processed as two conversations: {users:?}"
    );
}

/// A session's cost covers every trace in it, including one that only re-sent an earlier turn.
///
/// The multi-trace path added a trace's tokens only when it contributed a message the feed kept, so
/// a trace whose content was all history counted as free. It still called the model. The response
/// documents its totals as covering the spans in scope, and that is what they now do.
#[test]
fn a_replayed_trace_still_counts_towards_the_session() {
    let first = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
            "content": {"role": "user", "content": "the question"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:01Z"}},
            "content": {"role": "assistant", "content": "the answer"}
        }
    ]);
    // The second trace re-sends the same turn and adds nothing.
    let replay = first.clone();

    let mut a = make_span_row("trace1", "span1", None, &first.to_string(), "[]", "[]");
    a.session_id = Some("session-1".to_string());
    let mut b = make_span_row("trace2", "span2", None, &replay.to_string(), "[]", "[]");
    b.session_id = Some("session-1".to_string());
    b.span_timestamp = a.span_timestamp + chrono::Duration::seconds(10);
    let per_trace_tokens = a.total_tokens;
    let per_trace_cost = a.cost_total;

    let result = process_spans(vec![a, b], &FeedOptions::new());

    assert_eq!(
        result.metadata.total_tokens,
        per_trace_tokens * 2,
        "the replayed trace called the model and must be counted"
    );
    assert!(
        (result.metadata.total_cost - per_trace_cost * 2.0).abs() < f64::EPSILON,
        "the replayed trace's cost is missing: {}",
        result.metadata.total_cost
    );
}

/// A span delivered twice is billed once, in a session exactly as in a trace.
///
/// The DuckDB session query reads the raw span table, so a retried OTLP delivery is two rows for
/// one span; ClickHouse reads it with FINAL and returns one. The single-trace path collapsed them
/// in `compute_metadata` and the multi-trace path summed rows, so a session reported double the
/// tokens of the traces it contains - and the two backends disagreed with each other.
#[test]
fn a_span_delivered_twice_is_counted_once_in_a_session() {
    let turn = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:00Z"}},
            "content": {"role": "user", "content": "the question"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:01Z"}},
            "content": {"role": "assistant", "content": "the answer"}
        }
    ]);
    let second = json!([
        {
            "source": {"event": {"name": "gen_ai.user.message", "time": "2025-01-01T00:00:10Z"}},
            "content": {"role": "user", "content": "a second question"}
        },
        {
            "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:11Z"}},
            "content": {"role": "assistant", "content": "a second answer"}
        }
    ]);

    let mut a = make_span_row("trace1", "span1", None, &turn.to_string(), "[]", "[]");
    a.session_id = Some("session-1".to_string());
    // The same span, delivered again: identical trace and span id, as a retry produces.
    let redelivered = a.clone();
    let mut b = make_span_row("trace2", "span2", None, &second.to_string(), "[]", "[]");
    b.session_id = Some("session-1".to_string());
    b.span_timestamp = a.span_timestamp + chrono::Duration::seconds(10);
    let per_span_tokens = a.total_tokens;
    let per_span_cost = a.cost_total;

    let result = process_spans(vec![a, redelivered, b], &FeedOptions::new());

    assert_eq!(
        result.metadata.total_tokens,
        per_span_tokens * 2,
        "the retried delivery was billed a second time"
    );
    assert!(
        (result.metadata.total_cost - per_span_cost * 2.0).abs() < f64::EPSILON,
        "the retried delivery was charged twice: {}",
        result.metadata.total_cost
    );
    assert_eq!(
        result.metadata.span_count, 2,
        "one span delivered twice is still one span"
    );
}

/// Two identical calls in one response are two calls.
///
/// A tool call's identity ignores the provider's call id, because history re-sends regenerate ids and
/// the same call would otherwise appear twice. The cost was that a model asking for the same thing
/// twice in one response came back as one call - `crewai/mcp_tools` really does retry an identical
/// MCP call after a validation error, and the feed showed one call, one error, and an apology with
/// nothing to explain it. Within one response the ids are unambiguous, so each call takes the rank of
/// its id and that rank joins its identity.
#[test]
fn two_identical_calls_in_one_response_both_survive() {
    let t = fixed_time();
    let messages = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t.to_rfc3339()}},
        "content": {"role": "assistant", "content": [
            {"type": "tool_use", "id": "call_1", "name": "generate_image", "input": {"prompt": "a cat"}},
            {"type": "tool_use", "id": "call_2", "name": "generate_image", "input": {"prompt": "a cat"}}
        ]}
    }]);
    let row = make_span_row("trace1", "span1", None, &messages.to_string(), "[]", "[]");
    let result = process_spans(vec![row], &FeedOptions::new());
    let ids: Vec<&str> = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_use")
        .filter_map(|b| b.tool_use_id.as_deref())
        .collect();
    assert_eq!(
        ids,
        vec!["call_1", "call_2"],
        "a response asking for the same thing twice must show both calls"
    );
}

/// ...and a history re-send of that pair is still one pair, whatever its ids became.
///
/// This is the reason ids are not simply part of the identity: a framework re-sending its history
/// regenerates them, and keying on the id would show every past call again on every turn. The rank is
/// per response, so the re-sent pair ranks 0 and 1 again and collapses onto the original pair.
#[test]
fn a_resent_pair_of_identical_calls_is_still_one_pair() {
    let t = fixed_time();
    let call = |id: &str| json!({"type": "tool_use", "id": id, "name": "generate_image", "input": {"prompt": "a cat"}});
    // The generation span emits the pair; a later span re-sends it as history with new ids.
    let produced = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t.to_rfc3339()}},
        "content": {"role": "assistant", "content": [call("call_1"), call("call_2")]}
    }]);
    let resent = json!([{
        "source": {"event": {"name": "gen_ai.assistant.message", "time": (t + chrono::Duration::seconds(5)).to_rfc3339()}},
        "content": {"role": "assistant", "content": [call("regenerated_9"), call("regenerated_10")]}
    }]);

    let first = make_span_row("trace1", "span1", None, &produced.to_string(), "[]", "[]");
    let mut second = make_span_row("trace1", "span2", None, &resent.to_string(), "[]", "[]");
    second.span_timestamp = first.span_timestamp + chrono::Duration::seconds(5);

    let result = process_spans(vec![first, second], &FeedOptions::new());
    let calls = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_use")
        .count();
    assert_eq!(
        calls,
        2,
        "the re-sent pair must collapse onto the original pair, not add to it: {:?}",
        result
            .messages
            .iter()
            .filter(|b| b.entry_type == "tool_use")
            .map(|b| b.tool_use_id.as_deref())
            .collect::<Vec<_>>()
    );
}

/// Two identical calls with *no* ids are still two calls.
///
/// This is what the position buys over the id-based rank it replaced: a framework that reports tool
/// calls without ids gave the rank nothing to work with, so a model asking for the same thing twice
/// came back as one call. The position is structure the payload stated, so it distinguishes them
/// whether or not ids were sent.
#[test]
fn two_identical_idless_calls_in_one_response_both_survive() {
    let t = fixed_time();
    let messages = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t.to_rfc3339()}},
        "content": {"role": "assistant", "content": [
            {"type": "tool_use", "name": "generate_image", "input": {"prompt": "a cat"}},
            {"type": "tool_use", "name": "generate_image", "input": {"prompt": "a cat"}}
        ]}
    }]);
    let row = make_span_row("trace1", "span1", None, &messages.to_string(), "[]", "[]");
    let result = process_spans(vec![row], &FeedOptions::new());
    let calls = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_use")
        .count();
    assert_eq!(
        calls, 2,
        "both id-less calls must survive: their positions differ even though nothing else does"
    );
}

/// A block records the route it was read by, and blocks of one payload never share a position.
///
/// The property everything else rests on. `gen_ai.input.messages` is an expandable array source, so
/// each entry's position names the array and its index - which is what tells two entries apart when
/// their content does not.
#[test]
fn every_block_carries_a_distinct_position_within_its_payload() {
    let t = fixed_time();
    let messages = json!([{
        "source": {"attribute": {"key": "gen_ai.input.messages", "time": t.to_rfc3339()}},
        "content": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "first question"},
            {"role": "user", "content": "second question"}
        ]
    }]);
    let row = make_span_row("trace1", "span1", None, &messages.to_string(), "[]", "[]");
    let result = process_spans(vec![row], &FeedOptions::new());

    let positions: Vec<String> = result
        .messages
        .iter()
        .map(|b| b.position.to_string())
        .collect();
    assert_eq!(
        positions.len(),
        3,
        "the three entries must survive: {positions:?}"
    );
    let mut unique = positions.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        positions.len(),
        "two blocks of one payload share a position: {positions:?}"
    );
    // Route: observation 0 of the stored list, entry N of its array, content block 0.
    assert_eq!(
        unique,
        vec![
            "0.0.0".to_string(),
            "0.1.0".to_string(),
            "0.2.0".to_string()
        ],
        "a position must name the route it was read by: {positions:?}"
    );
}

/// A framework re-listing its own messages in one payload is not two calls.
///
/// LangChain's `output.value` carries accumulated state: the same tool call appears twice in one
/// attribute, at different positions, describing one call. Position alone therefore cannot decide a
/// repeat - ranking by it turned every such echo into a second call. The provider's id is the
/// evidence when there is one, and position is the fallback for payloads that carry no ids.
#[test]
fn an_echoed_call_within_one_payload_is_one_call() {
    let t = fixed_time();
    let call = json!({
        "type": "tool_use", "id": "tooluse_same", "name": "generate_image",
        "input": {"prompt": "a cat"}
    });
    // One carrier, the same call listed twice - accumulated state, not a repeat.
    let messages = json!([{
        "source": {"attribute": {"key": "output.value", "time": t.to_rfc3339()}},
        "content": {"role": "assistant", "content": [call.clone(), call]}
    }]);
    let row = make_span_row("trace1", "span1", None, &messages.to_string(), "[]", "[]");
    let result = process_spans(vec![row], &FeedOptions::new());
    let calls = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_use")
        .count();
    assert_eq!(
        calls, 1,
        "the same id twice in one payload is one call, whatever its positions"
    );
}

/// With no ids, the *carrier* decides whether two identical calls are two calls.
///
/// The pair in `two_identical_idless_calls_in_one_response_both_survive` arrives in a
/// `gen_ai.choice` event - one emission, so two positions are two calls. The same pair in
/// `output.value` is accumulated framework state, which re-lists what it already said, so two
/// positions there describe one call. Nothing but the carrier distinguishes these two tests, which is
/// the point: the judgement is declared per carrier in `sideml::carrier` rather than guessed from
/// content.
#[test]
fn an_idless_echo_in_accumulated_state_is_one_call() {
    let t = fixed_time();
    let call = json!({"type": "tool_use", "name": "generate_image", "input": {"prompt": "a cat"}});
    let messages = json!([{
        "source": {"attribute": {"key": "output.value", "time": t.to_rfc3339()}},
        "content": {"role": "assistant", "content": [call.clone(), call]}
    }]);
    let row = make_span_row("trace1", "span1", None, &messages.to_string(), "[]", "[]");
    let result = process_spans(vec![row], &FeedOptions::new());
    let calls = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_use")
        .count();
    assert_eq!(
        calls, 1,
        "accumulated state re-lists itself, so two id-less positions there are one call"
    );
}

/// The number a block *reports* must not be the number it *sorts* by.
///
/// The two fields hold the same value, so no view can tell them apart today - which is exactly why a
/// later edit could quietly go back to reading `timestamp` and nothing would fail. Here the display
/// timestamps are rewritten into the reverse of the sort order, and the answer has to be unmoved.
/// The whole point of separating them is that the anchor can change without the reported time
/// following, and that only holds while the ordering path reads `order_time` alone.
#[test]
fn the_displayed_time_does_not_decide_the_order() {
    let first = fixed_time();
    let second = first + chrono::Duration::seconds(30);
    let turn = |question: &str, answer: &str, t: chrono::DateTime<Utc>| {
        json!([
            {"source": {"event": {"name": "gen_ai.user.message", "time": t.to_rfc3339()}},
             "content": {"role": "user", "content": question}},
            {"source": {"event": {"name": "gen_ai.choice", "time": t.to_rfc3339()}},
             "content": {"role": "assistant", "content": answer}},
        ])
        .to_string()
    };
    let rows = vec![
        make_span_row_with_timestamps(
            "trace1",
            "span1",
            None,
            &turn("first question", "first answer", first),
            first,
            Some(first + chrono::Duration::seconds(1)),
        ),
        make_span_row_with_timestamps(
            "trace1",
            "span2",
            None,
            &turn("second question", "second answer", second),
            second,
            Some(second + chrono::Duration::seconds(1)),
        ),
    ];

    let blocks = process_spans(rows, &FeedOptions::new()).messages;
    assert!(
        blocks.len() >= 4,
        "need several blocks across two responses for an order to be observable, got {}",
        blocks.len()
    );
    let texts = |blocks: &[BlockEntry]| -> Vec<String> {
        blocks.iter().map(|b| b.content_hash.clone()).collect()
    };
    let expected = texts(&sort_feed_newest_first(blocks.clone()));

    // Ascending over a newest-first answer, so a sort that read them would hand back the reverse -
    // and spread far enough apart that no tie-break could mask it.
    let mut misreported = sort_feed_newest_first(blocks);
    for (i, block) in misreported.iter_mut().enumerate() {
        block.timestamp = first + chrono::Duration::hours(i as i64);
    }
    assert_eq!(
        texts(&sort_feed_newest_first(misreported)),
        expected,
        "the feed's order came from the displayed timestamp - the two fields are conflated again"
    );
}

// ----------------------------------------------------------------------------
// Test: a replay in a different valid order is still a replay
// ----------------------------------------------------------------------------

/// The provider's serialisation of parallel tool calls is a *different linearisation* of one turn.
///
/// A model that calls two tools at once emits them together, and the results come back as they come:
/// `call1, call2, result1, result2`. A conversation history is a flat message list, so the next turn
/// re-sends that turn as `call1, result1, call2, result2` - every call immediately followed by its own
/// result. Both orders satisfy the same constraints (each result after its own call; the two pairs
/// unordered against each other), so the second is not new content, it is the same turn written down
/// differently.
///
/// Matching that against a stored sequence with one forward cursor fails at the second call - it sits
/// *behind* the cursor, which already passed it to reach `result1` - and since a mismatch ends the
/// prefix, that call, its result and everything after leak into the session as duplicates. Matching
/// against the relation accepts it, because a call and the other call's result are incomparable.
#[test]
fn a_replay_that_reorders_incomparable_blocks_is_still_stripped() {
    let t0 = fixed_time();
    let t1 = t0 + chrono::Duration::seconds(30);
    let t2 = t0 + chrono::Duration::seconds(60);

    // Turn 1, first call: the model emits both tool calls in one response.
    let span1 = json!([
        {"source": {"attribute": {"key": "llm.input_messages", "time": t0.to_rfc3339()}}, "content": {"role": "user", "content": "Weather in NYC and LA?"}},
        {"source": {"event": {"name": "gen_ai.choice", "time": t0.to_rfc3339()}},
         "content": {"role": "assistant", "content": [{"type": "tool_use", "id": "call_a", "name": "weather", "input": {"city": "NYC"}}, {"type": "tool_use", "id": "call_b", "name": "weather", "input": {"city": "LA"}}], "finish_reason": "tool_use"}}
    ]);

    // Turn 1, second call: the results come back, in the order they completed.
    let span2 = json!([
        {"source": {"attribute": {"key": "llm.input_messages", "time": t1.to_rfc3339()}}, "content": {"role": "user", "content": "Weather in NYC and LA?"}},
        {"source": {"attribute": {"key": "llm.input_messages", "time": t1.to_rfc3339()}}, "content": {"role": "assistant", "content": [{"type": "tool_use", "id": "call_a", "name": "weather", "input": {"city": "NYC"}}, {"type": "tool_use", "id": "call_b", "name": "weather", "input": {"city": "LA"}}]}},
        {"source": {"attribute": {"key": "llm.input_messages", "time": t1.to_rfc3339()}}, "content": {"role": "tool", "tool_call_id": "call_a", "content": "NYC is sunny"}},
        {"source": {"attribute": {"key": "llm.input_messages", "time": t1.to_rfc3339()}}, "content": {"role": "tool", "tool_call_id": "call_b", "content": "LA is warm"}},
        {"source": {"event": {"name": "gen_ai.choice", "time": t1.to_rfc3339()}},
         "content": {"role": "assistant", "content": "NYC is sunny and LA is warm"}}
    ]);

    // Turn 2 replays turn 1 the way a provider writes a history down: each call beside its own result.
    let span3 = json!([
        {"source": {"attribute": {"key": "llm.input_messages", "time": t2.to_rfc3339()}}, "content": {"role": "user", "content": "Weather in NYC and LA?"}},
        {"source": {"attribute": {"key": "llm.input_messages", "time": t2.to_rfc3339()}}, "content": {"role": "assistant", "content": [{"type": "tool_use", "id": "call_a", "name": "weather", "input": {"city": "NYC"}}]}},
        {"source": {"attribute": {"key": "llm.input_messages", "time": t2.to_rfc3339()}}, "content": {"role": "tool", "tool_call_id": "call_a", "content": "NYC is sunny"}},
        {"source": {"attribute": {"key": "llm.input_messages", "time": t2.to_rfc3339()}}, "content": {"role": "assistant", "content": [{"type": "tool_use", "id": "call_b", "name": "weather", "input": {"city": "LA"}}]}},
        {"source": {"attribute": {"key": "llm.input_messages", "time": t2.to_rfc3339()}}, "content": {"role": "tool", "tool_call_id": "call_b", "content": "LA is warm"}},
        {"source": {"attribute": {"key": "llm.input_messages", "time": t2.to_rfc3339()}}, "content": {"role": "assistant", "content": "NYC is sunny and LA is warm"}},
        {"source": {"attribute": {"key": "llm.input_messages", "time": t2.to_rfc3339()}}, "content": {"role": "user", "content": "And tomorrow?"}},
        {"source": {"event": {"name": "gen_ai.choice", "time": t2.to_rfc3339()}},
         "content": {"role": "assistant", "content": "Tomorrow looks similar"}}
    ]);

    let rows = vec![
        make_span_row_full(
            "trace1",
            "s1",
            None,
            &span1.to_string(),
            t0,
            Some(t0),
            Some("generation"),
        ),
        make_span_row_full(
            "trace1",
            "s2",
            None,
            &span2.to_string(),
            t1,
            Some(t1),
            Some("generation"),
        ),
        make_span_row_full(
            "trace2",
            "s3",
            None,
            &span3.to_string(),
            t2,
            Some(t2),
            Some("generation"),
        ),
    ];

    let result = process_spans(rows, &FeedOptions::default());
    let shape: Vec<(crate::domain::sideml::types::ChatRole, &str)> = result
        .messages
        .iter()
        .map(|b| (b.role, b.entry_type.as_str()))
        .collect();

    let calls: Vec<&str> = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_use")
        .filter_map(|b| b.tool_use_id.as_deref())
        .collect();
    assert_eq!(
        calls.len(),
        2,
        "each call once, not once per order it was written in: {:?}",
        shape
    );
    let results = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_result")
        .count();
    assert_eq!(results, 2, "and each result once: {:?}", shape);
    assert_eq!(
        result.messages.len(),
        8,
        "the question, two calls, two results, the first answer, the new question, the second: {:?}",
        shape
    );
}

// ----------------------------------------------------------------------------
// The replay matcher, against shapes no captured fixture contains
// ----------------------------------------------------------------------------

/// Build prior state from `(role, hash)` identities and a relation over them, as one trace.
#[cfg(test)]
fn prior_state(
    identities: &[(ChatRole, &str)],
    edges: &[(usize, usize)],
) -> super::CrossTracePrefixState {
    let t0 = fixed_time();
    let transcript: Vec<BlockEntry> = identities
        .iter()
        .enumerate()
        .map(|(i, &(role, hash))| BlockEntry {
            position: PositionPath::default(),
            entry_type: "text".to_string(),
            content: ContentBlock::Text {
                text: hash.to_string(),
            },
            role,
            trace_id: "trace1".to_string(),
            span_id: "s1".to_string(),
            session_id: None,
            message_index: i as i32,
            entry_index: 0,
            parent_span_id: None,
            span_path: vec!["s1".to_string()],
            timestamp: t0,
            order_time: t0,
            observation_type: Some("generation".to_string()),
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
            source_attribute: Some("llm.input_messages".to_string()),
            category: crate::data::types::MessageCategory::GenAIUserMessage,
            content_hash: hash.to_string(),
            is_semantic: true,
            uses_span_end: false,
            is_history: false,
            tool_use_id_correlated: false,
            promoted_to_span_output: false,
        })
        .collect();
    let mut state = super::CrossTracePrefixState::default();
    state.push_trace(
        &transcript,
        super::order_graph::Precedence::from_edges(identities.len(), edges),
    );
    state
}

/// Two interchangeable results, and the choice between them decides whether the rest strips.
///
/// The shape Codex found in the greedy matcher, and the reason the matcher searches. Two unordered
/// branches give `callA -> resultA` and `callB -> resultB`, and both results carry the *same* identity -
/// two tools that each answered `"ok"`, which is ordinary. Replayed as `callB, resultB, callA, resultA`,
/// a valid linear extension, taking the first permitted candidate for `resultB` claims `resultA`; that
/// assignment then requires `callA` to have come earlier, so `callA` is refused and everything after it
/// is duplicated in the session view. Only the order constraints distinguish the two choices.
#[test]
fn interchangeable_results_do_not_end_the_prefix() {
    // 0: callA  1: resultA  2: callB  3: resultB, with each result after its own call and the two
    // branches unordered against each other.
    let prior = prior_state(
        &[
            (ChatRole::Assistant, "callA"),
            (ChatRole::Tool, "ok"),
            (ChatRole::Assistant, "callB"),
            (ChatRole::Tool, "ok"),
        ],
        &[(0, 1), (2, 3)],
    );

    let replay = [
        (ChatRole::Assistant, "callB"),
        (ChatRole::Tool, "ok"),
        (ChatRole::Assistant, "callA"),
        (ChatRole::Tool, "ok"),
    ];
    let (matched, _) = prior.longest_matching_prefix(&replay);
    assert_eq!(
        matched.len(),
        replay.len(),
        "the whole replay is a linear extension of the prior order, so all of it is history"
    );
    let mut consumed = matched.clone();
    consumed.sort_unstable();
    consumed.dedup();
    assert_eq!(
        consumed.len(),
        matched.len(),
        "and each block claimed a distinct occurrence"
    );
}

/// A replay that contradicts the order is *not* history, however identical its content.
///
/// The other side of the property: without this the matcher could strip anything that merely looks alike,
/// which would delete real messages rather than duplicate them.
#[test]
fn a_replay_that_contradicts_the_order_is_not_stripped() {
    // A tool result cannot precede the call it answers.
    let prior = prior_state(
        &[(ChatRole::Assistant, "call"), (ChatRole::Tool, "ok")],
        &[(0, 1)],
    );
    let (matched, _) =
        prior.longest_matching_prefix(&[(ChatRole::Tool, "ok"), (ChatRole::Assistant, "call")]);
    assert_eq!(
        matched.len(),
        1,
        "the result matches, and the call after it contradicts the evidence, so the prefix ends"
    );
}

/// Every linear extension of every relation over four blocks is recognised as a replay of it.
///
/// The completeness property, over *all* shapes at this size rather than over chosen examples. Enumerated
/// exhaustively: every one of the 64 edge sets over four blocks (each a DAG by construction, since only
/// forward pairs are offered), against every assignment of the blocks to a two-symbol identity alphabet
/// (16 of them), against every one of the 24 orders - keeping the orders that satisfy the constraints and
/// requiring each to match in full and injectively.
///
/// Identities repeat by design: interchangeable candidates are what make the choice non-obvious, and
/// both defects found here were about them. Choosing greedily among them fails Codex's four-block
/// counterexample; choosing in stored order fails his ten-branch one, which
/// `ten_interchangeable_branches_replayed_in_reverse_are_fully_stripped` covers at a size this
/// enumeration cannot reach.
#[test]
fn every_linear_extension_of_every_small_relation_is_fully_stripped() {
    const BLOCKS: usize = 4;
    // The pairs an acyclic relation may contain when nodes are numbered in topological order.
    let forward_pairs: Vec<(usize, usize)> = (0..BLOCKS)
        .flat_map(|a| ((a + 1)..BLOCKS).map(move |b| (a, b)))
        .collect();

    fn permutations(n: usize) -> Vec<Vec<usize>> {
        let mut out = Vec::new();
        let mut current: Vec<usize> = (0..n).collect();
        fn go(current: &mut Vec<usize>, k: usize, out: &mut Vec<Vec<usize>>) {
            if k == current.len() {
                out.push(current.clone());
                return;
            }
            for i in k..current.len() {
                current.swap(k, i);
                go(current, k + 1, out);
                current.swap(k, i);
            }
        }
        go(&mut current, 0, &mut out);
        out
    }
    let orders = permutations(BLOCKS);

    let mut shapes = 0;
    let mut extensions = 0;
    for edge_mask in 0u32..(1 << forward_pairs.len()) {
        let edges: Vec<(usize, usize)> = forward_pairs
            .iter()
            .enumerate()
            .filter(|(i, _)| edge_mask & (1 << i) != 0)
            .map(|(_, &pair)| pair)
            .collect();

        for identity_mask in 0u32..(1 << BLOCKS) {
            // Two symbols, so blocks collide in identity as soon as the mask repeats a bit.
            let identities: Vec<(ChatRole, &str)> = (0..BLOCKS)
                .map(|i| {
                    if identity_mask & (1 << i) == 0 {
                        (ChatRole::Tool, "ok")
                    } else {
                        (ChatRole::Assistant, "call")
                    }
                })
                .collect();
            let prior = prior_state(&identities, &edges);
            shapes += 1;

            for order in &orders {
                let position: Vec<usize> = {
                    let mut p = vec![0; order.len()];
                    for (at, &block) in order.iter().enumerate() {
                        p[block] = at;
                    }
                    p
                };
                // Only a linear extension is a replay of this turn; anything else contradicts it.
                if edges.iter().any(|&(a, b)| position[a] > position[b]) {
                    continue;
                }
                extensions += 1;

                let replay: Vec<(ChatRole, &str)> = order.iter().map(|&i| identities[i]).collect();
                let (matched, _) = prior.longest_matching_prefix(&replay);
                assert_eq!(
                    matched.len(),
                    replay.len(),
                    "order {order:?} satisfies every constraint in {edges:?} with identities \
                     {identity_mask:04b}, so it is a replay - matched {} of {}",
                    matched.len(),
                    replay.len()
                );
                let mut distinct = matched.clone();
                distinct.sort_unstable();
                distinct.dedup();
                assert_eq!(distinct.len(), matched.len(), "matched injectively");
            }
        }
    }
    assert_eq!(
        shapes,
        64 * 16,
        "every relation and identity assignment was built"
    );
    assert!(
        extensions > 5_000,
        "only {extensions} linear extensions were checked, which is too few to be exhaustive"
    );
}

/// Ten interchangeable results, replayed in reverse: the shape that exhausts a naive search.
///
/// Codex's second counterexample, and it is about the *budget* rather than about the rule. Ten
/// independent branches `call_i -> result_i` where every result carries the same identity - ten tools
/// that each answered `"ok"` - replayed branch by branch in reverse order. Every step then offers ten
/// permitted candidates that differ only in which call they answer, so a search that tries them in
/// stored order picks wrong nine times out of ten and spends its whole budget backtracking; it gives up
/// part way and the tail of the old turn is duplicated in the session view.
///
/// The fix is the order of exploration: try the candidate with the fewest unmatched ancestors first,
/// which is the one whose call the replay has just matched. Nothing about what is *permitted* changes,
/// so a shape the heuristic guesses wrong is still found by backtracking.
#[test]
fn ten_interchangeable_branches_replayed_in_reverse_are_fully_stripped() {
    const BRANCHES: usize = 10;

    // 2i: call_i (unique identity), 2i+1: result_i (all identical), with each result after its own call.
    let mut identities: Vec<(ChatRole, String)> = Vec::new();
    let mut edges: Vec<(usize, usize)> = Vec::new();
    for i in 0..BRANCHES {
        identities.push((ChatRole::Assistant, format!("call{i}")));
        identities.push((ChatRole::Tool, "ok".to_string()));
        edges.push((2 * i, 2 * i + 1));
    }
    let borrowed: Vec<(ChatRole, &str)> = identities
        .iter()
        .map(|(role, hash)| (*role, hash.as_str()))
        .collect();
    let prior = prior_state(&borrowed, &edges);

    // Reverse by branch, each call still before its own result: a valid linear extension.
    let mut replay: Vec<(ChatRole, &str)> = Vec::new();
    for i in (0..BRANCHES).rev() {
        replay.push(borrowed[2 * i]);
        replay.push(borrowed[2 * i + 1]);
    }

    let (matched, _) = prior.longest_matching_prefix(&replay);
    assert_eq!(
        matched.len(),
        replay.len(),
        "the whole replay is a linear extension, so all {} blocks are history; matched {}",
        replay.len(),
        matched.len()
    );
    let mut distinct = matched.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), matched.len(), "and injectively");
}

/// Nine identical calls with distinct results, replayed in reverse: the budget's counterexample.
///
/// Codex's second shape, and it is the mirror of the first. A tool call's identity deliberately excludes
/// the provider's call id, so nine calls of the same tool with the same input are *one* identity - which
/// is what a model retrying the same call produces. Their results differ. Replayed branch by branch in
/// reverse, every step offers nine permitted candidates for the call, and "fewest unmatched ancestors"
/// cannot separate them: a call has no ancestors at all, so they tie.
///
/// The disambiguation has to come from the blocks whose identity is *unique* - the results - which is why
/// the matcher matches in order of ambiguity rather than in replay order.
#[test]
fn nine_identical_calls_with_distinct_results_are_fully_stripped() {
    const BRANCHES: usize = 9;

    let mut identities: Vec<(ChatRole, String)> = Vec::new();
    let mut edges: Vec<(usize, usize)> = Vec::new();
    // All calls first, then all results, as a transcript records them - and `call_i -> result_i`.
    for _ in 0..BRANCHES {
        identities.push((ChatRole::Assistant, "call".to_string()));
    }
    for i in 0..BRANCHES {
        identities.push((ChatRole::Tool, format!("result{i}")));
        edges.push((i, BRANCHES + i));
    }
    let borrowed: Vec<(ChatRole, &str)> = identities
        .iter()
        .map(|(role, hash)| (*role, hash.as_str()))
        .collect();
    let prior = prior_state(&borrowed, &edges);

    let mut replay: Vec<(ChatRole, &str)> = Vec::new();
    for i in (0..BRANCHES).rev() {
        replay.push(borrowed[i]);
        replay.push(borrowed[BRANCHES + i]);
    }

    let (matched, _) = prior.longest_matching_prefix(&replay);
    assert_eq!(
        matched.len(),
        replay.len(),
        "every constraint is satisfied, so all {} blocks are history; matched {}",
        replay.len(),
        matched.len()
    );
}

/// How far the bounded search reaches on the three-level shape, reported rather than assumed.
///
/// Codex's harder construction: identical roots, identical middles, unique leaves, `root_i -> middle_i ->
/// leaf_i`, replayed branch by branch in reverse. Two levels of interchangeable blocks rather than one.
#[test]
#[ignore]
fn probe_matcher_envelope_three_level() {
    for branches in [4usize, 6, 7, 8, 10, 12, 16] {
        let mut identities: Vec<(ChatRole, String)> = Vec::new();
        let mut edges: Vec<(usize, usize)> = Vec::new();
        for _ in 0..branches {
            identities.push((ChatRole::Assistant, "root".to_string()));
        }
        for _ in 0..branches {
            identities.push((ChatRole::Assistant, "middle".to_string()));
        }
        for i in 0..branches {
            identities.push((ChatRole::Tool, format!("leaf{i}")));
            edges.push((i, branches + i));
            edges.push((branches + i, 2 * branches + i));
        }
        let borrowed: Vec<(ChatRole, &str)> = identities
            .iter()
            .map(|(role, hash)| (*role, hash.as_str()))
            .collect();
        let prior = prior_state(&borrowed, &edges);
        let mut replay: Vec<(ChatRole, &str)> = Vec::new();
        for i in (0..branches).rev() {
            replay.push(borrowed[i]);
            replay.push(borrowed[branches + i]);
            replay.push(borrowed[2 * branches + i]);
        }
        let start = std::time::Instant::now();
        let (matched, _) = prior.longest_matching_prefix(&replay);
        eprintln!(
            "THREE-LEVEL {branches:3} branches ({} blocks): matched {} in {:?}",
            replay.len(),
            matched.len(),
            start.elapsed()
        );
    }
}

/// How far the bounded search actually reaches, reported rather than assumed.
#[test]
#[ignore]
fn probe_matcher_envelope() {
    for branches in [9usize, 16, 24, 32, 48, 64] {
        let mut identities: Vec<(ChatRole, String)> = Vec::new();
        let mut edges: Vec<(usize, usize)> = Vec::new();
        for _ in 0..branches {
            identities.push((ChatRole::Assistant, "call".to_string()));
        }
        for i in 0..branches {
            identities.push((ChatRole::Tool, format!("result{i}")));
            edges.push((i, branches + i));
        }
        let borrowed: Vec<(ChatRole, &str)> = identities
            .iter()
            .map(|(role, hash)| (*role, hash.as_str()))
            .collect();
        let prior = prior_state(&borrowed, &edges);
        let mut replay: Vec<(ChatRole, &str)> = Vec::new();
        for i in (0..branches).rev() {
            replay.push(borrowed[i]);
            replay.push(borrowed[branches + i]);
        }
        let start = std::time::Instant::now();
        let (matched, _) = prior.longest_matching_prefix(&replay);
        eprintln!(
            "ENVELOPE {branches:3} branches ({} blocks): matched {} in {:?}",
            replay.len(),
            matched.len(),
            start.elapsed()
        );
    }
}

/// When the search is cut short, the answer says so.
///
/// The budget is a resource guard, and a guard that silently changes the answer is the thing a caller
/// cannot reason about. So `longest_matching_prefix` reports whether it was exhaustive, and that travels to
/// `FeedMetadata::replay_matching_complete`: either the stripping is complete, or the response says it may
/// repeat history. Under-stripping is the safe direction - duplicated history rather than missing messages
/// - but only if the caller is told.
#[test]
fn an_incomplete_search_is_reported_as_incomplete() {
    // Three levels of interchangeable blocks, wide enough to exhaust the budget: identical roots,
    // identical middles, unique leaves, replayed branch by branch in reverse.
    const BRANCHES: usize = 12;
    let mut identities: Vec<(ChatRole, String)> = Vec::new();
    let mut edges: Vec<(usize, usize)> = Vec::new();
    for _ in 0..BRANCHES {
        identities.push((ChatRole::Assistant, "root".to_string()));
    }
    for _ in 0..BRANCHES {
        identities.push((ChatRole::Assistant, "middle".to_string()));
    }
    for i in 0..BRANCHES {
        identities.push((ChatRole::Tool, format!("leaf{i}")));
        edges.push((i, BRANCHES + i));
        edges.push((BRANCHES + i, 2 * BRANCHES + i));
    }
    let borrowed: Vec<(ChatRole, &str)> = identities
        .iter()
        .map(|(role, hash)| (*role, hash.as_str()))
        .collect();
    let prior = prior_state(&borrowed, &edges);
    let mut replay: Vec<(ChatRole, &str)> = Vec::new();
    for i in (0..BRANCHES).rev() {
        replay.push(borrowed[i]);
        replay.push(borrowed[BRANCHES + i]);
        replay.push(borrowed[2 * BRANCHES + i]);
    }

    let (matched, exhaustive) = prior.longest_matching_prefix(&replay);
    assert!(
        !exhaustive,
        "this shape is meant to exhaust the budget; if it no longer does, widen it rather than delete \
         the test - the point is that the flag is reachable"
    );
    assert!(
        matched.len() < replay.len(),
        "and an exhausted search is exactly when the answer is short"
    );
    assert!(
        !matched.is_empty(),
        "but it still strips what it found, rather than giving up entirely"
    );
}

/// **Known limit**, asserted as it behaves: two spans starting at the same instant are ordered by their
/// span ids, and an id-less tool result whose call lands on the far side of that tie stays uncorrelated.
///
/// Between spans, document order is `(timestamp_start, trace_id, span_id)`, and correlation is a single
/// forward pass over it - so when a tool span and the generation span that called it share a start time
/// (ordinary with a millisecond clock) the pairing depends on two random bytes. With the ids the other way
/// round the same telemetry correlates, which the second half of this test shows.
///
/// The obvious repair - letting a result also claim a *following* call in a span that starts at the same
/// instant - was implemented and reverted. It changed `adk/tool_use` for the worse in every variant tried
/// (an equal alternative to the preceding rule, and a fallback used only when no preceding call exists):
/// ADK's tool and generation spans do tie, so the relaxation lets one result claim a call that a later
/// result needed, and results that *had* ids lost them - three of them, with their order changing to put
/// the results before their calls. Rules 3 and 4 have nothing but document order to go on, and relaxing
/// them where that order is arbitrary trades a rare mis-order for a common mis-pairing.
///
/// What a real fix needs is causal evidence that does not come from the span id: the ordering redesign's
/// partial order (`order_graph`), where a call→result edge is a constraint rather than a position. Recorded
/// here so the next attempt starts from the measurement rather than the idea.
#[test]
fn an_idless_result_is_correlated_only_when_span_ids_order_its_call_first() {
    let t = fixed_time();
    let call = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": "2025-01-01T00:00:00Z"}},
        "content": {
            "role": "assistant",
            "content": [{"type": "tool_use", "id": "call-1", "name": "get_weather",
                         "input": {"city": "Paris"}}]
        }
    }]);
    let result = json!([{
        "source": {"event": {"name": "gen_ai.tool.message", "time": "2025-01-01T00:00:00Z"}},
        "content": {
            "role": "tool",
            "content": [{"type": "tool_result", "name": "get_weather", "content": "sunny"}]
        }
    }]);

    let feed_for = |tool_span: &str, gen_span: &str| {
        let mut rows = vec![
            make_span_row_full(
                "t1",
                tool_span,
                None,
                &result.to_string(),
                t,
                Some(t),
                Some("tool"),
            ),
            make_span_row_full(
                "t1",
                gen_span,
                None,
                &call.to_string(),
                t,
                Some(t),
                Some("generation"),
            ),
        ];
        // As the query delivers them: `ORDER BY timestamp_start, trace_id, span_id`. With the starts equal
        // the span id is the whole order, which is the point of this test.
        rows.sort_by(|a, b| a.span_id.cmp(&b.span_id));
        process_spans(rows, &FeedOptions::default())
            .messages
            .iter()
            .map(|b| (b.entry_type.clone(), b.tool_use_id.clone()))
            .collect::<Vec<_>>()
    };

    // The call's span sorts first: correlated, and the result follows its call.
    assert_eq!(
        feed_for("b-tool", "a-gen"),
        vec![
            ("tool_use".to_string(), Some("call-1".to_string())),
            ("tool_result".to_string(), Some("call-1".to_string())),
        ],
        "when document order puts the call first, the result answers it"
    );

    // The result's span sorts first: uncorrelated, and it precedes the call it answers. This is the limit.
    assert_eq!(
        feed_for("a-tool", "b-gen"),
        vec![
            ("tool_result".to_string(), None),
            ("tool_use".to_string(), Some("call-1".to_string())),
        ],
        "the same telemetry, ordered by span id the other way, leaves the result uncorrelated"
    );
}

/// The feed is a projection of the resolved order, never a re-sort of it.
///
/// This is what "the resolver is the ordering authority" means for the one non-chronological view:
/// `sort_feed_newest_first` may regroup responses and reverse the groups, and it may not change the
/// order of two blocks within a response. The old implementation did - a second scalar tuple undid
/// whatever the resolver had improved, which is how `bedrock/converse` showed the answer *before* the
/// tool result it used.
///
/// Asserted as three properties of the projection, none of which needs plumbing to the pre-feed order:
/// every response's subsequence is unchanged (full fingerprint, not role/kind); each response is one
/// contiguous run; and the runs descend by `(order_time, trace_id)`, so an anchor tie between two
/// traces has one deterministic answer.
#[test]
fn the_feed_projects_the_resolved_order_without_resorting_it() {
    let t = fixed_time();
    // Two traces, interleaved anchors, one response with several blocks including a call and result -
    // enough structure that a re-sort of any term would be visible.
    let turn = |q: &str, id: &str, time: chrono::DateTime<chrono::Utc>| {
        serde_json::json!([
            {"source": {"event": {"name": "gen_ai.user.message", "time": time.to_rfc3339()}},
             "content": {"role": "user", "content": q}},
            {"source": {"event": {"name": "gen_ai.choice", "time": time.to_rfc3339()}},
             "content": {"role": "assistant", "content": [
                 {"type": "text", "text": format!("thinking about {q}")},
                 {"type": "tool_use", "id": id, "name": "look_up", "input": {"q": q}}
             ]}},
            {"source": {"event": {"name": "gen_ai.tool.message", "time": (time + chrono::Duration::seconds(1)).to_rfc3339()}},
             "content": {"role": "tool", "content": [
                 {"type": "tool_result", "tool_use_id": id, "content": format!("answer to {q}")}
             ]}},
        ])
        .to_string()
    };
    let rows = vec![
        make_span_row_with_timestamps(
            "trace-a",
            "span-a",
            None,
            &turn("alpha", "call_a", t),
            t,
            Some(t + chrono::Duration::seconds(2)),
        ),
        make_span_row_with_timestamps(
            "trace-b",
            "span-b",
            None,
            &turn("beta", "call_b", t + chrono::Duration::seconds(10)),
            t + chrono::Duration::seconds(10),
            Some(t + chrono::Duration::seconds(12)),
        ),
    ];

    let resolved = process_spans(rows, &FeedOptions::new()).messages;
    assert!(resolved.len() >= 6, "got {}", resolved.len());
    let fingerprint = |b: &BlockEntry| {
        format!(
            "{}/{}/{}/{}/{}#{}",
            b.trace_id, b.span_id, b.message_index, b.entry_index, b.entry_type, b.content_hash
        )
    };
    let response_of = |b: &BlockEntry| (b.order_time, b.trace_id.clone());

    let feed = sort_feed_newest_first(resolved.clone());

    // 1. Within each response, the subsequence is byte-for-byte the resolved one.
    let subsequence = |blocks: &[BlockEntry]| {
        let mut map: std::collections::BTreeMap<_, Vec<String>> = std::collections::BTreeMap::new();
        for b in blocks {
            map.entry(response_of(b)).or_default().push(fingerprint(b));
        }
        map
    };
    assert_eq!(
        subsequence(&resolved),
        subsequence(&feed),
        "the feed changed a response's internal order - it re-sorted instead of projecting"
    );

    // 2. Each response is one contiguous run.
    let mut seen = std::collections::HashSet::new();
    let mut current = None;
    for b in &feed {
        let key = response_of(b);
        if current.as_ref() != Some(&key) {
            assert!(
                seen.insert(key.clone()),
                "response {key:?} appears in two separate runs"
            );
            current = Some(key);
        }
    }

    // 3. Runs descend by (order_time, trace_id).
    let mut anchors: Vec<_> = Vec::new();
    for b in &feed {
        let key = response_of(b);
        if anchors.last() != Some(&key) {
            anchors.push(key);
        }
    }
    let mut sorted = anchors.clone();
    sorted.sort();
    sorted.reverse();
    assert_eq!(anchors, sorted, "responses are not newest-first");
}

/// A *single* call re-sent once with a regenerated id is still one call.
///
/// The pair form of this is pinned above; the single form is the case review 25 found unguarded, and
/// it slips past the pair's own defence. Two executions are told apart from a re-sent pair by how many
/// calls of one shape a single response lists - but a lone call re-sent once lists its shape once in
/// *each* response, so that discriminator sees two "executions", and with the provider's regenerated id
/// trusted trace-wide the echo ranked as a second call. The guard the rank scope's comment always
/// claimed - only a **non-history** call ranks trace-wide - is what this test holds in place.
#[test]
fn a_resent_single_call_with_a_regenerated_id_is_still_one_call() {
    let t = fixed_time();
    let call = |id: &str| json!({"type": "tool_use", "id": id, "name": "generate_image", "input": {"prompt": "a cat"}});
    let produced = json!([{
        "source": {"event": {"name": "gen_ai.choice", "time": t.to_rfc3339()}},
        "content": {"role": "assistant", "content": [call("call_1")]}
    }]);
    // A later span re-sends the conversation; the framework regenerated the call id.
    let resent = json!([{
        "source": {"event": {"name": "gen_ai.assistant.message", "time": (t + chrono::Duration::seconds(5)).to_rfc3339()}},
        "content": {"role": "assistant", "content": [call("regenerated_9")]}
    }]);

    let first = make_span_row("trace1", "span1", None, &produced.to_string(), "[]", "[]");
    let mut second = make_span_row("trace1", "span2", None, &resent.to_string(), "[]", "[]");
    second.span_timestamp = first.span_timestamp + chrono::Duration::seconds(5);

    let result = process_spans(vec![first, second], &FeedOptions::new());
    let calls = result
        .messages
        .iter()
        .filter(|b| b.entry_type == "tool_use")
        .count();
    assert_eq!(
        calls,
        1,
        "the re-sent call must collapse onto the original, not rank as a second execution: {:?}",
        result
            .messages
            .iter()
            .filter(|b| b.entry_type == "tool_use")
            .map(|b| b.tool_use_id.as_deref())
            .collect::<Vec<_>>()
    );
}
