import type { Block } from "@/api/otel/types";
import {
  ContentRenderer,
  TextContent,
  ToolUseContent,
  ToolResultContent,
  ThinkingContent,
  ToolDefinitionsContent,
} from "./content";

/**
 * Generate a unique key for a block.
 */
export function getBlockKey(block: Block): string {
  return `${block.span_id}-${block.message_index}-${block.entry_index}`;
}

/**
 * Get a preview string for a block.
 */
export function getBlockPreview(block: Block): string {
  const { entry_type, content } = block;

  if (entry_type === "tool_use" && content.type === "tool_use") {
    const inputStr = JSON.stringify(content.input);
    return inputStr.length > 60 ? inputStr.slice(0, 60) + "..." : inputStr;
  }

  if (entry_type === "tool_result" && content.type === "tool_result") {
    const resultStr =
      typeof content.content === "string" ? content.content : JSON.stringify(content.content);
    const firstLine = resultStr.split("\n")[0];
    return firstLine.length > 60 ? firstLine.slice(0, 60) + "..." : firstLine;
  }

  if (entry_type === "thinking" && content.type === "thinking") {
    const truncated = content.text.length > 60 ? content.text.slice(0, 60) + "..." : content.text;
    return `"${truncated}" (${content.text.length} chars)`;
  }

  if (entry_type === "tool_definitions" && content.type === "tool_definitions") {
    return `${content.tools.length} tool${content.tools.length !== 1 ? "s" : ""} defined`;
  }

  if (entry_type === "text" && content.type === "text") {
    const firstLine = content.text.split("\n")[0];
    return firstLine.length > 80 ? firstLine.slice(0, 80) + "..." : firstLine;
  }

  return `[${entry_type}]`;
}

/**
 * Get copyable text for a block.
 */
export function getBlockCopyText(block: Block): string {
  const { entry_type, content } = block;

  if (entry_type === "tool_use" && content.type === "tool_use") {
    return JSON.stringify(content.input, null, 2);
  }

  if (entry_type === "tool_result" && content.type === "tool_result") {
    return typeof content.content === "string"
      ? content.content
      : JSON.stringify(content.content, null, 2);
  }

  if (entry_type === "thinking" && content.type === "thinking") {
    return content.text;
  }

  if (entry_type === "tool_definitions" && content.type === "tool_definitions") {
    return JSON.stringify(content.tools, null, 2);
  }

  if (entry_type === "text" && content.type === "text") {
    return content.text;
  }

  return JSON.stringify(content, null, 2);
}

/**
 * Render content for a block.
 */
export function renderBlockContent(
  block: Block,
  markdownEnabled: boolean,
  projectId?: string,
): React.ReactNode {
  const { entry_type, content } = block;

  if (entry_type === "text" && content.type === "text") {
    return <TextContent text={content.text} markdownEnabled={markdownEnabled} />;
  }

  if (entry_type === "tool_use" && content.type === "tool_use") {
    return <ToolUseContent id={content.id} name={content.name} input={content.input} />;
  }

  if (entry_type === "tool_result" && content.type === "tool_result") {
    return (
      <ToolResultContent
        content={content.content}
        isError={content.is_error || block.is_error}
        toolCallId={content.tool_use_id || block.tool_use_id}
        toolCallIdInferred={block.tool_use_id_correlated}
        // The result's own name first: for Gemini and ADK it is the only identification the source
        // gave, and the block-level fields are derived rather than reported.
        toolName={content.name || block.tool_name || block.name}
        projectId={projectId}
      />
    );
  }

  if (entry_type === "thinking" && content.type === "thinking") {
    return <ThinkingContent text={content.text} markdownEnabled={markdownEnabled} />;
  }

  if (entry_type === "tool_definitions" && content.type === "tool_definitions") {
    return <ToolDefinitionsContent tools={content.tools} toolChoice={content.tool_choice} />;
  }

  // Fallback: use generic content renderer
  return (
    <ContentRenderer block={content} markdownEnabled={markdownEnabled} projectId={projectId} />
  );
}
