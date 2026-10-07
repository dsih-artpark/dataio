import { useMemo } from 'preact/hooks';

interface ToolCall {
  tool: string;
  input: Record<string, unknown>;
}

interface ChatMessageProps {
  role: 'user' | 'assistant' | 'system';
  content: string;
  toolCalls?: ToolCall[];
  isStreaming?: boolean;
}

export default function ChatMessage({ role, content, toolCalls, isStreaming }: ChatMessageProps) {
  const isUser = role === 'user';

  const formattedContent = useMemo(() => {
    // Simple markdown-like formatting
    return content
      .split('\n')
      .map((line) => {
        // Bold
        line = line.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
        // Inline code
        line = line.replace(/`([^`]+)`/g, '<code class="bg-gray-100 px-1 rounded text-sm">$1</code>');
        return line;
      })
      .join('<br />');
  }, [content]);

  return (
    <div class={`flex ${isUser ? 'justify-end' : 'justify-start'} mb-4`}>
      <div
        class={`max-w-[80%] rounded-lg px-4 py-3 ${
          isUser
            ? 'bg-primary-500 text-white'
            : 'bg-gray-100 text-gray-900'
        }`}
      >
        {/* Message content */}
        <div
          class="prose prose-sm max-w-none"
          dangerouslySetInnerHTML={{ __html: formattedContent }}
        />

        {/* Streaming indicator */}
        {isStreaming && (
          <span class="inline-block w-2 h-4 bg-current animate-pulse ml-1" />
        )}

        {/* Tool calls indicator */}
        {toolCalls && toolCalls.length > 0 && (
          <div class="mt-2 pt-2 border-t border-gray-200">
            <div class="text-xs text-gray-500 mb-1">Tools used:</div>
            <div class="flex flex-wrap gap-1">
              {toolCalls.map((tc, i) => (
                <span
                  key={i}
                  class="inline-flex items-center px-2 py-0.5 rounded text-xs bg-gray-200 text-gray-700"
                >
                  {tc.tool.replace(/_/g, ' ')}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
