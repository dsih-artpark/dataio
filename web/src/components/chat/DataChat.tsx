import { useState, useRef, useEffect } from 'preact/hooks';
import ChatMessage from './ChatMessage';
import ToolIndicator from './ToolIndicator';
import { api } from '../../lib/api';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  toolCalls?: { tool: string; input: Record<string, unknown> }[];
}

interface ActiveTool {
  tool: string;
  status: 'running' | 'complete' | 'error';
  preview?: string;
}

type AIProvider = 'bedrock' | 'openrouter';

interface DataChatProps {
  initialMessage?: string;
  provider?: AIProvider;
  showProviderSelector?: boolean;
}

export default function DataChat({
  initialMessage,
  provider: initialProvider,
  showProviderSelector = false
}: DataChatProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState(initialMessage || '');
  const [isLoading, setIsLoading] = useState(false);
  const [activeTools, setActiveTools] = useState<ActiveTool[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [selectedProvider, setSelectedProvider] = useState<AIProvider | undefined>(initialProvider);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Auto-scroll to bottom
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, activeTools]);

  // Focus input on load
  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const generateId = () => Math.random().toString(36).substring(7);

  const buildHistoryForAPI = (): { role: string; content: { text: string }[] }[] => {
    return messages.map((msg) => ({
      role: msg.role,
      content: [{ text: msg.content }],
    }));
  };

  const sendMessage = async () => {
    const trimmedInput = input.trim();
    if (!trimmedInput || isLoading) return;

    // Add user message
    const userMessage: Message = {
      id: generateId(),
      role: 'user',
      content: trimmedInput,
    };
    setMessages((prev) => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);
    setError(null);
    setActiveTools([]);

    // Create placeholder for assistant response
    const assistantId = generateId();
    setMessages((prev) => [
      ...prev,
      { id: assistantId, role: 'assistant', content: '', toolCalls: [] },
    ]);

    try {
      const accessToken = api.getAccessToken();
      if (!accessToken) {
        throw new Error('Not authenticated');
      }

      const API_URL = import.meta.env.PUBLIC_API_URL || 'http://localhost:8000/api/v1';
      const response = await fetch(`${API_URL}/web/chat/stream`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${accessToken}`,
        },
        body: JSON.stringify({
          message: trimmedInput,
          history: buildHistoryForAPI().slice(0, -1), // Exclude the message we just added
          provider: selectedProvider,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || 'Failed to send message');
      }

      const reader = response.body?.getReader();
      const decoder = new TextDecoder();
      let accumulatedText = '';
      const toolCalls: { tool: string; input: Record<string, unknown> }[] = [];

      if (!reader) {
        throw new Error('No response body');
      }

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const chunk = decoder.decode(value);
        const lines = chunk.split('\n');

        for (const line of lines) {
          if (line.startsWith('data: ')) {
            const jsonStr = line.slice(6).trim();
            if (!jsonStr) continue;

            try {
              const event = JSON.parse(jsonStr);

              switch (event.type) {
                case 'text':
                  accumulatedText += event.content;
                  setMessages((prev) => {
                    const updated = [...prev];
                    const lastIdx = updated.findIndex((m) => m.id === assistantId);
                    if (lastIdx >= 0) {
                      updated[lastIdx] = {
                        ...updated[lastIdx],
                        content: accumulatedText,
                      };
                    }
                    return updated;
                  });
                  break;

                case 'tool_use_start':
                  // Clear previous tool indicators
                  setActiveTools([]);
                  break;

                case 'tool_use':
                  setActiveTools((prev) => [
                    ...prev,
                    { tool: event.tool, status: 'running' },
                  ]);
                  toolCalls.push({ tool: event.tool, input: event.input });
                  break;

                case 'tool_result':
                  setActiveTools((prev) =>
                    prev.map((t) =>
                      t.tool === event.tool
                        ? { ...t, status: event.success ? 'complete' : 'error', preview: event.preview }
                        : t
                    )
                  );
                  break;

                case 'done':
                  // Update final message with tool calls
                  setMessages((prev) => {
                    const updated = [...prev];
                    const lastIdx = updated.findIndex((m) => m.id === assistantId);
                    if (lastIdx >= 0) {
                      updated[lastIdx] = {
                        ...updated[lastIdx],
                        toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
                      };
                    }
                    return updated;
                  });
                  setActiveTools([]);
                  break;

                case 'error':
                  setError(event.message);
                  break;
              }
            } catch {
              // Ignore JSON parse errors for incomplete chunks
            }
          }
        }
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'An error occurred';
      setError(errorMessage);

      // Remove the empty assistant message on error
      setMessages((prev) => prev.filter((m) => m.id !== assistantId));
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  const suggestedQueries = [
    'What datasets do you have about weather?',
    'Show me satellite imagery datasets',
    'List all categories',
    'Find datasets from ISRO',
  ];

  return (
    <div class="flex flex-col h-full bg-white rounded-lg shadow-sm border">
      {/* Header */}
      <div class="px-4 py-3 border-b bg-gray-50 rounded-t-lg">
        <div class="flex items-center justify-between">
          <div>
            <h2 class="font-semibold text-gray-900">Data Assistant</h2>
            <p class="text-sm text-gray-500">Ask questions about available datasets</p>
          </div>
          {showProviderSelector && (
            <div class="flex items-center gap-2">
              <label class="text-xs text-gray-500">Provider:</label>
              <select
                value={selectedProvider || ''}
                onChange={(e) => setSelectedProvider(e.currentTarget.value as AIProvider || undefined)}
                class="text-sm border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-2 focus:ring-primary-500"
                disabled={isLoading}
              >
                <option value="">Default</option>
                <option value="bedrock">AWS Bedrock</option>
                <option value="openrouter">OpenRouter</option>
              </select>
            </div>
          )}
        </div>
      </div>

      {/* Messages area */}
      <div class="flex-1 overflow-y-auto p-4 space-y-2">
        {messages.length === 0 ? (
          <div class="text-center py-8">
            <div class="text-gray-400 mb-4">
              <svg class="w-12 h-12 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={1.5}
                  d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"
                />
              </svg>
            </div>
            <p class="text-gray-500 mb-4">Start a conversation to explore datasets</p>
            <div class="flex flex-wrap justify-center gap-2">
              {suggestedQueries.map((query) => (
                <button
                  key={query}
                  onClick={() => setInput(query)}
                  class="px-3 py-1.5 text-sm bg-gray-100 hover:bg-gray-200 rounded-full text-gray-700 transition-colors"
                >
                  {query}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <>
            {messages.map((msg) => (
              <ChatMessage
                key={msg.id}
                role={msg.role}
                content={msg.content}
                toolCalls={msg.toolCalls}
                isStreaming={isLoading && msg.role === 'assistant' && msg === messages[messages.length - 1]}
              />
            ))}
          </>
        )}

        {/* Active tool indicators */}
        {activeTools.length > 0 && (
          <div class="pl-2">
            {activeTools.map((tool, i) => (
              <ToolIndicator
                key={`${tool.tool}-${i}`}
                tool={tool.tool}
                status={tool.status}
                preview={tool.preview}
              />
            ))}
          </div>
        )}

        {/* Error message */}
        {error && (
          <div class="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-700">
            {error}
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input area */}
      <div class="p-4 border-t bg-gray-50 rounded-b-lg">
        <div class="flex gap-2">
          <input
            ref={inputRef}
            type="text"
            value={input}
            onInput={(e) => setInput(e.currentTarget.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about datasets..."
            disabled={isLoading}
            class="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-primary-500 disabled:bg-gray-100 disabled:text-gray-500"
          />
          <button
            onClick={sendMessage}
            disabled={isLoading || !input.trim()}
            class="px-4 py-2 bg-primary-500 text-white rounded-lg hover:bg-primary-600 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors"
          >
            {isLoading ? (
              <svg class="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path
                  class="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                />
              </svg>
            ) : (
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8"
                />
              </svg>
            )}
          </button>
        </div>
        <p class="text-xs text-gray-500 mt-2">
          Press Enter to send. The assistant can search datasets and provide information based on your access level.
        </p>
      </div>
    </div>
  );
}
