import { useState } from 'preact/hooks';
import type { JSX } from 'preact';

interface CodeSnippetsProps {
  datasetId: string;
}

type TabId = 'cli' | 'python' | 'api';

interface CodeBlock {
  label: string;
  code: string;
  description?: string;
}

// Simple syntax highlighter for code blocks
function highlightCode(code: string, language: TabId): JSX.Element[] {
  const lines = code.split('\n');

  return lines.map((line, lineIndex) => {
    const elements: JSX.Element[] = [];
    let remaining = line;
    let keyIndex = 0;

    // Process the line character by character
    while (remaining.length > 0) {
      // Comments (# for shell/python)
      if (remaining.startsWith('#')) {
        elements.push(<span key={`${lineIndex}-${keyIndex++}`} class="text-gray-500">{remaining}</span>);
        break;
      }

      // Shell prompt
      if (remaining.startsWith('$ ')) {
        elements.push(<span key={`${lineIndex}-${keyIndex++}`} class="text-green-400">$</span>);
        elements.push(<span key={`${lineIndex}-${keyIndex++}`}> </span>);
        remaining = remaining.slice(2);
        continue;
      }

      // Python keywords
      if (language === 'python') {
        const pythonKeywords = ['from', 'import', 'as', 'def', 'class', 'return', 'if', 'else', 'elif', 'for', 'while', 'try', 'except', 'with', 'print'];
        let matched = false;
        for (const keyword of pythonKeywords) {
          const regex = new RegExp(`^(${keyword})(?=[^a-zA-Z0-9_]|$)`);
          const match = remaining.match(regex);
          if (match) {
            elements.push(<span key={`${lineIndex}-${keyIndex++}`} class="text-purple-400">{match[1]}</span>);
            remaining = remaining.slice(match[1].length);
            matched = true;
            break;
          }
        }
        if (matched) continue;
      }

      // Strings (double quotes)
      if (remaining.startsWith('"')) {
        const endIndex = remaining.indexOf('"', 1);
        if (endIndex !== -1) {
          const str = remaining.slice(0, endIndex + 1);
          elements.push(<span key={`${lineIndex}-${keyIndex++}`} class="text-green-400">{str}</span>);
          remaining = remaining.slice(endIndex + 1);
          continue;
        }
      }

      // Strings (single quotes)
      if (remaining.startsWith("'")) {
        const endIndex = remaining.indexOf("'", 1);
        if (endIndex !== -1) {
          const str = remaining.slice(0, endIndex + 1);
          elements.push(<span key={`${lineIndex}-${keyIndex++}`} class="text-green-400">{str}</span>);
          remaining = remaining.slice(endIndex + 1);
          continue;
        }
      }

      // f-strings in Python
      if (remaining.startsWith('f"') || remaining.startsWith("f'")) {
        const quote = remaining[1];
        const endIndex = remaining.indexOf(quote, 2);
        if (endIndex !== -1) {
          const str = remaining.slice(0, endIndex + 1);
          elements.push(<span key={`${lineIndex}-${keyIndex++}`} class="text-green-400">{str}</span>);
          remaining = remaining.slice(endIndex + 1);
          continue;
        }
      }

      // Numbers
      const numMatch = remaining.match(/^(\d+)/);
      if (numMatch) {
        elements.push(<span key={`${lineIndex}-${keyIndex++}`} class="text-blue-400">{numMatch[1]}</span>);
        remaining = remaining.slice(numMatch[1].length);
        continue;
      }

      // Environment variables ($VAR)
      const envMatch = remaining.match(/^(\$[A-Z_][A-Z0-9_]*)/);
      if (envMatch) {
        elements.push(<span key={`${lineIndex}-${keyIndex++}`} class="text-cyan-400">{envMatch[1]}</span>);
        remaining = remaining.slice(envMatch[1].length);
        continue;
      }

      // Commands after shell prompt (first word)
      if (language === 'cli' || language === 'api') {
        const cmdMatch = remaining.match(/^(curl|uv|dataio|export)\b/);
        if (cmdMatch) {
          elements.push(<span key={`${lineIndex}-${keyIndex++}`} class="text-yellow-400">{cmdMatch[1]}</span>);
          remaining = remaining.slice(cmdMatch[1].length);
          continue;
        }
      }

      // Flags (-x, --xxx)
      const flagMatch = remaining.match(/^(--?[a-zA-Z][a-zA-Z0-9-]*)/);
      if (flagMatch) {
        elements.push(<span key={`${lineIndex}-${keyIndex++}`} class="text-cyan-400">{flagMatch[1]}</span>);
        remaining = remaining.slice(flagMatch[1].length);
        continue;
      }

      // Default: take one character
      elements.push(<span key={`${lineIndex}-${keyIndex++}`}>{remaining[0]}</span>);
      remaining = remaining.slice(1);
    }

    return <div key={lineIndex}>{elements.length > 0 ? elements : '\u00A0'}</div>;
  });
}

export default function CodeSnippets({ datasetId }: CodeSnippetsProps) {
  const [activeTab, setActiveTab] = useState<TabId>('cli');
  const [copied, setCopied] = useState<string | null>(null);

  const tabs: { id: TabId; label: string; icon: string }[] = [
    { id: 'cli', label: 'CLI', icon: 'terminal' },
    { id: 'python', label: 'Python', icon: 'python' },
    { id: 'api', label: 'REST API', icon: 'api' },
  ];

  const codeBlocks: Record<TabId, CodeBlock[]> = {
    cli: [
      {
        label: 'Setup',
        description: 'Install and configure (one-time)',
        code: `# Install dataio
$ uv init && uv add dataio-artpark

# Configure your API key
$ uv run dataio init`,
      },
      {
        label: 'Usage',
        description: 'Download this dataset',
        code: `# Download the dataset
$ uv run dataio download-dataset ${datasetId}`,
      },
    ],
    python: [
      {
        label: 'Setup',
        description: 'Install and initialize',
        code: `# pip install dataio-artpark
import os
from dotenv import load_dotenv
load_dotenv()  # Load DATAIO_API_KEY from .env

from dataio import DataIOAPI
client = DataIOAPI()`,
      },
      {
        label: 'Usage',
        description: 'Download this dataset',
        code: `# Get dataset metadata
details = client.get_dataset_details("${datasetId}")
print(f"Dataset: {details['title']}")

# Download to local directory
path = client.download_dataset(
    "${datasetId}",
    root_dir="./data"
)
print(f"Downloaded to: {path}")`,
      },
    ],
    api: [
      {
        label: 'Setup',
        description: 'Set your API key',
        code: `$ export API_KEY="your-api-key-here"`,
      },
      {
        label: 'Usage',
        description: 'API requests',
        code: `# Get dataset details
$ curl -H "X-API-Key: $API_KEY" \\
  "https://dataio.artpark.ai/api/v1/datasets/${datasetId}"

# List available tables
$ curl -H "X-API-Key: $API_KEY" \\
  "https://dataio.artpark.ai/api/v1/datasets/${datasetId}/STANDARDISED/tables"

# Download a specific table
$ curl -H "X-API-Key: $API_KEY" -O \\
  "https://dataio.artpark.ai/api/v1/datasets/${datasetId}/STANDARDISED/tables/{table_name}"`,
      },
    ],
  };

  const copyToClipboard = async (text: string, id: string) => {
    // Remove shell prompts for cleaner clipboard content
    const cleanText = text.replace(/^\$ /gm, '');
    try {
      await navigator.clipboard.writeText(cleanText);
      setCopied(id);
      setTimeout(() => setCopied(null), 2000);
    } catch {
      const textarea = document.createElement('textarea');
      textarea.value = cleanText;
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand('copy');
      document.body.removeChild(textarea);
      setCopied(id);
      setTimeout(() => setCopied(null), 2000);
    }
  };

  const currentBlocks = codeBlocks[activeTab];

  const docsLinks: Record<TabId, { url: string; label: string }> = {
    cli: { url: 'https://dataio.artpark.ai/docs/cli/', label: 'CLI docs' },
    python: { url: 'https://dataio.artpark.ai/docs/sdk', label: 'SDK docs' },
    api: { url: 'https://dataio.artpark.ai/api/v1', label: 'API reference' },
  };

  const tabIcons: Record<string, JSX.Element> = {
    terminal: (
      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
      </svg>
    ),
    python: (
      <svg class="w-4 h-4" viewBox="0 0 24 24" fill="currentColor">
        <path d="M12 0C5.373 0 5.373 2.4 5.373 4.8v1.8h6.627v.6H4.8C2.4 7.2 0 8.4 0 12s2.4 4.8 4.8 4.8h1.8v-2.16c0-2.64 2.28-4.8 4.8-4.8h4.8c2.16 0 3.6-1.44 3.6-3.6V4.8C19.8 2.4 17.64 0 12 0zm-2.4 2.4a.9.9 0 11-.002 1.8A.9.9 0 019.6 2.4z"/>
        <path d="M12 24c6.627 0 6.627-2.4 6.627-4.8v-1.8h-6.627v-.6h7.2c2.4 0 4.8-1.2 4.8-4.8s-2.4-4.8-4.8-4.8h-1.8v2.16c0 2.64-2.28 4.8-4.8 4.8h-4.8c-2.16 0-3.6 1.44-3.6 3.6v1.44C4.2 21.6 6.36 24 12 24zm2.4-2.4a.9.9 0 110-1.8.9.9 0 010 1.8z"/>
      </svg>
    ),
    api: (
      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
      </svg>
    ),
  };

  return (
    <div class="h-full flex flex-col">
      {/* Header with title and docs link */}
      <div class="flex items-center justify-between mb-4">
        <h3 class="text-sm font-semibold text-gray-700">Quick Start</h3>
        <a
          href={docsLinks[activeTab].url}
          target="_blank"
          rel="noopener noreferrer"
          class="inline-flex items-center gap-1.5 text-xs text-primary-600 hover:text-primary-700 font-medium transition-colors"
        >
          {docsLinks[activeTab].label}
          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
          </svg>
        </a>
      </div>

      {/* Dark themed code container */}
      <div class="flex-1 bg-gray-900 rounded-xl border border-gray-700 overflow-hidden flex flex-col">
        {/* Tab header */}
        <div class="flex border-b border-gray-700">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              class={`flex-1 flex items-center justify-center gap-2 px-3 py-2.5 text-sm font-medium transition-colors ${
                activeTab === tab.id
                  ? 'text-white bg-gray-800 border-b-2 border-primary-500'
                  : 'text-gray-400 hover:text-white hover:bg-gray-800/50'
              }`}
            >
              {tabIcons[tab.icon]}
              {tab.label}
            </button>
          ))}
        </div>

        {/* Code blocks */}
        <div class="flex-1 overflow-auto p-4 space-y-4">
          {currentBlocks.map((block, index) => {
            const blockId = `${activeTab}-${index}`;
            return (
              <div key={blockId} class="rounded-lg overflow-hidden bg-gray-800 border border-gray-700">
                {/* Block header */}
                <div class="flex items-center justify-between px-4 py-2 bg-gray-800/80 border-b border-gray-700">
                  <div class="flex items-center gap-3">
                    <span class={`px-2 py-0.5 text-xs font-semibold rounded-full ${
                      block.label === 'Setup'
                        ? 'bg-blue-900/50 text-blue-300 border border-blue-700'
                        : 'bg-green-900/50 text-green-300 border border-green-700'
                    }`}>
                      {block.label}
                    </span>
                    {block.description && (
                      <span class="text-xs text-gray-400">{block.description}</span>
                    )}
                  </div>
                  <button
                    onClick={() => copyToClipboard(block.code, blockId)}
                    class="flex items-center gap-1.5 px-2 py-1 text-xs font-medium rounded transition-colors text-gray-400 hover:text-white hover:bg-gray-700"
                  >
                    {copied === blockId ? (
                      <>
                        <svg class="w-3.5 h-3.5 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                        </svg>
                        <span class="text-green-400">Copied!</span>
                      </>
                    ) : (
                      <>
                        <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                        </svg>
                        Copy
                      </>
                    )}
                  </button>
                </div>
                {/* Code content with syntax highlighting */}
                <div class="p-4 overflow-x-auto">
                  <pre class="text-sm text-gray-300 leading-relaxed font-mono">
                    <code>{highlightCode(block.code, activeTab)}</code>
                  </pre>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
