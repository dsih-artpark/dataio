import { useState } from 'preact/hooks';

interface CodeSnippetsProps {
  datasetId: string;
}

type TabId = 'cli' | 'python' | 'api';

interface CodeBlock {
  label: string;
  code: string;
  description?: string;
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
uv init && uv add dataio-artpark

# Configure your API key
uv run dataio init`,
      },
      {
        label: 'Usage',
        description: 'Download this dataset',
        code: `# Get dataset info
uv run dataio get-dataset ${datasetId}

# Download the dataset
uv run dataio download-dataset ${datasetId}`,
      },
    ],
    python: [
      {
        label: 'Setup',
        description: 'Install and initialize',
        code: `# pip install dataio-artpark
from dataio import DataIOAPI

# Initialize (reads API key from .env or config)
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
        code: `export API_KEY="your-api-key-here"`,
      },
      {
        label: 'Usage',
        description: 'API requests',
        code: `# Get dataset details
curl -H "X-API-Key: $API_KEY" \\
  "https://dataio.artpark.ai/api/v1/datasets/${datasetId}"

# List available tables
curl -H "X-API-Key: $API_KEY" \\
  "https://dataio.artpark.ai/api/v1/datasets/${datasetId}/STANDARDISED/tables"

# Download a specific table
curl -H "X-API-Key: $API_KEY" -O \\
  "https://dataio.artpark.ai/api/v1/datasets/${datasetId}/STANDARDISED/tables/{table_name}"`,
      },
    ],
  };

  const copyToClipboard = async (text: string, id: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(id);
      setTimeout(() => setCopied(null), 2000);
    } catch {
      const textarea = document.createElement('textarea');
      textarea.value = text;
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand('copy');
      document.body.removeChild(textarea);
      setCopied(id);
      setTimeout(() => setCopied(null), 2000);
    }
  };

  const currentBlocks = codeBlocks[activeTab];

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
      {/* Tab header */}
      <div class="flex gap-1 p-1 bg-gray-100 rounded-lg mb-4">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            class={`flex-1 flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium rounded-md transition-all ${
              activeTab === tab.id
                ? 'bg-white text-gray-900 shadow-sm'
                : 'text-gray-600 hover:text-gray-900 hover:bg-gray-50'
            }`}
          >
            {tabIcons[tab.icon]}
            {tab.label}
          </button>
        ))}
      </div>

      {/* Code blocks */}
      <div class="flex-1 overflow-auto space-y-4">
        {currentBlocks.map((block, index) => {
          const blockId = `${activeTab}-${index}`;
          return (
            <div key={blockId} class="rounded-xl overflow-hidden border border-gray-200 bg-white">
              {/* Block header */}
              <div class="flex items-center justify-between px-4 py-2.5 bg-gray-50 border-b border-gray-200">
                <div class="flex items-center gap-3">
                  <span class={`px-2 py-0.5 text-xs font-semibold rounded-full ${
                    block.label === 'Setup'
                      ? 'bg-blue-100 text-blue-700'
                      : 'bg-green-100 text-green-700'
                  }`}>
                    {block.label}
                  </span>
                  {block.description && (
                    <span class="text-sm text-gray-500">{block.description}</span>
                  )}
                </div>
                <button
                  onClick={() => copyToClipboard(block.code, blockId)}
                  class="flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded-md transition-colors bg-white border border-gray-200 hover:bg-gray-100 text-gray-600 hover:text-gray-900"
                >
                  {copied === blockId ? (
                    <>
                      <svg class="w-3.5 h-3.5 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                      </svg>
                      <span class="text-green-600">Copied!</span>
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
              {/* Code content - softer colors */}
              <div class="bg-slate-50">
                <pre class="p-4 text-sm overflow-x-auto">
                  <code class="text-slate-700 leading-relaxed">{block.code}</code>
                </pre>
              </div>
            </div>
          );
        })}
      </div>

      {/* Help link */}
      <div class="mt-4 pt-3 border-t border-gray-100 text-center">
        <a
          href="https://dataio.artpark.ai/docs"
          target="_blank"
          rel="noopener noreferrer"
          class="inline-flex items-center gap-1.5 text-sm text-gray-500 hover:text-primary-600 transition-colors"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
          </svg>
          View full documentation
        </a>
      </div>
    </div>
  );
}
