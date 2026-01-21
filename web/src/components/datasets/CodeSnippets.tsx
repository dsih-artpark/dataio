import { useState } from 'preact/hooks';

interface CodeSnippetsProps {
  datasetId: string;
}

type TabId = 'cli' | 'python' | 'api';

export default function CodeSnippets({ datasetId }: CodeSnippetsProps) {
  const [activeTab, setActiveTab] = useState<TabId>('cli');
  const [copied, setCopied] = useState<string | null>(null);

  const tabs: { id: TabId; label: string }[] = [
    { id: 'cli', label: 'CLI' },
    { id: 'python', label: 'Python' },
    { id: 'api', label: 'REST API' },
  ];

  const codeSnippets: Record<TabId, { code: string; filename: string }> = {
    cli: {
      filename: 'terminal',
      code: `# Install dataio (if not already installed)
uv init && uv add dataio-artpark

# Initialize and configure your API key
uv run dataio init

# Get dataset info
uv run dataio get-dataset ${datasetId}

# Download the dataset
uv run dataio download-dataset ${datasetId}`,
    },
    python: {
      filename: 'example.py',
      code: `from dataio import DataIOAPI

# Initialize the client (reads API key from .env or config)
client = DataIOAPI()

# Get dataset metadata
details = client.get_dataset_details("${datasetId}")
print(f"Dataset: {details['title']}")
print(f"Tables: {details.get('tables', [])}")

# Download the dataset to local directory
path = client.download_dataset(
    "${datasetId}",
    root_dir="./data"
)
print(f"Downloaded to: {path}")`,
    },
    api: {
      filename: 'curl',
      code: `# Set your API key
export API_KEY="your-api-key-here"

# Get dataset details
curl -H "X-API-Key: $API_KEY" \\
  "https://dataio.artpark.ai/api/v1/datasets/${datasetId}"

# List available tables
curl -H "X-API-Key: $API_KEY" \\
  "https://dataio.artpark.ai/api/v1/datasets/${datasetId}/STANDARDISED/tables"

# Download a specific table
curl -H "X-API-Key: $API_KEY" -O \\
  "https://dataio.artpark.ai/api/v1/datasets/${datasetId}/STANDARDISED/tables/{table_name}"`,
    },
  };

  const copyToClipboard = async (text: string, id: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(id);
      setTimeout(() => setCopied(null), 2000);
    } catch {
      // Fallback for older browsers
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

  const currentSnippet = codeSnippets[activeTab];

  return (
    <div class="h-full flex flex-col">
      {/* Tab header */}
      <div class="flex border-b border-gray-200">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            class={`px-4 py-2 text-sm font-medium transition-colors ${
              activeTab === tab.id
                ? 'text-primary-600 border-b-2 border-primary-600 -mb-px'
                : 'text-gray-500 hover:text-gray-700'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Code content */}
      <div class="flex-1 overflow-auto bg-gray-900 rounded-b-lg">
        <div class="flex items-center justify-between px-4 py-2 border-b border-gray-700">
          <span class="text-xs text-gray-400">{currentSnippet.filename}</span>
          <button
            onClick={() => copyToClipboard(currentSnippet.code, activeTab)}
            class="flex items-center gap-1.5 px-2 py-1 text-xs text-gray-400 hover:text-white rounded hover:bg-gray-700 transition-colors"
          >
            {copied === activeTab ? (
              <>
                <svg class="w-4 h-4 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
                Copied!
              </>
            ) : (
              <>
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                </svg>
                Copy
              </>
            )}
          </button>
        </div>
        <pre class="p-4 text-sm text-gray-300 overflow-x-auto">
          <code>{currentSnippet.code}</code>
        </pre>
      </div>

      {/* Help link */}
      <div class="mt-3 text-center">
        <a
          href="https://dataio.artpark.ai/docs"
          target="_blank"
          rel="noopener noreferrer"
          class="text-xs text-gray-500 hover:text-primary-600"
        >
          View full documentation →
        </a>
      </div>
    </div>
  );
}
