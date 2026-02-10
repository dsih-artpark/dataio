interface ToolIndicatorProps {
  tool: string;
  status: 'running' | 'complete' | 'error';
  preview?: string;
}

const toolIcons: Record<string, string> = {
  search_datasets: 'M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z',
  get_dataset_details: 'M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z',
  list_categories: 'M4 6h16M4 10h16M4 14h16M4 18h16',
  list_data_owners: 'M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z',
  get_download_info: 'M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4',
  get_dataset_schema: 'M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4',
};

const toolLabels: Record<string, string> = {
  search_datasets: 'Searching datasets',
  get_dataset_details: 'Getting dataset details',
  list_categories: 'Listing categories',
  list_data_owners: 'Listing data owners',
  get_download_info: 'Getting download info',
  get_dataset_schema: 'Getting schema',
};

export default function ToolIndicator({ tool, status, preview }: ToolIndicatorProps) {
  const icon = toolIcons[tool] || toolIcons.search_datasets;
  const label = toolLabels[tool] || tool.replace(/_/g, ' ');

  return (
    <div class="flex items-center gap-2 px-3 py-2 bg-blue-50 rounded-lg text-sm text-blue-700 mb-2">
      {/* Icon */}
      <svg
        class={`w-4 h-4 ${status === 'running' ? 'animate-pulse' : ''}`}
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
      >
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={icon} />
      </svg>

      {/* Label */}
      <span class="font-medium">{label}</span>

      {/* Status indicator */}
      {status === 'running' && (
        <div class="flex gap-1 ml-auto">
          <span class="w-1.5 h-1.5 bg-blue-500 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
          <span class="w-1.5 h-1.5 bg-blue-500 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
          <span class="w-1.5 h-1.5 bg-blue-500 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
        </div>
      )}

      {status === 'complete' && preview && (
        <span class="ml-auto text-blue-600">{preview}</span>
      )}

      {status === 'complete' && (
        <svg class="w-4 h-4 text-green-500 ml-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
        </svg>
      )}

      {status === 'error' && (
        <svg class="w-4 h-4 text-red-500 ml-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
        </svg>
      )}
    </div>
  );
}
