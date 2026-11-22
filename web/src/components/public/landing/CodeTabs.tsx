import { useRef, useState } from 'preact/hooks';

const TABS = [
  {
    id: 'cli',
    label: 'CLI',
    code: `$ uv add dataio-artpark
$ uv run dataio init
$ uv run dataio list-datasets --collection "Census Data"
$ uv run dataio download-dataset TS0001DS0042`,
  },
  {
    id: 'python',
    label: 'Python SDK',
    code: `from dataio import DataIOAPI

client = DataIOAPI()  # reads your API key from .env
datasets = client.list_datasets(limit=50)
path = client.download_dataset("TS0001DS0042", root_dir="./data")`,
  },
  {
    id: 'api',
    label: 'REST API',
    code: `$ curl -H "X-API-Key: $API_KEY" \\
    "https://dataio.artpark.ai/api/v1/datasets"
$ curl -H "X-API-Key: $API_KEY" \\
    "https://dataio.artpark.ai/api/v1/shapefiles/state_29"`,
  },
] as const;

/** CLI / Python / REST examples in the Developers section (ARIA tabs, arrow keys move between them). */
export default function CodeTabs() {
  const [active, setActive] = useState(0);
  const tabRefs = useRef<(HTMLButtonElement | null)[]>([]);

  const select = (index: number) => {
    const next = (index + TABS.length) % TABS.length;
    setActive(next);
    tabRefs.current[next]?.focus();
  };

  const onKeyDown = (event: KeyboardEvent) => {
    if (event.key === 'ArrowRight') select(active + 1);
    else if (event.key === 'ArrowLeft') select(active - 1);
    else if (event.key === 'Home') select(0);
    else if (event.key === 'End') select(TABS.length - 1);
    else return;
    event.preventDefault();
  };

  return (
    <div class="flex-[1_1_480px] min-w-0 rounded-xl bg-gray-900 border border-primary-900 overflow-hidden">
      <div role="tablist" aria-label="Code examples" class="flex gap-1 p-2 border-b border-primary-900" onKeyDown={onKeyDown}>
        {TABS.map((tab, index) => (
          <button
            key={tab.id}
            ref={(el) => {
              tabRefs.current[index] = el;
            }}
            type="button"
            role="tab"
            id={`code-tab-${tab.id}`}
            aria-selected={index === active}
            aria-controls={`code-panel-${tab.id}`}
            tabIndex={index === active ? 0 : -1}
            onClick={() => setActive(index)}
            class={`min-h-[44px] px-3.5 rounded-md text-[13px] focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-300 ${
              index === active ? 'bg-primary-900 text-white' : 'bg-transparent text-primary-200 hover:text-white'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>
      {TABS.map((tab, index) => (
        <div
          key={tab.id}
          role="tabpanel"
          id={`code-panel-${tab.id}`}
          aria-labelledby={`code-tab-${tab.id}`}
          hidden={index !== active}
          tabIndex={0}
          class="focus:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-primary-300"
        >
          <pre class="m-0 p-5 font-mono text-[13px] leading-[1.7] text-gray-200 overflow-x-auto">
            <code>{tab.code}</code>
          </pre>
        </div>
      ))}
    </div>
  );
}
