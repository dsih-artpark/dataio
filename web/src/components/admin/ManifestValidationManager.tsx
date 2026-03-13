import { useMemo, useState } from 'preact/hooks';

import { api } from '../../lib/api';
import type { ValidationFinding, ValidationResult } from '../../lib/types';

type DatasetKind = 'tabular' | 'geojson';

const SUPPORTED_DATA_TYPES = ['boolean', 'date', 'dateTime', 'enum', 'float', 'int', 'regionID', 'regionName', 'string'] as const;

const RULE_GROUPS = [
  {
    title: 'Required top-level manifest keys',
    items: [
      '`metadataSpecVersion`, `datasetTitle`, `datasetSlug`, `datasetDescription`, `source`, `category`, `collection`',
      '`datasetKind` must be either `tabular` or `geojson`',
      '`datasetID` should follow the platform dataset identifier pattern when provided',
    ],
  },
  {
    title: 'Tabular manifest rules',
    items: [
      '`datasetTables` is required for tabular manifests',
      'Each table should define a non-empty `dataDictionary`',
      'Uploaded table columns are checked against declared fields',
    ],
  },
  {
    title: 'Field-level rules',
    items: [
      `Allowed data types: ${SUPPORTED_DATA_TYPES.join(', ')}`,
      '`enum` fields must declare `allowedValues` or `enumRef`',
      '`date` and `dateTime` fields must provide valid strftime-based `format` values',
      '`dateTime` formats must include timezone information via `%z`',
      '`range` cannot be combined with `min` or `max`',
    ],
  },
  {
    title: 'Data validation rules',
    items: [
      'Required columns must exist in uploaded files',
      'Unknown declared field types are treated as validation errors',
      'Cell values are checked against their declared type and nullability',
      'Deep check optionally validates dataset, collection, and category identifiers against the platform database',
    ],
  },
] as const;

interface ReviewLine {
  lineNumber: number;
  text: string;
  findings: ValidationFinding[];
  paths: string[];
}

function statusClasses(status: ValidationResult['status']) {
  if (status === 'fail') return 'bg-red-50 text-red-700 ring-red-200';
  if (status === 'warn') return 'bg-amber-50 text-amber-700 ring-amber-200';
  return 'bg-emerald-50 text-emerald-700 ring-emerald-200';
}

function severityClasses(severity: string) {
  if (severity === 'error') return 'bg-red-50 text-red-700 ring-red-200';
  if (severity === 'warning') return 'bg-amber-50 text-amber-700 ring-amber-200';
  return 'bg-emerald-50 text-emerald-700 ring-emerald-200';
}

function buildLineReviews(manifestText: string, findings: ValidationFinding[]): ReviewLine[] {
  const lines = manifestText.split(/\r?\n/);
  const stack: { indent: number; path: string }[] = [];

  return lines.map((text, index) => {
    const indent = text.match(/^(\s*)/)?.[1].length ?? 0;
    const trimmed = text.trim();
    const keyMatch = trimmed.match(/^['"]?([^'":]+)['"]?\s*:/);

    while (stack.length > 0 && indent <= stack[stack.length - 1].indent) {
      stack.pop();
    }

    const paths: string[] = [];
    if (keyMatch) {
      const key = keyMatch[1].trim();
      const parentPath = stack.length > 0 ? stack[stack.length - 1].path : '';
      const fullPath = parentPath ? `${parentPath}.${key}` : key;
      stack.push({ indent, path: fullPath });

      for (let i = stack.length - 1; i >= 0; i -= 1) {
        paths.push(stack[i].path);
      }
    }

    const matchingFindings = findings.filter((finding) => {
      const findingPath = finding.path;
      if (!findingPath) return false;
      if (findingPath === 'manifest') return index === 0;
      return paths.some((path) => findingPath === path);
    });

    return {
      lineNumber: index + 1,
      text,
      findings: matchingFindings,
      paths,
    };
  });
}

function findingLabel(finding: ValidationFinding) {
  const parts = [
    finding.path ? `path ${finding.path}` : null,
    finding.table ? `table ${finding.table}` : null,
    typeof finding.row === 'number' ? `row ${finding.row}` : null,
    finding.field ? `field ${finding.field}` : null,
  ].filter(Boolean);
  return parts.join(' • ');
}

function buildNarrative(result: ValidationResult) {
  if (result.status === 'pass') {
    return `Validation completed successfully. The manifest parsed, ${result.summary.tables_checked || 0} table definitions were checked, and ${result.summary.rows_checked || 0} data rows were scanned without errors or warnings.`;
  }

  if (result.status === 'warn') {
    return `Validation completed with ${result.summary.warnings} warning${result.summary.warnings === 1 ? '' : 's'}. The manifest is structurally acceptable, but there are issues you should review before promotion.`;
  }

  return `Validation failed with ${result.summary.errors} error${result.summary.errors === 1 ? '' : 's'} and ${result.summary.warnings} warning${result.summary.warnings === 1 ? '' : 's'}. Fix the highlighted manifest paths first, then rerun validation to confirm downstream data checks.`;
}

function buildVerboseSuccessMessages(paths: string[], text: string): string[] {
  if (paths.length === 0) return [];

  const currentPath = paths[0];
  const rawValue = text.includes(':') ? text.split(':').slice(1).join(':').trim().replace(/^['"]|['"]$/g, '') : '';
  const messages: string[] = [];

  if (currentPath.endsWith('.type') && SUPPORTED_DATA_TYPES.includes(rawValue as (typeof SUPPORTED_DATA_TYPES)[number])) {
    messages.push(`'${rawValue}' is an allowed data type.`);
  }
  if (currentPath.endsWith('.format') && rawValue.includes('%')) {
    messages.push(`'${rawValue}' uses strftime directives, which is required for date and datetime fields.`);
  }
  if (currentPath.endsWith('.nullable') && ['true', 'false'].includes(rawValue.toLowerCase())) {
    messages.push(`'${rawValue.toLowerCase()}' is a valid nullability flag.`);
  }
  if (currentPath === 'datasetKind' && ['tabular', 'geojson'].includes(rawValue)) {
    messages.push(`'${rawValue}' is an allowed dataset kind.`);
  }
  if (currentPath.endsWith('.enumRef') && rawValue) {
    messages.push(`Enum reference '${rawValue}' is syntactically valid and will be resolved against enumDefinitions.`);
  }
  if (currentPath.endsWith('.allowedValues') && rawValue) {
    messages.push('Allowed values are declared inline for this enum field.');
  }

  return messages;
}

export default function ManifestValidationManager() {
  const [datasetKind, setDatasetKind] = useState<DatasetKind>('tabular');
  const [manifestFile, setManifestFile] = useState<File | null>(null);
  const [manifestText, setManifestText] = useState('');
  const [dataFile, setDataFile] = useState<File | null>(null);
  const [tableName, setTableName] = useState('');
  const [deepCheck, setDeepCheck] = useState(true);
  const [showRules, setShowRules] = useState(false);
  const [showVerboseSuccess, setShowVerboseSuccess] = useState(false);
  const [showIssueRowsOnly, setShowIssueRowsOnly] = useState(false);
  const [validating, setValidating] = useState(false);
  const [validationError, setValidationError] = useState('');
  const [validationResult, setValidationResult] = useState<ValidationResult | null>(null);

  const lineReviews = useMemo(
    () => (manifestText ? buildLineReviews(manifestText, validationResult?.findings ?? []) : []),
    [manifestText, validationResult]
  );

  const passedChecks = useMemo(() => {
    if (!validationResult) return [];

    const findingPaths = new Set(validationResult.findings.map((finding) => finding.path).filter(Boolean));
    const checks = [
      { label: 'Manifest parses successfully', path: 'manifest' },
      { label: 'datasetID is present and patterned correctly', path: 'datasetID' },
      { label: 'datasetSlug is present and correctly prefixed with datasetID', path: 'datasetSlug' },
      { label: 'Category identifiers and names are internally valid', path: 'category.ID' },
      { label: 'Collection identifiers and names are internally valid', path: 'collection.ID' },
      { label: 'Table definitions include data dictionaries', path: 'datasetTables' },
    ];

    return checks.filter((check) => !findingPaths.has(check.path));
  }, [validationResult]);

  const issueSummary = useMemo(
    () =>
      lineReviews
        .filter((line) => line.findings.length > 0)
        .map((line) => ({
          lineNumber: line.lineNumber,
          path: line.findings[0]?.path || line.paths[0] || 'manifest',
          messages: Array.from(new Set(line.findings.map((finding) => finding.message))),
        })),
    [lineReviews]
  );

  const displayedLineReviews = useMemo(() => {
    if (!showIssueRowsOnly) return lineReviews;

    const visibleLineNumbers = new Set<number>();
    for (const line of lineReviews) {
      if (line.findings.length > 0) {
        for (let lineNumber = line.lineNumber - 2; lineNumber <= line.lineNumber + 2; lineNumber += 1) {
          if (lineNumber >= 1 && lineNumber <= lineReviews.length) {
            visibleLineNumbers.add(lineNumber);
          }
        }
      }
    }

    return lineReviews.filter((line) => visibleLineNumbers.has(line.lineNumber));
  }, [lineReviews, showIssueRowsOnly]);

  const handleManifestChange = async (file: File | null) => {
    setManifestFile(file);
    setValidationResult(null);
    setValidationError('');
    if (!file) {
      setManifestText('');
      return;
    }
    setManifestText(await file.text());
  };

  const handleValidation = async (e: Event) => {
    e.preventDefault();
    if (!manifestFile) {
      setValidationError('Choose a manifest file to validate.');
      return;
    }

    setValidating(true);
    setValidationError('');
    setValidationResult(null);

    try {
      const result =
        datasetKind === 'tabular'
          ? await api.adminValidateTabular({
              manifestFile,
              tableFile: dataFile,
              tableName: tableName || undefined,
              deepCheck,
            })
          : await api.adminValidateGeojson({
              manifestFile,
              geojsonFile: dataFile,
              deepCheck,
            });
      setValidationResult(result);
    } catch (err) {
      setValidationError(err instanceof Error ? err.message : 'Validation failed');
    } finally {
      setValidating(false);
    }
  };

  return (
    <div class="grid min-h-[calc(100vh-10rem)] gap-6 xl:grid-cols-[minmax(23rem,0.82fr)_minmax(0,1.6fr)]">
      <section class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
        <div>
          <h2 class="text-lg font-semibold text-slate-900">Manifest Validation</h2>
          <p class="mt-1 text-sm text-slate-600">
            Validate a candidate manifest and optional data file before promoting it.
          </p>
          <p class="mt-2 text-sm text-slate-500">
            Uploaded files are only kept in your browser until you submit this form. The validation request reads them in memory and does not persist them.
          </p>
        </div>

        <form class="mt-6 space-y-4" onSubmit={handleValidation}>
          <div class="rounded-2xl border border-slate-200 bg-slate-50">
            <button
              type="button"
              onClick={() => setShowRules((value) => !value)}
              class="flex w-full items-center justify-between px-4 py-3 text-left"
            >
              <div>
                <span class="block text-sm font-semibold text-slate-900">Validation rules and allowed values</span>
                <span class="mt-1 block text-xs text-slate-500">
                  Open the full rulebook, including supported data types and manifest constraints.
                </span>
              </div>
              <span class="rounded-full border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700">
                {showRules ? 'Hide' : 'Show'}
              </span>
            </button>
            {showRules ? (
              <div class="grid gap-4 border-t border-slate-200 px-4 py-4">
                {RULE_GROUPS.map((group) => (
                  <div key={group.title} class="rounded-xl border border-slate-200 bg-white p-4">
                    <h3 class="text-sm font-semibold text-slate-900">{group.title}</h3>
                    <div class="mt-2 space-y-2 text-sm text-slate-600">
                      {group.items.map((item) => (
                        <p key={item}>{item}</p>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            ) : null}
          </div>

          <div class="grid gap-4 xl:grid-cols-2">
            <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <span class="mb-2 block text-sm font-medium text-slate-700">Candidate manifest</span>
              <input
                type="file"
                accept=".yaml,.yml"
                onChange={(e) => {
                  const input = e.currentTarget as HTMLInputElement;
                  handleManifestChange(input.files?.[0] ?? null);
                }}
                class="block w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 file:mr-4 file:rounded-lg file:border-0 file:bg-slate-900 file:px-3 file:py-2 file:text-sm file:font-medium file:text-white"
              />
              <div class="mt-3 flex min-h-11 items-center justify-between rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm">
                <span class="truncate text-slate-700">{manifestFile?.name || 'No manifest selected'}</span>
                {manifestFile ? (
                  <button
                    type="button"
                    onClick={() => handleManifestChange(null)}
                    class="rounded-lg border border-slate-300 px-3 py-1.5 text-slate-700 transition hover:bg-slate-50"
                  >
                    Remove
                  </button>
                ) : null}
              </div>
            </div>

            <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <span class="mb-2 block text-sm font-medium text-slate-700">
                {datasetKind === 'tabular' ? 'CSV or table file (optional)' : 'GeoJSON file (optional)'}
              </span>
              <input
                type="file"
                accept={datasetKind === 'tabular' ? '.csv,text/csv' : '.geojson,.json,application/geo+json,application/json'}
                onChange={(e) => {
                  const input = e.currentTarget as HTMLInputElement;
                  setDataFile(input.files?.[0] ?? null);
                }}
                class="block w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 file:mr-4 file:rounded-lg file:border-0 file:bg-slate-200 file:px-3 file:py-2 file:text-sm file:font-medium file:text-slate-900"
              />
              <div class="mt-3 flex min-h-11 items-center justify-between rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm">
                <span class="truncate text-slate-700">{dataFile?.name || 'No data file selected'}</span>
                {dataFile ? (
                  <button
                    type="button"
                    onClick={() => setDataFile(null)}
                    class="rounded-lg border border-slate-300 px-3 py-1.5 text-slate-700 transition hover:bg-slate-50"
                  >
                    Remove
                  </button>
                ) : null}
              </div>
            </div>
          </div>

          <div class="grid gap-4 xl:grid-cols-[0.8fr_1fr]">
            <label class="block">
              <span class="mb-2 block text-sm font-medium text-slate-700">Dataset kind</span>
              <select
                value={datasetKind}
                onChange={(e) => setDatasetKind((e.currentTarget as HTMLSelectElement).value as DatasetKind)}
                class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm focus:border-slate-500 focus:outline-none focus:ring-2 focus:ring-slate-200"
              >
                <option value="tabular">Tabular</option>
                <option value="geojson">GeoJSON</option>
              </select>
            </label>

            {datasetKind === 'tabular' ? (
              <label class="block">
                <span class="mb-2 block text-sm font-medium text-slate-700">Table name</span>
                <input
                  value={tableName}
                  onInput={(e) => setTableName((e.currentTarget as HTMLInputElement).value)}
                  placeholder="Optional if the manifest defines a single table"
                  class="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm shadow-sm focus:border-slate-500 focus:outline-none focus:ring-2 focus:ring-slate-200"
                />
              </label>
            ) : (
              <div class="rounded-2xl border border-dashed border-slate-200 px-4 py-3 text-sm text-slate-500">
                GeoJSON validation does not require a table name.
              </div>
            )}
          </div>

          <div class="grid gap-4 xl:grid-cols-2">
            <label class="flex items-center gap-3 rounded-2xl border border-slate-200 px-4 py-3">
              <input
                type="checkbox"
                checked={deepCheck}
                onChange={(e) => setDeepCheck((e.currentTarget as HTMLInputElement).checked)}
                class="h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-400"
              />
              <span>
                <span class="block text-sm font-medium text-slate-800">Deep check</span>
                <span class="block text-xs text-slate-500">Also verify dataset, collection, and category identity against the database.</span>
              </span>
            </label>

            <label class="flex items-center gap-3 rounded-2xl border border-slate-200 px-4 py-3">
              <input
                type="checkbox"
                checked={showVerboseSuccess}
                onChange={(e) => setShowVerboseSuccess((e.currentTarget as HTMLInputElement).checked)}
                class="h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-400"
              />
              <span>
                <span class="block text-sm font-medium text-slate-800">Verbose success messages</span>
                <span class="block text-xs text-slate-500">Show positive rule confirmations in the annotated YAML, not just failures.</span>
              </span>
            </label>
          </div>

          <label class="flex items-center gap-3 rounded-2xl border border-slate-200 px-4 py-3">
            <input
              type="checkbox"
              checked={showIssueRowsOnly}
              onChange={(e) => setShowIssueRowsOnly((e.currentTarget as HTMLInputElement).checked)}
              class="h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-400"
            />
            <span>
              <span class="block text-sm font-medium text-slate-800">Filter to issue rows with context</span>
              <span class="block text-xs text-slate-500">Show only lines with findings and two nearby lines on either side.</span>
            </span>
          </label>

          {validationError ? (
            <div class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              {validationError}
            </div>
          ) : null}

          <button
            type="submit"
            disabled={!manifestFile || validating}
            class="rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {validating ? 'Validating...' : 'Run Validation'}
          </button>
        </form>
      </section>

      <section class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
        <div class="flex items-center justify-between">
          <div>
            <h2 class="text-lg font-semibold text-slate-900">Review</h2>
            <p class="mt-1 text-sm text-slate-600">
              The validator calls out what it accepted and what it rejected from the uploaded manifest.
            </p>
          </div>
          {validationResult ? (
            <span class={`rounded-full px-3 py-1 text-sm font-semibold ring-1 ${statusClasses(validationResult.status)}`}>
              {validationResult.status.toUpperCase()}
            </span>
          ) : null}
        </div>

        {!validationResult ? (
          <div class="mt-6 rounded-xl border border-dashed border-slate-300 px-4 py-8 text-sm text-slate-500">
            Upload a manifest and run validation to see line-by-line review and findings.
          </div>
        ) : (
          <div class="mt-6 grid min-h-[42rem] gap-6 xl:grid-cols-[minmax(0,1.05fr)_minmax(24rem,0.95fr)]">
            <div>
              <div class="flex items-center justify-between">
                <h3 class="text-sm font-semibold text-slate-900">Annotated manifest</h3>
                <span class="text-xs text-slate-500">
                  {showIssueRowsOnly ? 'Issue rows plus nearby context' : 'Full deep-dive view'}
                </span>
              </div>
              <div class="mt-3 max-h-[70vh] overflow-auto rounded-2xl border border-slate-200">
                {displayedLineReviews.map((line) => (
                  <div key={line.lineNumber} class="grid grid-cols-[56px_1fr] border-b border-slate-100 last:border-b-0">
                    <div class="bg-slate-50 px-3 py-2 text-right font-mono text-xs text-slate-400">{line.lineNumber}</div>
                    <div class="px-3 py-2">
                      <pre class="overflow-x-auto whitespace-pre text-xs leading-6 text-slate-800">{line.text || ' '}</pre>
                      {line.paths.length > 0 ? (
                        <div class="mt-1 text-[10px] uppercase tracking-wide text-slate-400">
                          {line.paths[0]}
                        </div>
                      ) : null}
                      {line.findings.length === 0 ? (
                        showVerboseSuccess && line.text.trim() ? (
                          <div class="mt-2 space-y-2">
                            {buildVerboseSuccessMessages(line.paths, line.text).map((message) => (
                              <div key={message} class="rounded-lg bg-emerald-50 px-3 py-2 text-[11px] text-emerald-700 ring-1 ring-emerald-200">
                                {message}
                              </div>
                            ))}
                          </div>
                        ) : null
                      ) : (
                        <div class="mt-2 space-y-2">
                          {line.findings.map((finding, index) => (
                            <div key={`${line.lineNumber}-${finding.code}-${index}`} class={`rounded-lg px-3 py-2 text-[11px] ring-1 ${severityClasses(finding.severity)}`}>
                              <div class="font-semibold uppercase">{finding.severity}</div>
                              <div class="mt-1">{finding.message}</div>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div class="space-y-6">
              <div class={`rounded-2xl border px-5 py-4 ${statusClasses(validationResult.status)}`}>
                <h3 class="text-sm font-semibold uppercase tracking-wide">Validation narrative</h3>
                <p class="mt-2 text-sm leading-6">{buildNarrative(validationResult)}</p>
                <dl class="mt-4 grid grid-cols-2 gap-3 text-sm">
                  <div class="rounded-xl bg-white/60 px-3 py-2">
                    <dt class="text-xs uppercase tracking-wide opacity-70">Errors</dt>
                    <dd class="mt-1 text-lg font-semibold">{validationResult.summary.errors}</dd>
                  </div>
                  <div class="rounded-xl bg-white/60 px-3 py-2">
                    <dt class="text-xs uppercase tracking-wide opacity-70">Warnings</dt>
                    <dd class="mt-1 text-lg font-semibold">{validationResult.summary.warnings}</dd>
                  </div>
                  <div class="rounded-xl bg-white/60 px-3 py-2">
                    <dt class="text-xs uppercase tracking-wide opacity-70">Tables checked</dt>
                    <dd class="mt-1 text-lg font-semibold">{validationResult.summary.tables_checked}</dd>
                  </div>
                  <div class="rounded-xl bg-white/60 px-3 py-2">
                    <dt class="text-xs uppercase tracking-wide opacity-70">Rows checked</dt>
                    <dd class="mt-1 text-lg font-semibold">{validationResult.summary.rows_checked}</dd>
                  </div>
                </dl>
              </div>

              <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <div class="flex items-center justify-between gap-4">
                  <div>
                    <h3 class="text-sm font-semibold text-slate-900">Issue summary</h3>
                    <p class="mt-1 text-xs text-slate-500">
                      Exact YAML lines with findings, so you can jump straight to the broken entries.
                    </p>
                  </div>
                  <span class="rounded-full border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700">
                    {issueSummary.length} line{issueSummary.length === 1 ? '' : 's'} affected
                  </span>
                </div>
                <div class="mt-3 space-y-2">
                  {issueSummary.length === 0 ? (
                    <div class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
                      No issue lines found in the current manifest.
                    </div>
                  ) : (
                    issueSummary.map((issue) => (
                      <div key={`${issue.lineNumber}-${issue.path}`} class="rounded-xl border border-red-200 bg-white px-4 py-3">
                        <div class="flex flex-wrap items-center gap-2 text-sm">
                          <span class="rounded-full bg-red-50 px-2 py-0.5 font-semibold text-red-700 ring-1 ring-red-200">
                            Line {issue.lineNumber}
                          </span>
                          <code class="text-xs text-slate-600">{issue.path}</code>
                        </div>
                        <div class="mt-2 space-y-1 text-sm text-slate-700">
                          {issue.messages.map((message) => (
                            <p key={message}>{message}</p>
                          ))}
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>

              <div>
                <h3 class="text-sm font-semibold text-slate-900">What the validator accepted</h3>
                <div class="mt-3 space-y-2">
                  {passedChecks.length === 0 ? (
                    <div class="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                      No top-level checks passed cleanly enough to summarize here.
                    </div>
                  ) : (
                    passedChecks.map((check) => (
                      <div key={check.label} class="rounded-xl bg-emerald-50 px-4 py-3 text-sm text-emerald-700 ring-1 ring-emerald-200">
                        {check.label}
                      </div>
                    ))
                  )}
                </div>
              </div>

              <div>
                <h3 class="text-sm font-semibold text-slate-900">Verbose findings</h3>
                <div class="mt-3 max-h-[42vh] space-y-3 overflow-auto pr-1">
                  {validationResult.findings.length === 0 ? (
                    <div class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
                      No findings returned. The candidate payload passed validation.
                    </div>
                  ) : (
                    validationResult.findings.map((finding, index) => (
                      <div key={`${finding.code}-${index}`} class={`rounded-xl px-4 py-3 text-sm ring-1 ${severityClasses(finding.severity)}`}>
                        <div class="flex flex-wrap items-center gap-2">
                          <span class="font-semibold uppercase">{finding.severity}</span>
                          <span class="font-mono text-xs">{finding.code}</span>
                          {finding.rule_id ? (
                            <span class="rounded-full bg-white/70 px-2 py-0.5 text-[11px]">{finding.rule_id}</span>
                          ) : null}
                        </div>
                        <p class="mt-2 leading-6">{finding.message}</p>
                        {findingLabel(finding) ? <p class="mt-2 text-xs opacity-80">{findingLabel(finding)}</p> : null}
                        {finding.hint ? <p class="mt-2 text-xs opacity-80">Hint: {finding.hint}</p> : null}
                      </div>
                    ))
                  )}
                </div>
              </div>
            </div>
          </div>
        )}
      </section>
    </div>
  );
}
