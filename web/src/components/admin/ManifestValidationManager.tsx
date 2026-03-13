import { useMemo, useState } from 'preact/hooks';

import { api } from '../../lib/api';
import type { ValidationFinding, ValidationResult } from '../../lib/types';

type DatasetKind = 'tabular' | 'geojson';

const SUPPORTED_DATA_TYPES = [
  'boolean',
  'date',
  'dateTime',
  'enum',
  'float',
  'int',
  'regionID',
  'regionName',
  'string',
] as const;

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
      'Field declarations are validated without requiring uploaded data files on this screen',
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
    title: 'Platform rules',
    items: [
      'Unknown declared field types are validation errors',
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

function normalizeLineKey(text: string): string | null {
  const trimmed = text.trim();
  const keyMatch = trimmed.match(/^['"]?([^'":]+)['"]?\s*:/);
  return keyMatch ? keyMatch[1].trim() : null;
}

function findBestLineIndexForFinding(lines: ReviewLine[], finding: ValidationFinding): number {
  if (finding.line && finding.line >= 1 && finding.line <= lines.length) {
    return finding.line - 1;
  }

  const findingPath = finding.path;
  if (!findingPath) return -1;
  if (findingPath === 'manifest') return 0;

  const targetKey = findingPath.split('.').pop();
  let bestIndex = -1;
  let bestScore = -1;

  lines.forEach((line, index) => {
    const lineKey = normalizeLineKey(line.text);
    for (const linePath of line.paths) {
      let score = -1;
      if (findingPath === linePath) {
        score = 10000 + linePath.length;
      } else if (lineKey && targetKey && lineKey === targetKey && findingPath.endsWith(`.${lineKey}`)) {
        score = 9000 + linePath.length;
      } else if (findingPath.startsWith(`${linePath}.`)) {
        score = linePath.length;
      } else if (linePath.startsWith(`${findingPath}.`)) {
        score = findingPath.length;
      }

      if (score > bestScore) {
        bestScore = score;
        bestIndex = index;
      }
    }
  });

  return bestIndex;
}

function buildLineReviews(manifestText: string, findings: ValidationFinding[]): ReviewLine[] {
  const lines = manifestText.split(/\r?\n/);
  const stack: { indent: number; path: string }[] = [];
  const reviewLines = lines.map((text, index) => {
    const indent = text.match(/^(\s*)/)?.[1].length ?? 0;
    const key = normalizeLineKey(text);

    while (stack.length > 0 && indent <= stack[stack.length - 1].indent) {
      stack.pop();
    }

    const paths: string[] = [];
    if (key) {
      const parentPath = stack.length > 0 ? stack[stack.length - 1].path : '';
      const fullPath = parentPath ? `${parentPath}.${key}` : key;
      stack.push({ indent, path: fullPath });

      for (let i = stack.length - 1; i >= 0; i -= 1) {
        paths.push(stack[i].path);
      }
    }

    return {
      lineNumber: index + 1,
      text,
      findings: [] as ValidationFinding[],
      paths,
    };
  });

  for (const finding of findings) {
    const targetLineIndex = findBestLineIndexForFinding(reviewLines, finding);
    if (targetLineIndex >= 0) {
      reviewLines[targetLineIndex].findings.push(finding);
    }
  }

  return reviewLines;
}

function buildNarrative(result: ValidationResult) {
  if (result.status === 'pass') {
    return 'Validation completed successfully. The manifest parsed and the declared contract checks passed without errors or warnings.';
  }

  if (result.status === 'warn') {
    return `Validation completed with ${result.summary.warnings} warning${result.summary.warnings === 1 ? '' : 's'}. The manifest is structurally acceptable, but there are issues you should review before promotion.`;
  }

  return `Validation failed with ${result.summary.errors} error${result.summary.errors === 1 ? '' : 's'} and ${result.summary.warnings} warning${result.summary.warnings === 1 ? '' : 's'}. Fix the highlighted manifest paths first, then rerun validation to confirm the contract is clean.`;
}

function buildVerboseSuccessMessages(paths: string[], text: string): string[] {
  if (paths.length === 0) return [];

  const currentPath = paths[0];
  const rawValue = text.includes(':')
    ? text
        .split(':')
        .slice(1)
        .join(':')
        .trim()
        .replace(/^['"]|['"]$/g, '')
    : '';
  const messages: string[] = [];

  if (
    currentPath.endsWith('.type') &&
    SUPPORTED_DATA_TYPES.includes(rawValue as (typeof SUPPORTED_DATA_TYPES)[number])
  ) {
    messages.push(`'${rawValue}' is an allowed data type.`);
  }
  if (currentPath.endsWith('.format') && rawValue.includes('%')) {
    messages.push(`'${rawValue}' uses strftime directives, which is required for date and datetime fields.`);
  }
  if (
    currentPath.endsWith('.nullable') &&
    ['true', 'false'].includes(rawValue.toLowerCase())
  ) {
    messages.push(`'${rawValue.toLowerCase()}' is a valid nullability flag.`);
  }
  if (currentPath === 'datasetKind' && ['tabular', 'geojson'].includes(rawValue)) {
    messages.push(`'${rawValue}' is an allowed dataset kind.`);
  }
  if (currentPath === 'metadataSpecVersion' && rawValue) {
    messages.push(`Metadata spec version '${rawValue}' is present.`);
  }
  if (currentPath === 'datasetID' && rawValue) {
    messages.push(`Dataset ID '${rawValue}' is present for platform validation.`);
  }
  if (currentPath === 'datasetSlug' && rawValue) {
    messages.push(`Dataset slug '${rawValue}' is present and will be validated against the naming rules.`);
  }
  if (currentPath === 'collection.ID' && rawValue) {
    messages.push(`Collection ID '${rawValue}' is present and eligible for platform cross-checks.`);
  }
  if (currentPath === 'collection.name' && rawValue) {
    messages.push(`Collection name '${rawValue}' is present.`);
  }
  if (currentPath === 'category.ID' && rawValue) {
    messages.push(`Category ID '${rawValue}' is present and eligible for platform cross-checks.`);
  }
  if (currentPath === 'category.name' && rawValue) {
    messages.push(`Category name '${rawValue}' is present.`);
  }
  if (currentPath.endsWith('.enumRef') && rawValue) {
    messages.push(`Enum reference '${rawValue}' is syntactically valid and will be resolved against enumDefinitions.`);
  }

  return messages;
}

function findLineNumberForPath(
  lines: ReviewLine[],
  findingPath: string | null | undefined,
  explicitLine?: number | null
): number | null {
  if (explicitLine && explicitLine >= 1 && explicitLine <= lines.length) {
    return explicitLine;
  }
  if (!findingPath) return null;
  const exactLine = lines.find((line) => line.paths.includes(findingPath));
  if (exactLine) return exactLine.lineNumber;
  const nearestLine = lines.find((line) =>
    line.paths.some((path) => findingPath === path || findingPath.startsWith(`${path}.`))
  );
  return nearestLine ? nearestLine.lineNumber : null;
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

export default function ManifestValidationManager() {
  const [datasetKind, setDatasetKind] = useState<DatasetKind>('tabular');
  const [manifestFile, setManifestFile] = useState<File | null>(null);
  const [manifestText, setManifestText] = useState('');
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

    const findingPaths = new Set(
      validationResult.findings.map((finding) => finding.path).filter(Boolean)
    );
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
      (validationResult?.findings ?? []).map((finding, index) => ({
        id: `${finding.code}-${finding.path || 'manifest'}-${index}`,
        lineNumber: findLineNumberForPath(lineReviews, finding.path, finding.line),
        path: finding.path || 'manifest',
        message: finding.message,
      })),
    [lineReviews, validationResult]
  );

  const displayedLineReviews = useMemo(() => {
    if (!showIssueRowsOnly) return lineReviews;

    const visibleLineNumbers = new Set<number>();
    for (const line of lineReviews) {
      if (line.findings.length > 0) {
        for (
          let lineNumber = line.lineNumber - 2;
          lineNumber <= line.lineNumber + 2;
          lineNumber += 1
        ) {
          if (lineNumber >= 1 && lineNumber <= lineReviews.length) {
            visibleLineNumbers.add(lineNumber);
          }
        }
      }
    }

    return lineReviews.filter((line) => visibleLineNumbers.has(line.lineNumber));
  }, [lineReviews, showIssueRowsOnly]);

  const annotatedFindingCount = useMemo(
    () => lineReviews.reduce((count, line) => count + line.findings.length, 0),
    [lineReviews]
  );

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
              deepCheck,
            })
          : await api.adminValidateGeojson({
              manifestFile,
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
    <div class="space-y-5">
      <section class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
        <div>
          <h2 class="text-lg font-semibold text-slate-900">Manifest Validation</h2>
          <p class="mt-1 text-sm text-slate-600">
            Validate a candidate manifest before promoting it.
          </p>
        </div>

        <form class="mt-5 space-y-3" onSubmit={handleValidation}>
          <div class="flex flex-wrap items-end gap-3">
            <button
              type="button"
              onClick={() => setShowRules((value) => !value)}
              class="inline-flex h-11 items-center rounded-xl border border-slate-300 bg-slate-50 px-4 text-sm font-medium text-slate-800 transition hover:bg-white"
            >
              {showRules ? 'Hide rulebook' : 'Rules & allowed values'}
            </button>

            <div class="min-w-[22rem] flex-1 rounded-2xl border border-slate-200 bg-slate-50 p-3">
              <span class="mb-2 block text-xs font-medium uppercase tracking-wide text-slate-500">
                Candidate manifest
              </span>
              <div class="flex items-center gap-3">
                <input
                  type="file"
                  accept=".yaml,.yml"
                  onChange={(e) => {
                    const input = e.currentTarget as HTMLInputElement;
                    handleManifestChange(input.files?.[0] ?? null);
                  }}
                  class="block w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 file:mr-4 file:rounded-lg file:border-0 file:bg-slate-900 file:px-3 file:py-2 file:text-sm file:font-medium file:text-white"
                />
                {manifestFile ? (
                  <button
                    type="button"
                    onClick={() => handleManifestChange(null)}
                    class="rounded-lg border border-slate-300 px-3 py-2 text-sm text-slate-700 transition hover:bg-white"
                  >
                    Clear
                  </button>
                ) : null}
              </div>
              <div class="mt-2 truncate text-sm text-slate-600">
                {manifestFile?.name || 'No manifest selected'}
              </div>
            </div>

            <div class="min-w-[11rem] rounded-2xl border border-slate-200 bg-slate-50 p-3">
              <span class="mb-2 block text-xs font-medium uppercase tracking-wide text-slate-500">
                Dataset kind
              </span>
              <select
                value={datasetKind}
                onChange={(e) => setDatasetKind((e.currentTarget as HTMLSelectElement).value as DatasetKind)}
                class="w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-slate-500 focus:outline-none focus:ring-2 focus:ring-slate-200"
              >
                <option value="tabular">Tabular</option>
                <option value="geojson">GeoJSON</option>
              </select>
            </div>

            <div class="min-w-[21rem] rounded-2xl border border-slate-200 bg-slate-50 p-3">
              <div class="grid gap-2 sm:grid-cols-3">
                <label class="flex items-start gap-2 text-sm text-slate-700">
                  <input
                    type="checkbox"
                    checked={deepCheck}
                    onChange={(e) => setDeepCheck((e.currentTarget as HTMLInputElement).checked)}
                    class="mt-1 h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-400"
                  />
                  <span>Deep check</span>
                </label>
                <label class="flex items-start gap-2 text-sm text-slate-700">
                  <input
                    type="checkbox"
                    checked={showVerboseSuccess}
                    onChange={(e) => setShowVerboseSuccess((e.currentTarget as HTMLInputElement).checked)}
                    class="mt-1 h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-400"
                  />
                  <span>Verbose success</span>
                </label>
                <label class="flex items-start gap-2 text-sm text-slate-700">
                  <input
                    type="checkbox"
                    checked={showIssueRowsOnly}
                    onChange={(e) => setShowIssueRowsOnly((e.currentTarget as HTMLInputElement).checked)}
                    class="mt-1 h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-400"
                  />
                  <span>Issue rows only</span>
                </label>
              </div>
            </div>

            <div class="ml-auto">
              <button
                type="submit"
                disabled={!manifestFile || validating}
                class="h-11 rounded-xl bg-slate-900 px-5 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {validating ? 'Validating...' : 'Run Validation'}
              </button>
            </div>
          </div>

          {showRules ? (
            <div class="rounded-2xl border border-slate-200 bg-slate-50">
              <div class="border-b border-slate-200 px-4 py-3">
                <span class="block text-sm font-semibold text-slate-900">
                  Validation rules and allowed values
                </span>
                <span class="mt-1 block text-xs text-slate-500">
                  Supported data types, manifest constraints, and validation checks used on
                  this screen.
                </span>
              </div>
              <div class="grid gap-4 px-4 py-4 lg:grid-cols-2">
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
            </div>
          ) : null}

          {validationError ? (
            <div class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              {validationError}
            </div>
          ) : null}
        </form>
      </section>

      <section class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
        <div class="flex items-center justify-between">
          <div>
            <h2 class="text-lg font-semibold text-slate-900">Review</h2>
            <p class="mt-1 text-sm text-slate-600">
              The validator calls out what it accepted and what it rejected from the uploaded
              manifest.
            </p>
          </div>
          {validationResult ? (
            <span
              class={`rounded-full px-3 py-1 text-sm font-semibold ring-1 ${statusClasses(validationResult.status)}`}
            >
              {validationResult.status.toUpperCase()}
            </span>
          ) : null}
        </div>

        {!validationResult ? (
          <div class="mt-5 rounded-xl border border-dashed border-slate-300 px-4 py-8 text-sm text-slate-500">
            Upload a manifest and run validation to see line-by-line review and findings.
          </div>
        ) : (
          <div class="mt-5 grid gap-5 xl:grid-cols-[minmax(0,1.05fr)_minmax(24rem,0.95fr)]">
            <div>
              <div class="flex items-center justify-between">
                <h3 class="text-sm font-semibold text-slate-900">Annotated manifest</h3>
                <span class="text-xs text-slate-500">
                  {showIssueRowsOnly ? 'Issue rows plus nearby context' : 'Full deep-dive view'}
                </span>
              </div>
              <div class="mt-3 max-h-[62vh] overflow-auto rounded-2xl border border-slate-200">
                {displayedLineReviews.map((line) => (
                  <div
                    key={line.lineNumber}
                    class="grid grid-cols-[56px_1fr] border-b border-slate-100 last:border-b-0"
                  >
                    <div class="bg-slate-50 px-3 py-2 text-right font-mono text-xs text-slate-400">
                      {line.lineNumber}
                    </div>
                    <div class="px-3 py-2">
                      <pre class="overflow-x-auto whitespace-pre text-xs leading-6 text-slate-800">
                        {line.text || ' '}
                      </pre>
                      {line.paths.length > 0 ? (
                        <div class="mt-1 text-[10px] uppercase tracking-wide text-slate-400">
                          {line.paths[0]}
                        </div>
                      ) : null}
                      {line.findings.length > 0 ? (
                        <div class="mt-2 space-y-2">
                          {line.findings.map((finding, index) => (
                            <div
                              key={`${line.lineNumber}-${finding.code}-${index}`}
                              class={`rounded-lg px-3 py-2 text-[11px] ring-1 ${severityClasses(finding.severity)}`}
                            >
                              <div class="font-semibold uppercase">{finding.severity}</div>
                              <div class="mt-1">{finding.message}</div>
                            </div>
                          ))}
                        </div>
                      ) : null}
                      {showVerboseSuccess && line.text.trim() ? (
                        <div class="mt-2 space-y-2">
                          {buildVerboseSuccessMessages(line.paths, line.text).map((message) => (
                            <div
                              key={message}
                              class="rounded-lg bg-emerald-50 px-3 py-2 text-[11px] text-emerald-700 ring-1 ring-emerald-200"
                            >
                              {message}
                            </div>
                          ))}
                        </div>
                      ) : null}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div class="space-y-5">
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
                    <dt class="text-xs uppercase tracking-wide opacity-70">Findings</dt>
                    <dd class="mt-1 text-lg font-semibold">{validationResult.findings.length}</dd>
                  </div>
                  <div class="rounded-xl bg-white/60 px-3 py-2">
                    <dt class="text-xs uppercase tracking-wide opacity-70">Dataset kind</dt>
                    <dd class="mt-1 text-lg font-semibold capitalize">
                      {validationResult.dataset_kind}
                    </dd>
                  </div>
                </dl>
              </div>

              <div class="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <div class="flex items-center justify-between gap-4">
                  <div>
                    <h3 class="text-sm font-semibold text-slate-900">Issue summary</h3>
                    <p class="mt-1 text-xs text-slate-500">
                      Exact YAML lines with findings, so you can jump straight to the broken
                      entries.
                    </p>
                  </div>
                  <span class="rounded-full border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700">
                    {issueSummary.length} line{issueSummary.length === 1 ? '' : 's'} affected ·{' '}
                    {annotatedFindingCount} finding{annotatedFindingCount === 1 ? '' : 's'} annotated
                  </span>
                </div>
                <div class="mt-3 space-y-2">
                  {issueSummary.length === 0 ? (
                    <div class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
                      No issue lines found in the current manifest.
                    </div>
                  ) : (
                    issueSummary.map((issue) => (
                      <div key={issue.id} class="rounded-xl border border-red-200 bg-white px-4 py-3">
                        <div class="flex flex-wrap items-center gap-2 text-sm">
                          <span class="rounded-full bg-red-50 px-2 py-0.5 font-semibold text-red-700 ring-1 ring-red-200">
                            {issue.lineNumber ? `Line ${issue.lineNumber}` : 'Unmapped'}
                          </span>
                          <code class="text-xs text-slate-600">{issue.path}</code>
                        </div>
                        <p class="mt-2 text-sm text-slate-700">{issue.message}</p>
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
                      <div
                        key={check.label}
                        class="rounded-xl bg-emerald-50 px-4 py-3 text-sm text-emerald-700 ring-1 ring-emerald-200"
                      >
                        {check.label}
                      </div>
                    ))
                  )}
                </div>
              </div>

              <div>
                <h3 class="text-sm font-semibold text-slate-900">Verbose findings</h3>
                <div class="mt-3 max-h-[38vh] space-y-3 overflow-auto pr-1">
                  {validationResult.findings.length === 0 ? (
                    <div class="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
                      No findings returned. The candidate payload passed validation.
                    </div>
                  ) : (
                    validationResult.findings.map((finding, index) => (
                      <div
                        key={`${finding.code}-${index}`}
                        class={`rounded-xl px-4 py-3 text-sm ring-1 ${severityClasses(finding.severity)}`}
                      >
                        <div class="flex flex-wrap items-center gap-2">
                          <span class="font-semibold uppercase">{finding.severity}</span>
                          <span class="font-mono text-xs">{finding.code}</span>
                          {finding.rule_id ? (
                            <span class="rounded-full bg-white/70 px-2 py-0.5 text-[11px]">
                              {finding.rule_id}
                            </span>
                          ) : null}
                        </div>
                        <p class="mt-2 leading-6">{finding.message}</p>
                        {findingLabel(finding) ? (
                          <p class="mt-2 text-xs opacity-80">{findingLabel(finding)}</p>
                        ) : null}
                        {finding.hint ? (
                          <p class="mt-2 text-xs opacity-80">Hint: {finding.hint}</p>
                        ) : null}
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
