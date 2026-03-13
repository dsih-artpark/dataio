import { useMemo, useState } from 'preact/hooks';

import { api } from '../../lib/api';
import type { ValidationFinding, ValidationResult } from '../../lib/types';

type DatasetKind = 'tabular' | 'geojson';

interface ReviewLine {
  lineNumber: number;
  text: string;
  findings: ValidationFinding[];
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
  return lines.map((text, index) => {
    const trimmed = text.trim();
    const candidateKey = trimmed.match(/^([A-Za-z0-9._-]+):/)?.[1];
    const matchingFindings = findings.filter((finding) => {
      if (!finding.path) return false;
      if (candidateKey && (finding.path === candidateKey || finding.path.endsWith(`.${candidateKey}`))) {
        return true;
      }
      if (finding.path === 'manifest' && index === 0) {
        return true;
      }
      return false;
    });

    return {
      lineNumber: index + 1,
      text,
      findings: matchingFindings,
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

export default function ManifestValidationManager() {
  const [datasetKind, setDatasetKind] = useState<DatasetKind>('tabular');
  const [manifestFile, setManifestFile] = useState<File | null>(null);
  const [manifestText, setManifestText] = useState('');
  const [dataFile, setDataFile] = useState<File | null>(null);
  const [tableName, setTableName] = useState('');
  const [deepCheck, setDeepCheck] = useState(true);
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
    <div class="grid gap-6 xl:grid-cols-[0.9fr_1.1fr]">
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
          <div class="grid gap-4 md:grid-cols-2">
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
          </div>

          <label class="block">
            <span class="mb-2 block text-sm font-medium text-slate-700">Candidate manifest</span>
            <input
              type="file"
              accept=".yaml,.yml"
              onChange={(e) => {
                const input = e.currentTarget as HTMLInputElement;
                handleManifestChange(input.files?.[0] ?? null);
              }}
              class="block w-full rounded-xl border border-slate-300 px-3 py-2 text-sm text-slate-700 file:mr-4 file:rounded-lg file:border-0 file:bg-slate-900 file:px-3 file:py-2 file:text-sm file:font-medium file:text-white"
            />
          </label>
          {manifestFile ? (
            <div class="flex items-center justify-between rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm">
              <span class="text-slate-700">{manifestFile.name}</span>
              <button
                type="button"
                onClick={() => handleManifestChange(null)}
                class="rounded-lg border border-slate-300 px-3 py-1.5 text-slate-700 transition hover:bg-white"
              >
                Remove
              </button>
            </div>
          ) : null}

          <label class="block">
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
              class="block w-full rounded-xl border border-slate-300 px-3 py-2 text-sm text-slate-700 file:mr-4 file:rounded-lg file:border-0 file:bg-slate-200 file:px-3 file:py-2 file:text-sm file:font-medium file:text-slate-900"
            />
          </label>
          {dataFile ? (
            <div class="flex items-center justify-between rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm">
              <span class="text-slate-700">{dataFile.name}</span>
              <button
                type="button"
                onClick={() => setDataFile(null)}
                class="rounded-lg border border-slate-300 px-3 py-1.5 text-slate-700 transition hover:bg-white"
              >
                Remove
              </button>
            </div>
          ) : null}

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
          ) : null}

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
          <div class="mt-6 space-y-6">
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
              <h3 class="text-sm font-semibold text-slate-900">Findings</h3>
              <div class="mt-3 space-y-3">
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
                      <p class="mt-2">{finding.message}</p>
                      {findingLabel(finding) ? <p class="mt-1 text-xs opacity-80">{findingLabel(finding)}</p> : null}
                      {finding.hint ? <p class="mt-2 text-xs opacity-80">Hint: {finding.hint}</p> : null}
                    </div>
                  ))
                )}
              </div>
            </div>

            <div>
              <h3 class="text-sm font-semibold text-slate-900">Line-by-line manifest review</h3>
              <div class="mt-3 max-h-[32rem] overflow-auto rounded-2xl border border-slate-200">
                {lineReviews.map((line) => (
                  <div key={line.lineNumber} class="grid grid-cols-[56px_1fr] border-b border-slate-100 last:border-b-0">
                    <div class="bg-slate-50 px-3 py-2 text-right font-mono text-xs text-slate-400">{line.lineNumber}</div>
                    <div class="px-3 py-2">
                      <pre class="whitespace-pre-wrap break-all text-xs leading-6 text-slate-800">{line.text || ' '}</pre>
                      {line.findings.length === 0 ? (
                        line.text.trim() ? (
                          <div class="mt-1 text-[11px] text-emerald-700">No issue detected for this line.</div>
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
          </div>
        )}
      </section>
    </div>
  );
}
