import { useState } from 'react';
import { AlertTriangle, CheckCircle, Minus } from 'lucide-react';
import { useSpecStore } from '../store/specStore';
import type { Column, ColumnTransformations } from '../types/spec';
import type { CompiledOutput } from '../store/specStore';
import * as api from '../utils/api';

// ─── Helpers ─────────────────────────────────────────────────────────────────

function hasAnyTransform(t: ColumnTransformations): boolean {
  return (
    t.trim ||
    t.case_normalization !== 'none' ||
    t.null_strategy !== 'none' ||
    !!t.type_cast ||
    t.strip_special_chars ||
    t.regex.enabled ||
    t.where_filter.enabled ||
    t.conditional.enabled ||
    t.delimiter_split.enabled ||
    !!t.custom_expression
  );
}

interface Pill {
  label: string;
  color: string;
}

function getTransformPills(col: Column): Pill[] {
  const t = col.transformations;
  const pills: Pill[] = [];
  if (t.trim)
    pills.push({ label: 'Trim', color: 'bg-blue-100 text-blue-700' });
  if (t.case_normalization !== 'none')
    pills.push({
      label: t.case_normalization === 'upper' ? 'UPPER' : t.case_normalization === 'lower' ? 'lower' : 'Title',
      color: 'bg-purple-100 text-purple-700',
    });
  if (t.null_strategy !== 'none')
    pills.push({ label: `Nulls: ${t.null_strategy}`, color: 'bg-orange-100 text-orange-700' });
  if (t.type_cast)
    pills.push({ label: `Cast: ${t.type_cast}`, color: 'bg-green-100 text-green-700' });
  if (t.strip_special_chars)
    pills.push({ label: 'Strip', color: 'bg-red-100 text-red-700' });
  if (t.regex.enabled)
    pills.push({ label: 'Regex', color: 'bg-yellow-100 text-yellow-700' });
  if (t.where_filter.enabled)
    pills.push({ label: 'Where', color: 'bg-indigo-100 text-indigo-700' });
  if (t.conditional.enabled)
    pills.push({ label: 'If/Else', color: 'bg-pink-100 text-pink-700' });
  if (t.delimiter_split.enabled)
    pills.push({ label: 'Split', color: 'bg-teal-100 text-teal-700' });
  return pills;
}

function getAutoFlags(col: Column): string[] {
  const t = col.transformations;
  const flags: string[] = [];
  const n = col.name.toLowerCase();

  if (col.data_type === 'string' && !t.trim)
    flags.push('String not trimmed');
  if ((col.data_type === 'date' || col.data_type === 'timestamp') && !t.type_cast)
    flags.push('Date not cast');
  if (col.nullable && t.null_strategy === 'none')
    flags.push('No null strategy');
  if ((n.includes('id') || n.includes('key')) && t.null_strategy === 'none')
    flags.push('ID column — consider dedup');
  if ((n.includes('email') || n.includes('phone')) && !t.regex.enabled)
    flags.push('Consider regex validation');

  return flags;
}

// ─── Spinner ─────────────────────────────────────────────────────────────────

function Spinner() {
  return (
    <svg className="h-4 w-4 animate-spin" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
    </svg>
  );
}

// ─── ReviewBoard ──────────────────────────────────────────────────────────────

export default function ReviewBoard({ onTabChange }: { onTabChange: (tab: number) => void }) {
  const { currentProject, currentTable, columns, notes, setCompiledOutput } = useSpecStore();

  const [isCompiling, setIsCompiling] = useState(false);
  const [isBuilding, setIsBuilding] = useState(false);
  const [compileError, setCompileError] = useState<string | null>(null);
  const [buildError, setBuildError] = useState<string | null>(null);

  if (!currentProject || !currentTable || columns.length === 0) {
    return (
      <div className="flex h-full items-center justify-center">
        <p className="text-lg text-gray-400">Select a table from the sidebar to begin</p>
      </div>
    );
  }

  const dialect = currentProject.dialect;

  // ── Summary counts ──────────────────────────────────────────────────────────
  let configuredCount = 0;
  let flagCount = 0;
  for (const col of columns) {
    if (hasAnyTransform(col.transformations)) configuredCount++;
    if (getAutoFlags(col).length > 0) flagCount++;
  }
  const passthroughCount = columns.length - configuredCount;

  // ── Action handlers ─────────────────────────────────────────────────────────

  async function handleCompileOnly() {
    setIsCompiling(true);
    setCompileError(null);
    try {
      const result = await api.compileAll(
        currentProject!.name,
        currentTable!.layer,
        currentTable!.name,
        dialect,
      );
      setCompiledOutput(result as CompiledOutput);
      onTabChange(3);
    } catch (err) {
      setCompileError(String(err));
    } finally {
      setIsCompiling(false);
    }
  }

  async function handleBuildAll() {
    setIsBuilding(true);
    setBuildError(null);
    const [compileResult, dataResult] = await Promise.allSettled([
      api.compileAll(currentProject!.name, currentTable!.layer, currentTable!.name, dialect),
      api.generateData(currentProject!.name, currentTable!.layer, currentTable!.name, 100, 'csv'),
    ]);
    setIsBuilding(false);

    if (compileResult.status === 'rejected' && dataResult.status === 'rejected') {
      setBuildError('Both code generation and data generation failed');
      return;
    }
    if (compileResult.status === 'rejected') {
      setBuildError(`Code generation failed: ${String(compileResult.reason)}`);
      return;
    }
    if (dataResult.status === 'rejected') {
      setBuildError(`Data generation failed: ${String(dataResult.reason)}`);
      return;
    }
    setCompiledOutput(compileResult.value as CompiledOutput);
    onTabChange(3);
  }

  const busy = isCompiling || isBuilding;

  // ── Render ──────────────────────────────────────────────────────────────────

  return (
    <div className="flex h-full flex-col overflow-hidden">

      {/* ── Summary table ─────────────────────────────────────────────────── */}
      <div className="flex-1 overflow-auto px-6 py-4">
        <div className="mb-4">
          <h2 className="text-lg font-semibold text-gray-900">Transformation Review</h2>
          <p className="mt-0.5 text-sm text-gray-500">
            Review all configured transforms before generating code and data
          </p>
        </div>

        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 z-10 bg-gray-50">
            <tr>
              {['Status', 'Column', 'Type', 'Transforms Configured', 'Auto-flags', 'Notes Preview'].map(h => (
                <th
                  key={h}
                  className="border-b border-gray-200 px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500 whitespace-nowrap"
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {columns.map(col => {
              const t = col.transformations;
              const colNotes = notes[col.name] ?? col.notes;
              const anyTransform = hasAnyTransform(t);
              const pills = getTransformPills(col);
              const flags = getAutoFlags(col);

              return (
                <tr key={col.name} className="hover:bg-gray-50">

                  {/* Status */}
                  <td className="px-3 py-3 text-center">
                    {anyTransform ? (
                      <CheckCircle size={16} className="mx-auto text-green-500" />
                    ) : colNotes ? (
                      <AlertTriangle size={16} className="mx-auto text-amber-500" />
                    ) : (
                      <Minus size={16} className="mx-auto text-gray-300" />
                    )}
                  </td>

                  {/* Column */}
                  <td className="px-3 py-3 whitespace-nowrap">
                    <div className="font-medium text-gray-900">{col.name}</div>
                    <span
                      className={`mt-0.5 inline-block rounded px-1 py-0.5 text-xs ${
                        col.nullable ? 'bg-blue-50 text-blue-600' : 'bg-red-50 text-red-600'
                      }`}
                    >
                      {col.nullable ? 'nullable' : 'required'}
                    </span>
                  </td>

                  {/* Type */}
                  <td className="px-3 py-3 whitespace-nowrap">
                    <code className="rounded bg-gray-100 px-1.5 py-0.5 text-xs text-gray-700">
                      {col.data_type}
                    </code>
                  </td>

                  {/* Transforms Configured */}
                  <td className="px-3 py-3">
                    {pills.length > 0 ? (
                      <div className="flex flex-wrap gap-1">
                        {pills.map((pill, i) => (
                          <span
                            key={i}
                            className={`rounded px-1.5 py-0.5 text-xs font-medium ${pill.color}`}
                          >
                            {pill.label}
                          </span>
                        ))}
                      </div>
                    ) : (
                      <span className="text-xs text-gray-400">No transforms — passthrough</span>
                    )}
                  </td>

                  {/* Auto-flags */}
                  <td className="px-3 py-3">
                    {flags.length > 0 ? (
                      <div className="space-y-0.5">
                        {flags.map((flag, i) => (
                          <div key={i} className="flex items-start gap-1">
                            <AlertTriangle
                              size={11}
                              className="mt-0.5 flex-shrink-0 text-amber-500"
                            />
                            <span className="text-xs text-amber-700">{flag}</span>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <span className="text-xs text-green-600">Looks good</span>
                    )}
                  </td>

                  {/* Notes Preview */}
                  <td className="px-3 py-3 max-w-xs">
                    {colNotes ? (
                      <span className="text-xs italic text-gray-400">
                        {colNotes.length > 60 ? `${colNotes.slice(0, 60)}…` : colNotes}
                      </span>
                    ) : (
                      <span className="text-xs text-gray-300">
                        No notes — AI will use column name and type only
                      </span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* ── Action bar ────────────────────────────────────────────────────── */}
      <div className="flex-shrink-0 border-t border-gray-200 bg-white px-6 py-4">

        {/* Inline error messages */}
        {compileError && (
          <div className="mb-3 flex items-start gap-2 rounded bg-red-50 px-3 py-2 text-xs text-red-700">
            <AlertTriangle size={13} className="mt-0.5 flex-shrink-0" />
            <span>Code generation failed: {compileError}</span>
          </div>
        )}
        {buildError && (
          <div className="mb-3 flex items-start gap-2 rounded bg-red-50 px-3 py-2 text-xs text-red-700">
            <AlertTriangle size={13} className="mt-0.5 flex-shrink-0" />
            <span>{buildError}</span>
          </div>
        )}

        <div className="flex items-center justify-between">

          {/* Summary counts */}
          <div className="flex items-center gap-5 text-sm">
            <span className="text-green-600 font-medium">
              {configuredCount} column{configuredCount !== 1 ? 's' : ''} configured
            </span>
            <span className="text-gray-400">
              {passthroughCount} passing through
            </span>
            {flagCount > 0 && (
              <span className="text-amber-500">
                {flagCount} auto-flag{flagCount !== 1 ? 's' : ''}
              </span>
            )}
          </div>

          {/* Buttons */}
          <div className="flex items-center gap-3">
            <button
              onClick={handleCompileOnly}
              disabled={busy}
              className="flex items-center gap-2 rounded border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 transition-colors hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {isCompiling ? (
                <>
                  <Spinner />
                  Generating code...
                </>
              ) : (
                'Generate Code Only'
              )}
            </button>

            <button
              onClick={handleBuildAll}
              disabled={busy}
              className="flex items-center gap-2 rounded bg-blue-600 px-5 py-2 text-sm font-semibold text-white transition-colors hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {isBuilding ? (
                <>
                  <Spinner />
                  Generating code + data...
                </>
              ) : (
                'Looks Good — Build It'
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
