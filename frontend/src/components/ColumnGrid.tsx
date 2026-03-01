import { useState, useRef, useCallback, useEffect, Fragment } from 'react';
import { CheckCircle } from 'lucide-react';
import { useSpecStore } from '../store/specStore';
import type {
  ColumnTransformations,
  ConditionalCase,
  ConditionalTransform,
  DelimiterSplit,
  ProjectConfig,
  RegexTransform,
  WhereFilter,
} from '../types/spec';

// ─── Constants ───────────────────────────────────────────────────────────────

const DIALECTS: ProjectConfig['dialect'][] = [
  'snowflake_sql', 'spark_sql', 'bigquery_sql', 'tsql', 'mysql', 'postgresql', 'ansi',
];

const COL_COUNT = 13;

const LAYER_BADGE: Record<string, string> = {
  bronze: 'bg-amber-100 text-amber-800',
  silver: 'bg-slate-100 text-slate-700',
  gold:   'bg-yellow-100 text-yellow-800',
};

const NOTE_TAGS = ['[include nulls]', '[mixed formats]', '[invalid values]', '[duplicates]'];

type PanelType = 'regex' | 'where' | 'conditional' | 'delimiter';

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

// ─── RegexPanel ──────────────────────────────────────────────────────────────

function RegexPanel({
  regex,
  testInput,
  onTestInputChange,
  onChange,
}: {
  regex: RegexTransform;
  testInput: string;
  onTestInputChange: (v: string) => void;
  onChange: (rx: RegexTransform) => void;
}) {
  type TestResult =
    | { kind: 'match'; output: string }
    | { kind: 'nomatch' }
    | { kind: 'error' };

  let result: TestResult | null = null;
  if (testInput && regex.pattern) {
    try {
      const matched = new RegExp(regex.pattern).test(testInput);
      if (!matched) {
        result = { kind: 'nomatch' };
      } else {
        const output = testInput.replace(new RegExp(regex.pattern, 'g'), regex.replacement);
        result = { kind: 'match', output };
      }
    } catch {
      result = { kind: 'error' };
    }
  }

  return (
    <div className="space-y-2 max-w-xl">
      <label className="flex items-center gap-2 text-xs font-medium text-gray-700">
        <input
          type="checkbox"
          checked={regex.enabled}
          onChange={e => onChange({ ...regex, enabled: e.target.checked })}
          className="h-3.5 w-3.5"
        />
        Enable regex
      </label>
      <div className="grid grid-cols-2 gap-2">
        <div>
          <p className="mb-0.5 text-xs text-gray-500">Pattern</p>
          <input
            type="text"
            value={regex.pattern}
            onChange={e => onChange({ ...regex, pattern: e.target.value })}
            placeholder="[^a-zA-Z0-9]"
            className="w-full rounded border border-gray-300 px-2 py-1 text-xs font-mono"
          />
        </div>
        <div>
          <p className="mb-0.5 text-xs text-gray-500">Replacement</p>
          <input
            type="text"
            value={regex.replacement}
            onChange={e => onChange({ ...regex, replacement: e.target.value })}
            placeholder=""
            className="w-full rounded border border-gray-300 px-2 py-1 text-xs font-mono"
          />
        </div>
      </div>
      <div>
        <p className="mb-0.5 text-xs text-gray-500">Test input</p>
        <input
          type="text"
          value={testInput}
          onChange={e => onTestInputChange(e.target.value)}
          placeholder="Type to test live..."
          className="w-full rounded border border-gray-300 px-2 py-1 text-xs"
        />
        {result && (
          <p
            className={`mt-1 rounded px-2 py-0.5 text-xs ${
              result.kind === 'match'
                ? 'bg-green-50 text-green-700'
                : result.kind === 'nomatch'
                  ? 'bg-gray-100 text-gray-400'
                  : 'bg-red-50 text-red-600'
            }`}
          >
            {result.kind === 'match'
              ? `→ ${result.output}`
              : result.kind === 'nomatch'
                ? 'no match'
                : 'invalid regex'}
          </p>
        )}
      </div>
    </div>
  );
}

// ─── WherePanel ──────────────────────────────────────────────────────────────

function WherePanel({
  filter,
  onChange,
}: {
  filter: WhereFilter;
  onChange: (f: WhereFilter) => void;
}) {
  return (
    <div className="space-y-2 max-w-xl">
      <label className="flex items-center gap-2 text-xs font-medium text-gray-700">
        <input
          type="checkbox"
          checked={filter.enabled}
          onChange={e => onChange({ ...filter, enabled: e.target.checked })}
          className="h-3.5 w-3.5"
        />
        Enable where filter
      </label>
      <input
        type="text"
        value={filter.condition}
        onChange={e => onChange({ ...filter, condition: e.target.value })}
        placeholder="value IS NOT NULL AND LENGTH(value) > 2"
        className="w-full rounded border border-gray-300 px-2 py-1 text-xs font-mono"
      />
      <p className="text-xs text-gray-400">
        Column kept if condition is TRUE, set to NULL otherwise
      </p>
    </div>
  );
}

// ─── ConditionalPanel ────────────────────────────────────────────────────────

function ConditionalPanel({
  cond,
  onChange,
}: {
  cond: ConditionalTransform;
  onChange: (c: ConditionalTransform) => void;
}) {
  function addCase() {
    onChange({ ...cond, cases: [...cond.cases, { when: '', then: '' }] });
  }
  function removeCase(i: number) {
    onChange({ ...cond, cases: cond.cases.filter((_, idx) => idx !== i) });
  }
  function updateCase(i: number, field: keyof ConditionalCase, val: string) {
    const updated = cond.cases.map((c, idx) => (idx === i ? { ...c, [field]: val } : c));
    onChange({ ...cond, cases: updated });
  }

  return (
    <div className="space-y-2 max-w-2xl">
      <label className="flex items-center gap-2 text-xs font-medium text-gray-700">
        <input
          type="checkbox"
          checked={cond.enabled}
          onChange={e => onChange({ ...cond, enabled: e.target.checked })}
          className="h-3.5 w-3.5"
        />
        Enable CASE WHEN
      </label>
      <div className="space-y-1">
        {cond.cases.map((c, i) => (
          <div key={i} className="flex items-center gap-2">
            <span className="w-12 text-right text-xs text-gray-400">WHEN</span>
            <input
              type="text"
              value={c.when}
              onChange={e => updateCase(i, 'when', e.target.value)}
              placeholder="condition"
              className="flex-1 rounded border border-gray-300 px-2 py-0.5 text-xs font-mono"
            />
            <span className="w-10 text-right text-xs text-gray-400">THEN</span>
            <input
              type="text"
              value={c.then}
              onChange={e => updateCase(i, 'then', e.target.value)}
              placeholder="value"
              className="flex-1 rounded border border-gray-300 px-2 py-0.5 text-xs font-mono"
            />
            <button
              onClick={() => removeCase(i)}
              className="rounded px-1.5 py-0.5 text-xs text-red-400 hover:bg-red-50"
            >
              ×
            </button>
          </div>
        ))}
        <div className="flex items-center gap-2">
          <span className="w-12 text-right text-xs text-gray-400">ELSE</span>
          <input
            type="text"
            value={cond.else_value}
            onChange={e => onChange({ ...cond, else_value: e.target.value })}
            placeholder="default value"
            className="flex-1 rounded border border-gray-300 px-2 py-0.5 text-xs font-mono"
          />
          {/* spacers to align with WHEN rows */}
          <span className="w-10" />
          <span className="flex-1" />
          <span className="w-6" />
        </div>
      </div>
      <button
        onClick={addCase}
        className="rounded bg-blue-50 px-2 py-0.5 text-xs text-blue-600 hover:bg-blue-100"
      >
        + Add Case
      </button>
    </div>
  );
}

// ─── DelimiterPanel ──────────────────────────────────────────────────────────

function DelimiterPanel({
  split,
  onChange,
}: {
  split: DelimiterSplit;
  onChange: (d: DelimiterSplit) => void;
}) {
  return (
    <div className="space-y-2 max-w-xs">
      <label className="flex items-center gap-2 text-xs font-medium text-gray-700">
        <input
          type="checkbox"
          checked={split.enabled}
          onChange={e => onChange({ ...split, enabled: e.target.checked })}
          className="h-3.5 w-3.5"
        />
        Enable split
      </label>
      <div className="grid grid-cols-2 gap-2">
        <div>
          <p className="mb-0.5 text-xs text-gray-500">Delimiter</p>
          <input
            type="text"
            value={split.delimiter}
            onChange={e => onChange({ ...split, delimiter: e.target.value })}
            placeholder=","
            className="w-full rounded border border-gray-300 px-2 py-1 text-xs font-mono"
          />
        </div>
        <div>
          <p className="mb-0.5 text-xs text-gray-500">Index</p>
          <input
            type="number"
            value={split.index}
            min={0}
            onChange={e => onChange({ ...split, index: parseInt(e.target.value, 10) || 0 })}
            placeholder="0"
            className="w-full rounded border border-gray-300 px-2 py-1 text-xs"
          />
        </div>
      </div>
      <p className="text-xs text-gray-400">Takes part at index after split</p>
    </div>
  );
}

// ─── ColumnGrid ───────────────────────────────────────────────────────────────

export default function ColumnGrid() {
  const {
    currentProject,
    currentTable,
    columns,
    notes,
    isSaving,
    updateColumnTransformation,
    updateColumnNote,
    saveTransformations,
  } = useSpecStore();

  const [dialect, setDialect] = useState<ProjectConfig['dialect']>(
    currentProject?.dialect ?? 'ansi',
  );
  const [expanded, setExpanded] = useState<{ col: string; panel: PanelType } | null>(null);
  const [regexTests, setRegexTests] = useState<Record<string, string>>({});
  const [savedJustNow, setSavedJustNow] = useState(false);

  const prevIsSaving = useRef(false);
  const saveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const savedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Detect isSaving false-edge → show "Saved" badge for 2 s
  useEffect(() => {
    if (prevIsSaving.current && !isSaving) {
      if (savedTimerRef.current) clearTimeout(savedTimerRef.current);
      setSavedJustNow(true);
      savedTimerRef.current = setTimeout(() => setSavedJustNow(false), 2000);
    }
    prevIsSaving.current = isSaving;
  }, [isSaving]);

  // Sync dialect when project changes
  useEffect(() => {
    if (currentProject) setDialect(currentProject.dialect);
  }, [currentProject]);

  // Reset panels when table switches
  const tableName = currentTable?.name;
  useEffect(() => {
    setExpanded(null);
    setRegexTests({});
  }, [tableName]);

  const triggerSave = useCallback(() => {
    if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
    saveTimerRef.current = setTimeout(() => {
      saveTransformations();
    }, 800);
  }, [saveTransformations]);

  function update(
    colName: string,
    field: keyof ColumnTransformations,
    value: ColumnTransformations[keyof ColumnTransformations],
  ) {
    updateColumnTransformation(colName, field, value);
    triggerSave();
  }

  function updateNote(colName: string, value: string) {
    updateColumnNote(colName, value);
    triggerSave();
  }

  function togglePanel(colName: string, panel: PanelType) {
    setExpanded(prev =>
      prev?.col === colName && prev.panel === panel ? null : { col: colName, panel },
    );
  }

  if (!currentProject || !currentTable) return null;

  return (
    <div className="flex h-full w-full flex-col overflow-hidden">

      {/* ── Header ──────────────────────────────────────────────────────────── */}
      <div className="flex flex-shrink-0 items-center justify-between border-b border-gray-200 bg-white px-4 py-2">
        <div className="flex items-center gap-3">
          <span className="font-semibold text-gray-900">{currentTable.name}</span>
          <span
            className={`rounded px-2 py-0.5 text-xs font-medium ${
              LAYER_BADGE[currentTable.layer] ?? 'bg-gray-100 text-gray-600'
            }`}
          >
            {currentTable.layer}
          </span>
        </div>
        <div className="flex items-center gap-3">
          {isSaving && <span className="text-xs text-gray-400">Saving...</span>}
          {!isSaving && savedJustNow && (
            <span className="flex items-center gap-1 text-xs text-green-600">
              <CheckCircle size={12} />
              Saved
            </span>
          )}
          <select
            value={dialect}
            onChange={e => setDialect(e.target.value as ProjectConfig['dialect'])}
            className="rounded border border-gray-300 bg-white px-2 py-1 text-xs"
          >
            {DIALECTS.map(d => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>
        </div>
      </div>

      {/* ── Scrollable table ────────────────────────────────────────────────── */}
      <div className="flex-1 overflow-auto">
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 z-10 bg-gray-50">
            <tr>
              {['Status','Column','Type','Trim','Case','Nulls','Cast','Strip',
                'Regex','Where','Conditional','Delimiter','Notes'].map(h => (
                <th
                  key={h}
                  className="border-b border-gray-200 px-2 py-2 text-left text-xs font-medium uppercase tracking-wide text-gray-500 whitespace-nowrap"
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
              const statusDot = anyTransform
                ? 'bg-green-500'
                : colNotes
                  ? 'bg-amber-400'
                  : 'bg-gray-300';
              const isThisExpanded = expanded?.col === col.name;
              const activePanel = isThisExpanded ? expanded!.panel : null;

              return (
                <Fragment key={col.name}>

                  {/* ── Column row ─────────────────────────────────────────── */}
                  <tr className="hover:bg-gray-50">

                    {/* Status */}
                    <td className="px-3 py-2 text-center">
                      <span className={`inline-block h-2.5 w-2.5 rounded-full ${statusDot}`} />
                    </td>

                    {/* Column name + nullable badge */}
                    <td className="px-2 py-2 whitespace-nowrap">
                      <span className="font-medium text-gray-900">{col.name}</span>
                      <span
                        className={`ml-1.5 rounded px-1 py-0.5 text-xs ${
                          col.nullable
                            ? 'bg-blue-50 text-blue-600'
                            : 'bg-red-50 text-red-600'
                        }`}
                      >
                        {col.nullable ? 'nullable' : 'required'}
                      </span>
                    </td>

                    {/* Type */}
                    <td className="px-2 py-2 text-xs text-gray-500 whitespace-nowrap">
                      {col.data_type}
                    </td>

                    {/* Trim */}
                    <td className="px-2 py-2 text-center">
                      <input
                        type="checkbox"
                        checked={t.trim}
                        onChange={e => update(col.name, 'trim', e.target.checked)}
                        className="h-4 w-4 rounded border-gray-300 accent-blue-600"
                      />
                    </td>

                    {/* Case */}
                    <td className="px-2 py-2">
                      <select
                        value={t.case_normalization}
                        onChange={e => update(col.name, 'case_normalization', e.target.value)}
                        className="w-20 rounded border border-gray-200 bg-white px-1 py-0.5 text-xs"
                      >
                        <option value="none">none</option>
                        <option value="upper">UPPER</option>
                        <option value="lower">lower</option>
                        <option value="title">Title</option>
                      </select>
                    </td>

                    {/* Nulls */}
                    <td className="px-2 py-2">
                      <select
                        value={t.null_strategy}
                        onChange={e => update(col.name, 'null_strategy', e.target.value)}
                        className="w-20 rounded border border-gray-200 bg-white px-1 py-0.5 text-xs"
                      >
                        <option value="none">none</option>
                        <option value="drop">drop</option>
                        <option value="replace">replace</option>
                        <option value="flag">flag</option>
                      </select>
                      {t.null_strategy === 'replace' && (
                        <input
                          type="text"
                          value={t.null_replacement}
                          onChange={e => update(col.name, 'null_replacement', e.target.value)}
                          placeholder="value"
                          className="mt-1 w-20 rounded border border-gray-200 px-1 py-0.5 text-xs"
                        />
                      )}
                    </td>

                    {/* Cast */}
                    <td className="px-2 py-2">
                      <select
                        value={t.type_cast}
                        onChange={e => update(col.name, 'type_cast', e.target.value)}
                        className="w-24 rounded border border-gray-200 bg-white px-1 py-0.5 text-xs"
                      >
                        <option value="">none</option>
                        <option value="string">string</option>
                        <option value="integer">integer</option>
                        <option value="float">float</option>
                        <option value="date">date</option>
                        <option value="timestamp">timestamp</option>
                      </select>
                    </td>

                    {/* Strip */}
                    <td className="px-2 py-2 text-center">
                      <input
                        type="checkbox"
                        checked={t.strip_special_chars}
                        onChange={e => update(col.name, 'strip_special_chars', e.target.checked)}
                        className="h-4 w-4 rounded border-gray-300 accent-blue-600"
                      />
                    </td>

                    {/* Regex */}
                    <td className="px-2 py-2">
                      <button
                        onClick={() => togglePanel(col.name, 'regex')}
                        className={`rounded px-2 py-0.5 text-xs font-medium transition-colors ${
                          activePanel === 'regex' || t.regex.enabled
                            ? 'bg-blue-100 text-blue-700'
                            : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                        }`}
                      >
                        Regex
                      </button>
                    </td>

                    {/* Where */}
                    <td className="px-2 py-2">
                      <button
                        onClick={() => togglePanel(col.name, 'where')}
                        className={`rounded px-2 py-0.5 text-xs font-medium transition-colors ${
                          activePanel === 'where' || t.where_filter.enabled
                            ? 'bg-blue-100 text-blue-700'
                            : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                        }`}
                      >
                        Where
                      </button>
                    </td>

                    {/* Conditional */}
                    <td className="px-2 py-2">
                      <button
                        onClick={() => togglePanel(col.name, 'conditional')}
                        className={`rounded px-2 py-0.5 text-xs font-medium transition-colors ${
                          activePanel === 'conditional' || t.conditional.enabled
                            ? 'bg-blue-100 text-blue-700'
                            : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                        }`}
                      >
                        If/Else
                      </button>
                    </td>

                    {/* Delimiter */}
                    <td className="px-2 py-2">
                      <button
                        onClick={() => togglePanel(col.name, 'delimiter')}
                        className={`rounded px-2 py-0.5 text-xs font-medium transition-colors ${
                          activePanel === 'delimiter' || t.delimiter_split.enabled
                            ? 'bg-blue-100 text-blue-700'
                            : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                        }`}
                      >
                        Split
                      </button>
                    </td>

                    {/* Notes */}
                    <td className="px-2 py-2">
                      <textarea
                        rows={2}
                        value={colNotes}
                        onChange={e => updateNote(col.name, e.target.value)}
                        placeholder="Describe expected data for AI generation..."
                        className="w-52 resize-y rounded border border-gray-200 px-2 py-1 text-xs"
                      />
                      <div className="mt-1 flex flex-wrap gap-1">
                        {NOTE_TAGS.map(tag => (
                          <button
                            key={tag}
                            onClick={() =>
                              updateNote(col.name, colNotes ? `${colNotes} ${tag}` : tag)
                            }
                            className="rounded bg-gray-100 px-1 py-0.5 text-xs text-gray-500 hover:bg-gray-200"
                          >
                            {tag}
                          </button>
                        ))}
                      </div>
                    </td>
                  </tr>

                  {/* ── Expanded panel row ──────────────────────────────────── */}
                  {isThisExpanded && activePanel && (
                    <tr className="bg-blue-50/30">
                      <td colSpan={COL_COUNT} className="px-6 py-3">
                        {activePanel === 'regex' && (
                          <RegexPanel
                            regex={t.regex}
                            testInput={regexTests[col.name] ?? ''}
                            onTestInputChange={v =>
                              setRegexTests(prev => ({ ...prev, [col.name]: v }))
                            }
                            onChange={rx => update(col.name, 'regex', rx)}
                          />
                        )}
                        {activePanel === 'where' && (
                          <WherePanel
                            filter={t.where_filter}
                            onChange={f => update(col.name, 'where_filter', f)}
                          />
                        )}
                        {activePanel === 'conditional' && (
                          <ConditionalPanel
                            cond={t.conditional}
                            onChange={c => update(col.name, 'conditional', c)}
                          />
                        )}
                        {activePanel === 'delimiter' && (
                          <DelimiterPanel
                            split={t.delimiter_split}
                            onChange={d => update(col.name, 'delimiter_split', d)}
                          />
                        )}
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
