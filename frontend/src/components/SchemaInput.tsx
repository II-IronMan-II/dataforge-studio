import { useState, useRef } from 'react';
import { X, Loader2, Plus } from 'lucide-react';
import type { Column, ProjectConfig } from '../types/spec';
import { defaultColumn } from '../types/spec';
import { useSpecStore } from '../store/specStore';
import * as api from '../utils/api';

// ---------------------------------------------------------------------------
// Local types
// ---------------------------------------------------------------------------

interface DraftColumn {
  id: string;
  name: string;
  data_type: Column['data_type'];
  nullable: boolean;
}

interface Props {
  onSuccess: () => void;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PLATFORM_DEFAULTS: Record<ProjectConfig['platform'], ProjectConfig['dialect']> = {
  databricks: 'spark_sql',
  snowflake:  'snowflake_sql',
  bigquery:   'bigquery_sql',
  synapse:    'tsql',
  dbt:        'ansi',
  generic:    'ansi',
};

const PLATFORMS: ProjectConfig['platform'][] = [
  'databricks', 'snowflake', 'bigquery', 'synapse', 'dbt', 'generic',
];

const DIALECTS: ProjectConfig['dialect'][] = [
  'snowflake_sql', 'spark_sql', 'bigquery_sql', 'tsql', 'mysql', 'postgresql', 'ansi',
];

const DATA_TYPES: Column['data_type'][] = [
  'string', 'integer', 'float', 'boolean', 'date', 'timestamp', 'json',
];

const LAYER_ACTIVE: Record<string, string> = {
  bronze: 'bg-amber-500 text-white',
  silver: 'bg-gray-400 text-white',
  gold:   'bg-yellow-400 text-gray-900',
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let _uid = 0;
const newId = () => String(++_uid);
const slug = (v: string) => v.replace(/\s/g, '_').toLowerCase();

const emptyDraft = (): DraftColumn => ({
  id: newId(),
  name: '',
  data_type: 'string',
  nullable: true,
});

function extractError(err: unknown): string {
  if (err && typeof err === 'object') {
    const ax = err as { response?: { data?: { detail?: string } }; message?: string };
    if (ax.response?.data?.detail) return ax.response.data.detail;
    if (ax.message) return ax.message;
  }
  return String(err);
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function SchemaInput({ onSuccess }: Props) {
  const { setProject, setCurrentTable, setColumns } = useSpecStore();

  // Project fields
  const [projectName, setProjectName] = useState('');
  const [platform, setPlatform]       = useState<ProjectConfig['platform']>('databricks');
  const [dialect, setDialect]         = useState<ProjectConfig['dialect']>('spark_sql');
  const [catalog, setCatalog]         = useState('');

  // Table fields
  const [tableName, setTableName] = useState('');
  const [layer, setLayer]         = useState<'bronze' | 'silver' | 'gold'>('bronze');

  // Column rows
  const [draftCols, setDraftCols] = useState<DraftColumn[]>([
    emptyDraft(), emptyDraft(), emptyDraft(),
  ]);

  // UI state
  const [isSubmitting, setIsSubmitting]   = useState(false);
  const [submitError, setSubmitError]     = useState<string | null>(null);
  const [errors, setErrors]               = useState<Record<string, string>>({});

  const lastNameRef = useRef<HTMLInputElement | null>(null);

  // --------------------------------------------------------------------------
  // Handlers
  // --------------------------------------------------------------------------

  function handlePlatformChange(p: ProjectConfig['platform']) {
    setPlatform(p);
    setDialect(PLATFORM_DEFAULTS[p]);
  }

  function addColumn() {
    setDraftCols(prev => [...prev, emptyDraft()]);
    setTimeout(() => lastNameRef.current?.focus(), 0);
  }

  function deleteColumn(id: string) {
    setDraftCols(prev => prev.filter(c => c.id !== id));
  }

  function updateCol(id: string, patch: Partial<DraftColumn>) {
    setDraftCols(prev => prev.map(c => c.id === id ? { ...c, ...patch } : c));
  }

  function validate(): boolean {
    const errs: Record<string, string> = {};
    if (!projectName.trim()) errs.projectName = 'Project name is required';
    if (!tableName.trim())   errs.tableName   = 'Table name is required';
    if (!draftCols.some(c => c.name.trim()))
      errs.columns = 'At least one column must have a name';
    setErrors(errs);
    return Object.keys(errs).length === 0;
  }

  async function handleSubmit() {
    if (!validate()) return;
    setIsSubmitting(true);
    setSubmitError(null);

    try {
      const columns: Column[] = draftCols
        .filter(c => c.name.trim())
        .map(c => ({ ...defaultColumn(c.name.trim(), c.data_type), nullable: c.nullable }));

      const project = await api.createProject({
        name:         projectName.trim(),
        platform,
        dialect,
        catalog:      catalog.trim(),
        schema_layer: '',
      });

      const tableResp = await api.createTable(projectName.trim(), {
        name:    tableName.trim(),
        layer,
        columns,
      });

      setProject(project);
      setCurrentTable(tableName.trim(), layer);
      setColumns(tableResp.columns);
      onSuccess();
    } catch (err: unknown) {
      setSubmitError(extractError(err));
    } finally {
      setIsSubmitting(false);
    }
  }

  // --------------------------------------------------------------------------
  // Render
  // --------------------------------------------------------------------------

  return (
    <div className="flex min-h-screen items-start justify-center bg-gray-50 px-4 py-12">
      <div className="w-full max-w-2xl rounded-xl bg-white p-8 shadow-md">

        {/* ── Section 1: Project Settings ─────────────────────────────────── */}
        <h2 className="mb-6 text-xl font-semibold text-gray-800">New Project</h2>

        <div className="space-y-4">

          {/* Project name */}
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">
              Project Name
            </label>
            <input
              type="text"
              placeholder="my_pipeline"
              value={projectName}
              onChange={e => setProjectName(slug(e.target.value))}
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
            />
            {errors.projectName && (
              <p className="mt-1 text-xs text-red-500">{errors.projectName}</p>
            )}
          </div>

          {/* Platform */}
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Platform</label>
            <select
              value={platform}
              onChange={e => handlePlatformChange(e.target.value as ProjectConfig['platform'])}
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
            >
              {PLATFORMS.map(p => <option key={p} value={p}>{p}</option>)}
            </select>
          </div>

          {/* SQL Dialect */}
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">SQL Dialect</label>
            <select
              value={dialect}
              onChange={e => setDialect(e.target.value as ProjectConfig['dialect'])}
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
            >
              {DIALECTS.map(d => <option key={d} value={d}>{d}</option>)}
            </select>
          </div>

          {/* Catalog */}
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">
              Catalog / Database
            </label>
            <input
              type="text"
              placeholder="hive_metastore (optional)"
              value={catalog}
              onChange={e => setCatalog(e.target.value)}
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
            />
          </div>

          {/* Table name */}
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Table Name</label>
            <input
              type="text"
              placeholder="customers"
              value={tableName}
              onChange={e => setTableName(slug(e.target.value))}
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
            />
            {errors.tableName && (
              <p className="mt-1 text-xs text-red-500">{errors.tableName}</p>
            )}
          </div>

          {/* Layer toggle */}
          <div>
            <label className="mb-2 block text-sm font-medium text-gray-700">Layer</label>
            <div className="flex gap-2">
              {(['bronze', 'silver', 'gold'] as const).map(l => (
                <button
                  key={l}
                  type="button"
                  onClick={() => setLayer(l)}
                  className={[
                    'rounded-md px-5 py-2 text-sm font-medium capitalize transition-colors',
                    layer === l
                      ? LAYER_ACTIVE[l]
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200',
                  ].join(' ')}
                >
                  {l}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* ── Section 2: Columns ──────────────────────────────────────────── */}
        <div className="mt-8">
          <h2 className="text-xl font-semibold text-gray-800">Columns</h2>
          <p className="mb-4 mt-1 text-sm text-gray-500">Define your table schema</p>

          {errors.columns && (
            <p className="mb-3 text-xs text-red-500">{errors.columns}</p>
          )}

          <div className="space-y-2">
            {draftCols.map((col, idx) => {
              const isLast = idx === draftCols.length - 1;
              return (
                <div key={col.id} className="flex items-center gap-2">
                  {/* Column name */}
                  <input
                    type="text"
                    placeholder="column_name"
                    value={col.name}
                    ref={isLast ? lastNameRef : null}
                    onChange={e => updateCol(col.id, { name: slug(e.target.value) })}
                    className="min-w-0 flex-1 rounded-md border border-gray-300 px-3 py-1.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                  />

                  {/* Data type */}
                  <select
                    value={col.data_type}
                    onChange={e =>
                      updateCol(col.id, { data_type: e.target.value as Column['data_type'] })
                    }
                    className="w-28 rounded-md border border-gray-300 px-2 py-1.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                  >
                    {DATA_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                  </select>

                  {/* Nullable */}
                  <label className="flex cursor-pointer items-center gap-1 text-xs text-gray-600 select-none">
                    <input
                      type="checkbox"
                      checked={col.nullable}
                      onChange={e => updateCol(col.id, { nullable: e.target.checked })}
                      className="h-3.5 w-3.5 rounded"
                    />
                    null
                  </label>

                  {/* Delete — only shown when more than 1 column */}
                  {draftCols.length > 1 ? (
                    <button
                      type="button"
                      onClick={() => deleteColumn(col.id)}
                      className="rounded p-1 text-red-400 transition-colors hover:bg-red-50 hover:text-red-600"
                    >
                      <X size={14} />
                    </button>
                  ) : (
                    /* placeholder so columns don't jump when button appears */
                    <span className="w-6" />
                  )}
                </div>
              );
            })}
          </div>

          <button
            type="button"
            onClick={addColumn}
            className="mt-3 flex items-center gap-1.5 text-sm text-blue-600 hover:text-blue-800"
          >
            <Plus size={14} />
            Add Column
          </button>
        </div>

        {/* ── Submit ──────────────────────────────────────────────────────── */}
        <div className="mt-8">
          <button
            type="button"
            disabled={isSubmitting}
            onClick={handleSubmit}
            className="flex w-full items-center justify-center gap-2 rounded-lg bg-blue-600 px-6 py-3 text-sm font-semibold text-white transition-colors hover:bg-blue-700 disabled:opacity-60"
          >
            {isSubmitting && <Loader2 size={16} className="animate-spin" />}
            {isSubmitting ? 'Creating…' : 'Create Project & Load Schema'}
          </button>

          {submitError && (
            <p className="mt-3 rounded-md bg-red-50 px-4 py-2 text-sm text-red-600">
              {submitError}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
