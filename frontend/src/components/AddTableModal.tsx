import { useState, useRef } from 'react';
import { X, Loader2, Plus } from 'lucide-react';
import type { Column } from '../types/spec';
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
  projectName: string;
  layer: 'bronze' | 'silver' | 'gold';
  onSuccess: () => void;
  onClose: () => void;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

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
const newId = () => `m-${++_uid}`;
const slug = (v: string) => v.replace(/\s/g, '_').toLowerCase();
const emptyDraft = (): DraftColumn => ({ id: newId(), name: '', data_type: 'string', nullable: true });

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

export default function AddTableModal({ projectName, layer: initialLayer, onSuccess, onClose }: Props) {
  const { loadTableList, switchTable } = useSpecStore();

  const [tableName, setTableName] = useState('');
  const [layer, setLayer]         = useState<'bronze' | 'silver' | 'gold'>(initialLayer);
  const [draftCols, setDraftCols] = useState<DraftColumn[]>([emptyDraft(), emptyDraft()]);

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitError, setSubmitError]   = useState<string | null>(null);
  const [errors, setErrors]             = useState<Record<string, string>>({});

  const lastNameRef = useRef<HTMLInputElement | null>(null);

  // --------------------------------------------------------------------------
  // Handlers
  // --------------------------------------------------------------------------

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
    if (!tableName.trim()) errs.tableName = 'Table name is required';
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

      await api.createTable(projectName, { name: tableName.trim(), layer, columns });
      await loadTableList(projectName);
      await switchTable(projectName, layer, tableName.trim());
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
    // Backdrop
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4"
      onClick={onClose}
    >
      {/* Card — stop propagation so clicks inside don't close */}
      <div
        className="w-full max-w-md rounded-xl bg-white p-6 shadow-xl"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="mb-5 flex items-center justify-between">
          <h2 className="text-lg font-semibold text-gray-800">Add Table</h2>
          <button
            onClick={onClose}
            className="rounded p-1 text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-600"
          >
            <X size={18} />
          </button>
        </div>

        <div className="space-y-4">

          {/* Table name */}
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Table Name</label>
            <input
              type="text"
              placeholder="table_name"
              value={tableName}
              autoFocus
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
                    'rounded-md px-4 py-1.5 text-sm font-medium capitalize transition-colors',
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

          {/* Column builder */}
          <div>
            <label className="mb-2 block text-sm font-medium text-gray-700">Columns</label>
            {errors.columns && (
              <p className="mb-2 text-xs text-red-500">{errors.columns}</p>
            )}
            <div className="space-y-2">
              {draftCols.map((col, idx) => {
                const isLast = idx === draftCols.length - 1;
                return (
                  <div key={col.id} className="flex items-center gap-2">
                    <input
                      type="text"
                      placeholder="column_name"
                      value={col.name}
                      ref={isLast ? lastNameRef : null}
                      onChange={e => updateCol(col.id, { name: slug(e.target.value) })}
                      className="min-w-0 flex-1 rounded-md border border-gray-300 px-2 py-1.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                    />
                    <select
                      value={col.data_type}
                      onChange={e =>
                        updateCol(col.id, { data_type: e.target.value as Column['data_type'] })
                      }
                      className="w-24 rounded-md border border-gray-300 px-1.5 py-1.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                    >
                      {DATA_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                    </select>
                    <label className="flex cursor-pointer select-none items-center gap-1 text-xs text-gray-600">
                      <input
                        type="checkbox"
                        checked={col.nullable}
                        onChange={e => updateCol(col.id, { nullable: e.target.checked })}
                        className="h-3.5 w-3.5 rounded"
                      />
                      null
                    </label>
                    {draftCols.length > 1 ? (
                      <button
                        type="button"
                        onClick={() => deleteColumn(col.id)}
                        className="rounded p-1 text-red-400 transition-colors hover:bg-red-50 hover:text-red-600"
                      >
                        <X size={13} />
                      </button>
                    ) : (
                      <span className="w-5" />
                    )}
                  </div>
                );
              })}
            </div>
            <button
              type="button"
              onClick={addColumn}
              className="mt-2 flex items-center gap-1.5 text-sm text-blue-600 hover:text-blue-800"
            >
              <Plus size={13} />
              Add Column
            </button>
          </div>
        </div>

        {/* Submit */}
        <div className="mt-6">
          <button
            type="button"
            disabled={isSubmitting}
            onClick={handleSubmit}
            className="flex w-full items-center justify-center gap-2 rounded-lg bg-blue-600 px-6 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-blue-700 disabled:opacity-60"
          >
            {isSubmitting && <Loader2 size={15} className="animate-spin" />}
            {isSubmitting ? 'Adding…' : 'Add Table'}
          </button>
          {submitError && (
            <p className="mt-2 rounded-md bg-red-50 px-3 py-2 text-sm text-red-600">
              {submitError}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
