import { useState } from 'react';
import { ChevronDown, ChevronRight, Plus } from 'lucide-react';
import { useSpecStore } from '../store/specStore';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LAYERS = ['bronze', 'silver', 'gold'] as const;
type Layer = typeof LAYERS[number];

const LAYER_TEXT: Record<Layer, string> = {
  bronze: 'text-amber-400',
  silver: 'text-gray-300',
  gold:   'text-yellow-400',
};

const PLATFORM_BADGE: Record<string, string> = {
  databricks: 'bg-orange-500',
  snowflake:  'bg-sky-500',
  bigquery:   'bg-blue-600',
  synapse:    'bg-purple-600',
  dbt:        'bg-orange-600',
  generic:    'bg-gray-500',
};

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

interface Props {
  projectName: string;
  platform: string;
  onAddTable: (layer: Layer) => void;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function TableSidebar({ projectName, platform, onAddTable }: Props) {
  const { currentTable, columns, tableList, switchTable } = useSpecStore();

  // Bronze open by default, silver and gold closed
  const [openLayers, setOpenLayers] = useState<Set<Layer>>(new Set<Layer>(['bronze']));

  function toggleLayer(layer: Layer) {
    setOpenLayers(prev => {
      const next = new Set(prev);
      if (next.has(layer)) next.delete(layer);
      else next.add(layer);
      return next;
    });
  }

  // Green dot: shown only when the active table's columns are all validated
  const isValidated = columns.length > 0 && columns.every(c => c.validated);
  const badgeClass = PLATFORM_BADGE[platform] ?? 'bg-gray-500';

  return (
    <div className="flex flex-1 flex-col overflow-hidden">

      {/* ── Project name + platform badge ─────────────────────────────── */}
      <div className="border-b border-gray-700 px-3 py-3">
        <p className="truncate text-sm font-bold text-white">{projectName}</p>
        <span className={`mt-1.5 inline-block rounded px-1.5 py-0.5 text-xs font-medium text-white ${badgeClass}`}>
          {platform}
        </span>
      </div>

      {/* ── Scrollable table tree ──────────────────────────────────────── */}
      <div className="flex-1 overflow-y-auto py-1">
        {LAYERS.map(layer => {
          const tables: string[] = (tableList[layer] as string[] | undefined) ?? [];
          const isOpen = openLayers.has(layer);

          return (
            <div key={layer}>
              {/* Section header */}
              <button
                onClick={() => toggleLayer(layer)}
                className="flex w-full items-center gap-1.5 px-3 py-1.5 text-xs font-semibold uppercase tracking-wider transition-colors hover:bg-gray-800"
              >
                <span className={`flex items-center gap-1 ${LAYER_TEXT[layer]}`}>
                  {isOpen
                    ? <ChevronDown size={12} />
                    : <ChevronRight size={12} />
                  }
                  {layer}
                </span>
                <span className="ml-1 font-normal normal-case text-gray-500">
                  ({tables.length})
                </span>
              </button>

              {/* Table rows + Add Table */}
              {isOpen && (
                <div className="mb-1">
                  {tables.map(tableName => {
                    const isActive =
                      currentTable?.name === tableName &&
                      currentTable?.layer === layer;

                    return (
                      <button
                        key={tableName}
                        onClick={() => switchTable(projectName, layer, tableName)}
                        className={[
                          'flex w-full items-center gap-2 px-6 py-1.5 text-xs transition-colors',
                          isActive
                            ? 'bg-blue-600 text-white'
                            : 'text-gray-400 hover:bg-gray-800 hover:text-white',
                        ].join(' ')}
                      >
                        <span className="truncate">{tableName}</span>
                        {isActive && isValidated && (
                          <span className="ml-auto h-1.5 w-1.5 flex-shrink-0 rounded-full bg-green-400" />
                        )}
                      </button>
                    );
                  })}

                  {/* Add Table */}
                  <button
                    onClick={() => onAddTable(layer)}
                    className="flex w-full items-center gap-1.5 px-6 py-1.5 text-xs text-gray-500 transition-colors hover:text-gray-300"
                  >
                    <Plus size={11} />
                    Add table
                  </button>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
