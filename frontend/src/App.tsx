import { useEffect, useState } from 'react';
import { Grid3x3, ClipboardCheck, Code2, CheckCircle, PlusCircle } from 'lucide-react';
import { useSpecStore } from './store/specStore';
import SchemaInput from './components/SchemaInput';
import TableSidebar from './components/TableSidebar';
import AddTableModal from './components/AddTableModal';
import ColumnGrid from './components/ColumnGrid';
import ReviewBoard from './components/ReviewBoard';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

type TabId = 1 | 2 | 3 | 4;
type Layer = 'bronze' | 'silver' | 'gold';

const TABS: {
  id: TabId;
  label: string;
  icon: React.ElementType;
  content: string;
}[] = [
  { id: 1, label: '1. Columns',  icon: Grid3x3,        content: 'Window 1 — Column Assignment Grid' },
  { id: 2, label: '2. Review',   icon: ClipboardCheck, content: 'Window 2 — Review Board' },
  { id: 3, label: '3. Notebook', icon: Code2,          content: 'Window 3 — Notebook Editor' },
  { id: 4, label: '4. Validate', icon: CheckCircle,    content: 'Window 4 — Validation Runner' },
];

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function App() {
  const [activeTab, setActiveTab]   = useState<TabId>(1);
  const [isCreatingNew, setIsCreatingNew] = useState(false);
  const [addTableModal, setAddTableModal] = useState<{ open: boolean; layer: Layer }>({
    open: false,
    layer: 'bronze',
  });

  const { currentProject, columns, setCurrentTable, setColumns, loadTableList } =
    useSpecStore();

  // Load table list whenever the active project changes
  useEffect(() => {
    if (currentProject) {
      loadTableList(currentProject.name);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentProject?.name]);

  // ── Gate: show SchemaInput when no project loaded or "New Project" clicked
  if (!currentProject || isCreatingNew) {
    return <SchemaInput onSuccess={() => setIsCreatingNew(false)} />;
  }

  const activeContent = TABS.find(t => t.id === activeTab)?.content ?? '';

  function handleNewProject() {
    setColumns([]);
    setCurrentTable('', '');
    setIsCreatingNew(true);
  }

  function closeAddTableModal() {
    setAddTableModal({ open: false, layer: 'bronze' });
  }

  return (
    <>
      <div className="flex h-screen w-screen overflow-hidden">

        {/* ── Sidebar ───────────────────────────────────────────────────── */}
        <aside className="flex w-60 flex-shrink-0 flex-col bg-gray-900 text-white">

          {/* App title */}
          <div className="border-b border-gray-700 px-4 py-3">
            <h1 className="text-base font-semibold tracking-tight">DataForge Studio</h1>
          </div>

          {/* Project + table tree — fills available space */}
          <TableSidebar
            projectName={currentProject.name}
            platform={currentProject.platform}
            onAddTable={(layer: Layer) => setAddTableModal({ open: true, layer })}
          />

          {/* Nav items */}
          <nav className="border-t border-gray-700 space-y-1 p-2">
            {TABS.map(({ id, label, icon: Icon }) => (
              <button
                key={id}
                onClick={() => setActiveTab(id)}
                className={[
                  'flex w-full items-center gap-3 rounded-md px-3 py-2 text-sm transition-colors',
                  activeTab === id
                    ? 'bg-blue-600 text-white'
                    : 'text-gray-300 hover:bg-gray-700 hover:text-white',
                ].join(' ')}
              >
                <Icon size={16} />
                {label}
              </button>
            ))}
          </nav>

          {/* New Project button */}
          <div className="border-t border-gray-700 px-3 py-3">
            <button
              onClick={handleNewProject}
              className="flex w-full items-center gap-2 rounded-md px-3 py-2 text-xs text-gray-400 transition-colors hover:bg-gray-700 hover:text-white"
            >
              <PlusCircle size={13} />
              New Project
            </button>
          </div>
        </aside>

        {/* ── Main content ──────────────────────────────────────────────── */}
        <main
          className={`flex flex-1 overflow-hidden bg-white ${
            (activeTab === 1 && columns.length > 0) || activeTab === 2
              ? 'flex-col'
              : 'items-center justify-center'
          }`}
        >
          {activeTab === 1 && columns.length > 0 ? (
            <ColumnGrid />
          ) : activeTab === 1 ? (
            <p className="text-lg text-gray-400">
              Select a table from the sidebar to begin
            </p>
          ) : activeTab === 2 ? (
            <ReviewBoard onTabChange={tab => setActiveTab(tab as TabId)} />
          ) : (
            <p className="text-lg text-gray-400">{activeContent}</p>
          )}
        </main>
      </div>

      {/* ── Add Table Modal (portal-style, rendered outside layout div) ── */}
      {addTableModal.open && (
        <AddTableModal
          projectName={currentProject.name}
          layer={addTableModal.layer}
          onSuccess={closeAddTableModal}
          onClose={closeAddTableModal}
        />
      )}
    </>
  );
}
