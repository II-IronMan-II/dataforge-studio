import { useState } from 'react';
import { Grid3x3, ClipboardCheck, Code2, CheckCircle } from 'lucide-react';
import { useSpecStore } from './store/specStore';

type TabId = 1 | 2 | 3 | 4;

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

export default function App() {
  const [activeTab, setActiveTab] = useState<TabId>(1);
  const { currentProject, currentTable } = useSpecStore();

  const activeContent = TABS.find(t => t.id === activeTab)?.content ?? '';

  return (
    <div className="flex h-screen w-screen overflow-hidden">
      {/* Sidebar */}
      <aside className="flex w-60 flex-shrink-0 flex-col bg-gray-900 text-white">
        {/* App name */}
        <div className="border-b border-gray-700 p-4">
          <h1 className="text-base font-semibold tracking-tight">DataForge Studio</h1>
        </div>

        {/* Nav items */}
        <nav className="flex-1 space-y-1 p-2">
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

        {/* Current context */}
        <div className="border-t border-gray-700 p-3 text-xs text-gray-400">
          {currentProject ? (
            <p className="truncate">Project: {currentProject.name}</p>
          ) : (
            <p className="italic">No project selected</p>
          )}
          {currentTable && (
            <p className="truncate">
              Table: {currentTable.layer}/{currentTable.name}
            </p>
          )}
        </div>
      </aside>

      {/* Main content */}
      <main className="flex flex-1 items-center justify-center bg-white">
        <p className="text-lg text-gray-400">{activeContent}</p>
      </main>
    </div>
  );
}
