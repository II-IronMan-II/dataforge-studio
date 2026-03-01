import { create } from 'zustand';
import type { Column, ColumnTransformations, ProjectConfig } from '../types/spec';
import * as api from '../utils/api';

interface SpecState {
  currentProject: ProjectConfig | null;
  currentTable: { name: string; layer: string } | null;
  columns: Column[];
  notes: Record<string, string>;
  tableList: Record<string, string[]>;
  isSaving: boolean;
  isLoading: boolean;
  error: string | null;

  // Actions
  setProject: (project: ProjectConfig) => void;
  setCurrentTable: (name: string, layer: string) => void;
  setColumns: (columns: Column[]) => void;
  updateColumnTransformation: (
    columnName: string,
    field: keyof ColumnTransformations,
    value: ColumnTransformations[keyof ColumnTransformations],
  ) => void;
  updateColumnNote: (columnName: string, note: string) => void;
  saveTransformations: () => Promise<void>;
  loadTableSpec: (projectName: string, layer: string, tableName: string) => Promise<void>;
  loadTableList: (projectName: string) => Promise<void>;
  switchTable: (projectName: string, layer: string, tableName: string) => Promise<void>;
}

export const useSpecStore = create<SpecState>()((set, get) => ({
  currentProject: null,
  currentTable: null,
  columns: [],
  notes: {},
  tableList: { bronze: [], silver: [], gold: [] },
  isSaving: false,
  isLoading: false,
  error: null,

  setProject(project) {
    set({ currentProject: project });
  },

  setCurrentTable(name, layer) {
    set({ currentTable: { name, layer } });
  },

  setColumns(columns) {
    set({ columns });
  },

  updateColumnTransformation(columnName, field, value) {
    set(state => ({
      columns: state.columns.map(col =>
        col.name === columnName
          ? { ...col, transformations: { ...col.transformations, [field]: value } }
          : col,
      ),
    }));
  },

  updateColumnNote(columnName, note) {
    set(state => ({
      notes: { ...state.notes, [columnName]: note },
    }));
  },

  async saveTransformations() {
    const { currentProject, currentTable, columns } = get();
    if (!currentProject || !currentTable) return;
    set({ isSaving: true, error: null });
    try {
      await api.saveTransformations(
        currentProject.name,
        currentTable.layer,
        currentTable.name,
        columns,
      );
      set({ isSaving: false });
    } catch (err) {
      set({ isSaving: false, error: String(err) });
    }
  },

  async loadTableSpec(projectName, layer, tableName) {
    set({ isLoading: true, error: null });
    try {
      const spec = await api.getTableSpec(projectName, layer, tableName);
      set({
        columns: spec.columns,
        notes: spec.notes,
        currentTable: { name: tableName, layer },
        isLoading: false,
      });
    } catch (err) {
      set({ isLoading: false, error: String(err) });
    }
  },

  async loadTableList(projectName) {
    try {
      const list = await api.listTables(projectName);
      set({ tableList: list });
    } catch (err) {
      set({ error: String(err) });
    }
  },

  async switchTable(projectName, layer, tableName) {
    set({ isLoading: true, error: null });
    try {
      const spec = await api.getTableSpec(projectName, layer, tableName);
      set({
        columns: spec.columns,
        notes: spec.notes,
        currentTable: { name: tableName, layer },
        isLoading: false,
      });
    } catch (err) {
      set({ isLoading: false, error: String(err) });
    }
  },
}));
