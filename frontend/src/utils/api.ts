import axios from 'axios';
import type { Column, ProjectConfig, TableSpecResponse } from '../types/spec';

const client = axios.create({
  baseURL: 'http://localhost:8000',
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
});

// ---------------------------------------------------------------------------
// Projects
// ---------------------------------------------------------------------------

export function createProject(data: {
  name: string;
  platform: string;
  dialect: string;
  catalog?: string;
  schema_layer?: string;
}): Promise<ProjectConfig> {
  return client.post<ProjectConfig>('/api/projects/', data).then(r => r.data);
}

export function listProjects(): Promise<ProjectConfig[]> {
  return client.get<ProjectConfig[]>('/api/projects/').then(r => r.data);
}

export function getProject(name: string): Promise<ProjectConfig> {
  return client.get<ProjectConfig>(`/api/projects/${name}`).then(r => r.data);
}

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

export function createTable(
  projectName: string,
  data: { name: string; layer: string; columns: Column[] },
): Promise<TableSpecResponse> {
  return client
    .post<TableSpecResponse>(`/api/projects/${projectName}/tables`, data)
    .then(r => r.data);
}

export function listTables(projectName: string): Promise<Record<string, string[]>> {
  return client
    .get<Record<string, string[]>>(`/api/projects/${projectName}/tables`)
    .then(r => r.data);
}

export function getTableSpec(
  projectName: string,
  layer: string,
  tableName: string,
): Promise<TableSpecResponse> {
  return client
    .get<TableSpecResponse>(`/api/projects/${projectName}/tables/${layer}/${tableName}`)
    .then(r => r.data);
}

export function saveTransformations(
  projectName: string,
  layer: string,
  tableName: string,
  columns: Column[],
): Promise<TableSpecResponse> {
  return client
    .put<TableSpecResponse>(
      `/api/projects/${projectName}/tables/${layer}/${tableName}/transformations`,
      { columns },
    )
    .then(r => r.data);
}

export function saveNotes(
  projectName: string,
  layer: string,
  tableName: string,
  notes: Record<string, string>,
): Promise<unknown> {
  return client
    .put(`/api/projects/${projectName}/tables/${layer}/${tableName}/notes`, { notes })
    .then(r => r.data);
}

// ---------------------------------------------------------------------------
// Compile
// ---------------------------------------------------------------------------

export function compileAll(
  projectName: string,
  layer: string,
  tableName: string,
  dialect: string,
): Promise<unknown> {
  return client
    .post('/api/compile/all', {
      project_name: projectName,
      layer,
      table_name: tableName,
      target: 'all',
      dialect,
    })
    .then(r => r.data);
}

// ---------------------------------------------------------------------------
// Execute
// ---------------------------------------------------------------------------

export function validateTable(
  projectName: string,
  layer: string,
  tableName: string,
  compiledSql: string,
): Promise<unknown> {
  return client
    .post('/api/execute/validate', {
      project_name: projectName,
      layer,
      table_name: tableName,
      compiled_sql: compiledSql,
    })
    .then(r => r.data);
}

// ---------------------------------------------------------------------------
// LLM
// ---------------------------------------------------------------------------

export function getLLMStatus(): Promise<unknown> {
  return client.get('/api/llm/status').then(r => r.data);
}

export function generateData(
  projectName: string,
  layer: string,
  tableName: string,
  rowCount: number,
  outputFormat: string,
): Promise<unknown> {
  return client
    .post('/api/llm/generate-data', {
      project_name: projectName,
      layer,
      table_name: tableName,
      row_count: rowCount,
      output_format: outputFormat,
    })
    .then(r => r.data);
}
