// Mirrors backend/app/models/spec.py exactly.

export interface RegexTransform {
  enabled: boolean;
  pattern: string;
  replacement: string;
  dialect: string;
}

export interface WhereFilter {
  enabled: boolean;
  condition: string;
  dialect: string;
}

export interface ConditionalCase {
  when: string;
  then: string;
}

export interface ConditionalTransform {
  enabled: boolean;
  cases: ConditionalCase[];
  else_value: string;
}

export interface DelimiterSplit {
  enabled: boolean;
  delimiter: string;
  index: number;
}

export interface ColumnTransformations {
  trim: boolean;
  case_normalization: 'none' | 'upper' | 'lower' | 'title';
  null_strategy: 'none' | 'drop' | 'replace' | 'flag';
  null_replacement: string;
  type_cast: string;
  strip_special_chars: boolean;
  regex: RegexTransform;
  where_filter: WhereFilter;
  conditional: ConditionalTransform;
  delimiter_split: DelimiterSplit;
  custom_expression: string;
}

export interface Column {
  name: string;
  data_type: 'string' | 'integer' | 'float' | 'boolean' | 'date' | 'timestamp' | 'json';
  nullable: boolean;
  notes: string;
  validated: boolean;
  transformations: ColumnTransformations;
}

export interface TableSpec {
  name: string;
  layer: 'bronze' | 'silver' | 'gold';
  columns: Column[];
}

export interface ProjectConfig {
  name: string;
  platform: 'databricks' | 'snowflake' | 'bigquery' | 'synapse' | 'dbt' | 'generic';
  dialect: 'snowflake_sql' | 'spark_sql' | 'bigquery_sql' | 'tsql' | 'mysql' | 'postgresql' | 'ansi';
  catalog: string;
  schema_layer: string;
  created_at: string;
}

export interface TableSpecResponse {
  project: ProjectConfig;
  table: { name: string; layer: string };
  columns: Column[];
  notes: Record<string, string>;
}

// ---------------------------------------------------------------------------
// Default factory functions — match Pydantic field defaults from spec.py
// ---------------------------------------------------------------------------

export function defaultRegexTransform(): RegexTransform {
  return { enabled: false, pattern: '', replacement: '', dialect: 'python' };
}

export function defaultWhereFilter(): WhereFilter {
  return { enabled: false, condition: '', dialect: 'ansi' };
}

export function defaultConditionalTransform(): ConditionalTransform {
  return { enabled: false, cases: [], else_value: '' };
}

export function defaultDelimiterSplit(): DelimiterSplit {
  return { enabled: false, delimiter: '', index: 0 };
}

export function defaultColumnTransformations(): ColumnTransformations {
  return {
    trim: false,
    case_normalization: 'none',
    null_strategy: 'none',
    null_replacement: '',
    type_cast: '',
    strip_special_chars: false,
    regex: defaultRegexTransform(),
    where_filter: defaultWhereFilter(),
    conditional: defaultConditionalTransform(),
    delimiter_split: defaultDelimiterSplit(),
    custom_expression: '',
  };
}

export function defaultColumn(name: string, data_type: Column['data_type']): Column {
  return {
    name,
    data_type,
    nullable: true,
    notes: '',
    validated: false,
    transformations: defaultColumnTransformations(),
  };
}
