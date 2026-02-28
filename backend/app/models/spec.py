from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class RegexTransform(BaseModel):
    enabled: bool = False
    pattern: str = ""
    replacement: str = ""
    dialect: str = "python"


class WhereFilter(BaseModel):
    enabled: bool = False
    condition: str = ""
    dialect: str = "ansi"


class ConditionalCase(BaseModel):
    when: str
    then: str


class ConditionalTransform(BaseModel):
    enabled: bool = False
    cases: list[ConditionalCase] = []
    else_value: str = ""


class DelimiterSplit(BaseModel):
    enabled: bool = False
    delimiter: str = ""
    index: int = 0


class ColumnTransformations(BaseModel):
    trim: bool = False
    case_normalization: Literal["none", "upper", "lower", "title"] = "none"
    null_strategy: Literal["none", "drop", "replace", "flag"] = "none"
    null_replacement: str = ""
    type_cast: str = ""
    strip_special_chars: bool = False
    regex: RegexTransform = RegexTransform()
    where_filter: WhereFilter = WhereFilter()
    conditional: ConditionalTransform = ConditionalTransform()
    delimiter_split: DelimiterSplit = DelimiterSplit()
    custom_expression: str = ""


class Column(BaseModel):
    name: str
    data_type: Literal["string", "integer", "float", "boolean", "date", "timestamp", "json"]
    nullable: bool = True
    notes: str = ""
    validated: bool = False
    transformations: ColumnTransformations = ColumnTransformations()


class TableSpec(BaseModel):
    name: str
    layer: Literal["bronze", "silver", "gold"]
    columns: list[Column]


class ProjectConfig(BaseModel):
    name: str
    platform: Literal["databricks", "snowflake", "bigquery", "synapse", "dbt", "generic"]
    dialect: Literal["snowflake_sql", "spark_sql", "bigquery_sql", "tsql", "mysql", "postgresql", "ansi"]
    catalog: str = ""
    schema_layer: str = ""
    created_at: datetime
