from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from app.models.spec import Column


class CreateProjectRequest(BaseModel):
    name: str
    platform: str
    dialect: str
    catalog: str = ""
    schema_layer: str = ""


class CreateTableRequest(BaseModel):
    name: str
    layer: Literal["bronze", "silver", "gold"]
    columns: list[Column]


class UpdateTransformationsRequest(BaseModel):
    columns: list[Column]


class CompileRequest(BaseModel):
    project_name: str
    layer: str
    table_name: str
    target: Literal["sql", "pyspark", "dbt", "all"]
    dialect: str


class ExecuteRequest(BaseModel):
    query: str
    data: list[dict]
    table_name: str


class GenerateDataRequest(BaseModel):
    project_name: str
    layer: str
    table_name: str
    row_count: int = 50
    output_format: Literal["csv", "json", "ndjson", "xlsx", "parquet"] = "csv"
