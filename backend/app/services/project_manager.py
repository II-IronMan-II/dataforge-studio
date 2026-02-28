from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from app.models.requests import CreateProjectRequest, CreateTableRequest
from app.models.spec import Column, ProjectConfig


class ProjectManager:
    def __init__(self) -> None:
        projects_dir = os.getenv("PROJECTS_DIR", "./projects")
        self.projects_dir = Path(projects_dir)
        self.projects_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ paths

    def _project_path(self, name: str) -> Path:
        return self.projects_dir / f"{name}.tforge"

    def _table_path(self, project_name: str, layer: str, table_name: str) -> Path:
        return self._project_path(project_name) / layer / table_name

    # --------------------------------------------------------------- projects

    def create_project(self, req: CreateProjectRequest) -> dict:
        project_path = self._project_path(req.name)
        if project_path.exists():
            raise ValueError(f"Project '{req.name}' already exists")

        project_path.mkdir(parents=True)
        for layer in ("bronze", "silver", "gold"):
            (project_path / layer).mkdir()

        config = ProjectConfig(
            name=req.name,
            platform=req.platform,
            dialect=req.dialect,
            catalog=req.catalog,
            schema_layer=req.schema_layer,
            created_at=datetime.now(timezone.utc),
        )
        data = config.model_dump(mode="json")
        (project_path / "project.json").write_text(json.dumps(data, indent=2))
        return data

    def list_projects(self) -> list[dict]:
        projects = []
        for path in self.projects_dir.glob("*.tforge"):
            config_file = path / "project.json"
            if config_file.exists():
                projects.append(json.loads(config_file.read_text()))
        return projects

    def get_project(self, name: str) -> dict:
        config_file = self._project_path(name) / "project.json"
        if not config_file.exists():
            raise FileNotFoundError(f"Project '{name}' not found")
        return json.loads(config_file.read_text())

    # ----------------------------------------------------------------- tables

    def create_table(self, project_name: str, req: CreateTableRequest) -> dict:
        project_path = self._project_path(project_name)
        if not project_path.exists():
            raise FileNotFoundError(f"Project '{project_name}' not found")

        table_path = self._table_path(project_name, req.layer, req.name)
        table_path.mkdir(parents=True, exist_ok=True)

        schema = [col.model_dump(mode="json") for col in req.columns]
        (table_path / "schema.json").write_text(json.dumps(schema, indent=2))

        # Reset transformations to all-default for each column
        default_columns = [
            Column(name=col.name, data_type=col.data_type, nullable=col.nullable)
            .model_dump(mode="json")
            for col in req.columns
        ]
        (table_path / "transformations.json").write_text(
            json.dumps(default_columns, indent=2)
        )

        notes = {col.name: "" for col in req.columns}
        (table_path / "column_notes.json").write_text(json.dumps(notes, indent=2))

        return {
            "project": self.get_project(project_name),
            "table": {"name": req.name, "layer": req.layer},
            "columns": default_columns,
            "notes": notes,
        }

    def get_table_spec(
        self, project_name: str, layer: str, table_name: str
    ) -> dict:
        table_path = self._table_path(project_name, layer, table_name)
        columns = json.loads((table_path / "transformations.json").read_text())
        notes = json.loads((table_path / "column_notes.json").read_text())
        return {
            "project": self.get_project(project_name),
            "table": {"name": table_name, "layer": layer},
            "columns": columns,
            "notes": notes,
        }

    def save_transformations(
        self,
        project_name: str,
        layer: str,
        table_name: str,
        columns: list[dict],
    ) -> None:
        table_path = self._table_path(project_name, layer, table_name)
        (table_path / "transformations.json").write_text(
            json.dumps(columns, indent=2)
        )

    def save_column_notes(
        self,
        project_name: str,
        layer: str,
        table_name: str,
        notes: dict,
    ) -> None:
        table_path = self._table_path(project_name, layer, table_name)
        (table_path / "column_notes.json").write_text(json.dumps(notes, indent=2))

    def list_tables(self, project_name: str) -> dict:
        project_path = self._project_path(project_name)
        result: dict[str, list[str]] = {"bronze": [], "silver": [], "gold": []}
        for layer in result:
            layer_path = project_path / layer
            if layer_path.exists():
                result[layer] = [p.name for p in layer_path.iterdir() if p.is_dir()]
        return result

    # --------------------------------------------------------- synthetic data

    _FORMATS = ["csv", "json", "ndjson", "xlsx", "parquet"]

    def save_synthetic_data(
        self,
        project_name: str,
        layer: str,
        table_name: str,
        data: list[dict],
        format: str,
    ) -> None:
        if format not in self._FORMATS:
            raise ValueError(f"Unsupported format: {format}")
        table_path = self._table_path(project_name, layer, table_name)
        df = pd.DataFrame(data)
        out = table_path / f"synthetic_data.{format}"

        if format == "csv":
            df.to_csv(out, index=False)
        elif format == "json":
            df.to_json(out, orient="records", indent=2)
        elif format == "ndjson":
            df.to_json(out, orient="records", lines=True)
        elif format == "xlsx":
            df.to_excel(out, index=False)
        elif format == "parquet":
            df.to_parquet(out, index=False)

    def get_synthetic_data(
        self, project_name: str, layer: str, table_name: str
    ) -> dict:
        table_path = self._table_path(project_name, layer, table_name)
        for fmt in self._FORMATS:
            candidate = table_path / f"synthetic_data.{fmt}"
            if not candidate.exists():
                continue
            if fmt == "csv":
                df = pd.read_csv(candidate)
            elif fmt == "json":
                df = pd.read_json(candidate, orient="records")
            elif fmt == "ndjson":
                df = pd.read_json(candidate, lines=True)
            elif fmt == "xlsx":
                df = pd.read_excel(candidate)
            elif fmt == "parquet":
                df = pd.read_parquet(candidate)
            data = df.to_dict(orient="records")
            return {"data": data, "format": fmt, "row_count": len(data)}

        raise FileNotFoundError("No synthetic data found for this table")

    # ----------------------------------------------------- validation results

    def save_validation_results(
        self, project_name: str, layer: str, table_name: str, results: dict
    ) -> None:
        table_path = self._table_path(project_name, layer, table_name)
        (table_path / "validation_results.json").write_text(
            json.dumps(results, indent=2)
        )

    def get_validation_results(
        self, project_name: str, layer: str, table_name: str
    ) -> dict:
        table_path = self._table_path(project_name, layer, table_name)
        results_file = table_path / "validation_results.json"
        if not results_file.exists():
            raise FileNotFoundError("Validation has not been run for this table")
        return json.loads(results_file.read_text())
