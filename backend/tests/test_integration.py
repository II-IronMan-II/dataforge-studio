"""
End-to-end integration test for DataForge Studio backend.

Uses a real DuckDB executor, real file system (tmp_path), and real compiler.
Only the LLM provider is never invoked — LLM_PROVIDER=phi3 is set but the
model is lazy-loaded only on generate_synthetic_data(), which is not called
here (data is seeded directly via ProjectManager in Step 5).
"""
from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.project_manager import ProjectManager

# ---------------------------------------------------------------------------
# Column definitions for project creation
# ---------------------------------------------------------------------------

_COLUMNS = [
    {"name": "customer_id",   "data_type": "integer", "nullable": False},
    {"name": "customer_name", "data_type": "string",  "nullable": True},
    {"name": "phone",         "data_type": "string",  "nullable": True},
    {"name": "email",         "data_type": "string",  "nullable": True},
    {"name": "signup_date",   "data_type": "date",    "nullable": True},
]

# ---------------------------------------------------------------------------
# Full column objects with transformations for the PUT step
# ---------------------------------------------------------------------------

_TRANSFORMS = [
    {
        "name": "customer_id",
        "data_type": "integer",
        "nullable": False,
        "transformations": {},
    },
    {
        "name": "customer_name",
        "data_type": "string",
        "nullable": True,
        "transformations": {
            "trim": True,
            "case_normalization": "lower",
        },
    },
    {
        "name": "phone",
        "data_type": "string",
        "nullable": True,
        "transformations": {
            "trim": True,
            "regex": {
                "enabled": True,
                "pattern": "[^0-9+]",
                "replacement": "",
                "dialect": "spark_sql",
            },
        },
    },
    {
        "name": "email",
        "data_type": "string",
        "nullable": True,
        "transformations": {
            "trim": True,
            "null_strategy": "replace",
            "null_replacement": "UNKNOWN@UNKNOWN.COM",
        },
    },
    {
        "name": "signup_date",
        "data_type": "date",
        "nullable": True,
        "transformations": {
            "type_cast": "date",
        },
    },
]


def _make_synthetic_rows() -> list[dict]:
    """20 rows: 5 names with spaces, 4 phones with dashes, 3 None emails."""
    rows = []
    for i in range(1, 21):
        rows.append({
            "customer_id":   i,
            "customer_name": f"  Customer {i}  " if i <= 5 else f"Customer {i}",
            "phone":         "98-765-4321" if i <= 4 else f"9876543{i:02d}",
            "email":         None if i <= 3 else f"customer{i}@example.com",
            "signup_date":   "2024-01-15",
        })
    return rows


# ---------------------------------------------------------------------------
# Integration test
# ---------------------------------------------------------------------------

def test_full_pipeline(tmp_path, monkeypatch):
    # Isolate project storage to a fresh tmp directory for this test run.
    monkeypatch.setenv("PROJECTS_DIR", str(tmp_path))
    # Keep the LLM provider as phi3 (lazy); it is never loaded in this test.
    monkeypatch.setenv("LLM_PROVIDER", "phi3")

    with TestClient(app) as client:

        # ------------------------------------------------------------------ #
        # Step 1 — Create project                                             #
        # ------------------------------------------------------------------ #
        r = client.post("/api/projects/", json={
            "name":         "test_bank",
            "platform":     "databricks",
            "dialect":      "spark_sql",
            "catalog":      "hive_metastore",
            "schema_layer": "bronze",
        })
        assert r.status_code == 201, r.text

        tforge = tmp_path / "test_bank.tforge"
        assert tforge.is_dir(), ".tforge folder must exist on disk"

        # ------------------------------------------------------------------ #
        # Step 2 — Create table                                               #
        # ------------------------------------------------------------------ #
        r = client.post("/api/projects/test_bank/tables", json={
            "name":    "customers",
            "layer":   "bronze",
            "columns": _COLUMNS,
        })
        assert r.status_code == 201, r.text

        table_path = tforge / "bronze" / "customers"
        assert (table_path / "schema.json").exists(),          "schema.json must exist"
        assert (table_path / "transformations.json").exists(), "transformations.json must exist"
        assert (table_path / "column_notes.json").exists(),    "column_notes.json must exist"

        # ------------------------------------------------------------------ #
        # Step 3 — Update transformations                                     #
        # ------------------------------------------------------------------ #
        r = client.put(
            "/api/projects/test_bank/tables/bronze/customers/transformations",
            json={"columns": _TRANSFORMS},
        )
        assert r.status_code == 200, r.text

        # Verify disk: read transformations.json directly
        saved = json.loads((table_path / "transformations.json").read_text())
        customer_name_col = next(c for c in saved if c["name"] == "customer_name")
        assert customer_name_col["transformations"]["trim"] is True, \
            "customer_name trim must be persisted as true"

        # ------------------------------------------------------------------ #
        # Step 4 — Compile all formats                                        #
        # ------------------------------------------------------------------ #
        r = client.post("/api/compile/all", json={
            "project_name": "test_bank",
            "layer":        "bronze",
            "table_name":   "customers",
            "target":       "all",
            "dialect":      "spark_sql",
        })
        assert r.status_code == 200, r.text

        compiled = r.json()
        assert "sql"    in compiled
        assert "pyspark" in compiled
        assert "dbt"    in compiled

        sql_output     = compiled["sql"]["sql"]
        pyspark_output = compiled["pyspark"]["code"]
        dbt_output     = compiled["dbt"]["model_sql"]

        assert "WITH cleaned AS" in sql_output,        "SQL must open with CTE"
        assert "LOWER"           in sql_output,        "LOWER() must appear for customer_name"
        assert "def clean_customers" in pyspark_output, "PySpark must define clean_customers"
        assert "source("         in dbt_output,        "dbt must reference source()"

        # Keep the SQL for step 6
        compiled_sql = sql_output

        # ------------------------------------------------------------------ #
        # Step 5 — Seed synthetic data directly via ProjectManager            #
        # ------------------------------------------------------------------ #
        pm = ProjectManager()
        synthetic_rows = _make_synthetic_rows()
        pm.save_synthetic_data("test_bank", "bronze", "customers", synthetic_rows, "csv")

        assert (table_path / "synthetic_data.csv").exists(), \
            "synthetic_data.csv must be written to disk"

        # ------------------------------------------------------------------ #
        # Step 6 — Run validation (real DuckDB, real data)                    #
        # ------------------------------------------------------------------ #
        r = client.post("/api/execute/validate", json={
            "project_name": "test_bank",
            "layer":        "bronze",
            "table_name":   "customers",
            "compiled_sql": compiled_sql,
        })
        assert r.status_code == 200, r.text

        report = r.json()
        assert "passed"      in report,              "report must have 'passed' key"
        assert "columns"     in report,              "report must have 'columns' key"
        assert "customer_id" in report["columns"],   "columns must include customer_id"

        assert (table_path / "validation_results.json").exists(), \
            "validation_results.json must be persisted"

        # ------------------------------------------------------------------ #
        # Step 7 — Verify complete folder structure on disk                   #
        # ------------------------------------------------------------------ #
        assert (tforge / "project.json").exists()
        assert (tforge / "bronze" / "customers" / "schema.json").exists()
        assert (tforge / "bronze" / "customers" / "transformations.json").exists()
        assert (tforge / "bronze" / "customers" / "column_notes.json").exists()
        assert (tforge / "bronze" / "customers" / "synthetic_data.csv").exists()
        assert (tforge / "bronze" / "customers" / "validation_results.json").exists()

    print("✅ PHASE 1 COMPLETE — DataForge Studio backend working end to end")
