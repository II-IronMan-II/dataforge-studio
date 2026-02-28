import json

import pytest

from app.models.requests import CreateProjectRequest, CreateTableRequest
from app.models.spec import Column, ColumnTransformations
from app.services.project_manager import ProjectManager


def _pm(tmp_path, monkeypatch) -> ProjectManager:
    monkeypatch.setenv("PROJECTS_DIR", str(tmp_path))
    return ProjectManager()


def _proj_req(name: str = "myproject") -> CreateProjectRequest:
    return CreateProjectRequest(
        name=name, platform="databricks", dialect="spark_sql"
    )


def _cols() -> list[Column]:
    return [
        Column(name="id", data_type="integer"),
        Column(name="name", data_type="string"),
    ]


def _table_req(name: str = "customers", layer: str = "bronze") -> CreateTableRequest:
    return CreateTableRequest(name=name, layer=layer, columns=_cols())


# ------------------------------------------------------------------ projects


def test_create_project(tmp_path, monkeypatch):
    pm = _pm(tmp_path, monkeypatch)
    result = pm.create_project(_proj_req())
    assert (tmp_path / "myproject.tforge").exists()
    assert result["name"] == "myproject"
    assert result["platform"] == "databricks"


def test_duplicate_project(tmp_path, monkeypatch):
    pm = _pm(tmp_path, monkeypatch)
    pm.create_project(_proj_req())
    with pytest.raises(ValueError, match="already exists"):
        pm.create_project(_proj_req())


# ------------------------------------------------------------------- tables


def test_create_table(tmp_path, monkeypatch):
    pm = _pm(tmp_path, monkeypatch)
    pm.create_project(_proj_req())
    pm.create_table("myproject", _table_req())

    table_path = tmp_path / "myproject.tforge" / "bronze" / "customers"
    assert table_path.exists()
    assert (table_path / "schema.json").exists()
    assert (table_path / "transformations.json").exists()
    assert (table_path / "column_notes.json").exists()


def test_get_table_spec(tmp_path, monkeypatch):
    pm = _pm(tmp_path, monkeypatch)
    pm.create_project(_proj_req())
    pm.create_table("myproject", _table_req())

    spec = pm.get_table_spec("myproject", "bronze", "customers")
    assert "project" in spec
    assert "table" in spec
    assert "columns" in spec
    assert "notes" in spec
    assert len(spec["columns"]) == len(_cols())


def test_save_transformations(tmp_path, monkeypatch):
    pm = _pm(tmp_path, monkeypatch)
    pm.create_project(_proj_req())
    pm.create_table("myproject", _table_req())

    updated = [
        Column(
            name="id",
            data_type="integer",
            transformations=ColumnTransformations(trim=True),
        ).model_dump(mode="json"),
        Column(name="name", data_type="string").model_dump(mode="json"),
    ]
    pm.save_transformations("myproject", "bronze", "customers", updated)

    raw = json.loads(
        (tmp_path / "myproject.tforge" / "bronze" / "customers" / "transformations.json")
        .read_text()
    )
    assert raw[0]["transformations"]["trim"] is True


def test_list_tables(tmp_path, monkeypatch):
    pm = _pm(tmp_path, monkeypatch)
    pm.create_project(_proj_req())

    pm.create_table("myproject", _table_req("orders", "bronze"))
    pm.create_table("myproject", _table_req("customers", "bronze"))
    pm.create_table("myproject", _table_req("dim_date", "silver"))

    result = pm.list_tables("myproject")
    assert len(result["bronze"]) == 2
    assert len(result["silver"]) == 1
    assert result["gold"] == []


# ------------------------------------------------------------ synthetic data


def _setup(tmp_path, monkeypatch) -> ProjectManager:
    pm = _pm(tmp_path, monkeypatch)
    pm.create_project(_proj_req())
    pm.create_table("myproject", _table_req())
    return pm


def _sample_data(n: int = 10) -> list[dict]:
    return [{"id": i, "name": f"user_{i}"} for i in range(n)]


def test_save_and_get_synthetic_data_csv(tmp_path, monkeypatch):
    pm = _setup(tmp_path, monkeypatch)
    pm.save_synthetic_data("myproject", "bronze", "customers", _sample_data(), "csv")
    result = pm.get_synthetic_data("myproject", "bronze", "customers")
    assert result["row_count"] == 10
    assert result["format"] == "csv"


def test_save_and_get_synthetic_data_json(tmp_path, monkeypatch):
    pm = _setup(tmp_path, monkeypatch)
    pm.save_synthetic_data("myproject", "bronze", "customers", _sample_data(), "json")
    result = pm.get_synthetic_data("myproject", "bronze", "customers")
    assert result["row_count"] == 10
    assert result["format"] == "json"
