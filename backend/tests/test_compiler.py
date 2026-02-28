import pytest

from app.models.spec import (
    Column,
    ColumnTransformations,
    RegexTransform,
)
from app.services.compiler import TransformCompiler


# ------------------------------------------------------------------ helpers

def _col(name="col_a", data_type="string", **kwargs) -> Column:
    return Column(name=name, data_type=data_type, **kwargs)


def _table_spec(cols: list[Column], name="customers", layer="bronze") -> dict:
    return {
        "project": {"name": "proj"},
        "table": {"name": name, "layer": layer},
        "columns": [c.model_dump(mode="json") for c in cols],
        "notes": {c.name: c.notes for c in cols},
    }


# ------------------------------------------------------------------ tests

def test_trim_all_sql_dialects():
    compiler = TransformCompiler()
    col = _col(transformations=ColumnTransformations(trim=True))
    dialects = [
        "snowflake_sql", "spark_sql", "bigquery_sql",
        "tsql", "mysql", "postgresql", "ansi",
    ]
    for dialect in dialects:
        result = compiler.compile_column(col, "sql", dialect)
        assert result["expression"] != "", f"Empty expression for {dialect}"
        if dialect == "tsql":
            assert "LTRIM" in result["expression"], f"tsql missing LTRIM: {result['expression']}"
        else:
            assert "TRIM" in result["expression"], f"{dialect} missing TRIM: {result['expression']}"


def test_tsql_regex_warning():
    compiler = TransformCompiler()
    col = _col(
        name="phone",
        transformations=ColumnTransformations(
            regex=RegexTransform(enabled=True, pattern=r"\d+", replacement="X")
        ),
    )
    result = compiler.compile_column(col, "sql", "tsql")
    assert len(result["warnings"]) > 0, "Expected at least one warning for tsql regex"
    assert "REGEXP_REPLACE" not in result["expression"], (
        f"Expression must not contain REGEXP_REPLACE: {result['expression']}"
    )


def test_transform_stacking_order():
    compiler = TransformCompiler()
    col = _col(
        transformations=ColumnTransformations(
            trim=True,
            regex=RegexTransform(enabled=True, pattern=r"\d+", replacement="X"),
            case_normalization="upper",
        )
    )
    result = compiler.compile_column(col, "sql", "snowflake_sql")
    expr = result["expression"]
    assert "TRIM" in expr, f"TRIM missing from: {expr}"
    assert "UPPER" in expr, f"UPPER missing from: {expr}"
    # UPPER wraps everything → lower index; TRIM is innermost → higher index
    assert expr.index("UPPER") < expr.index("TRIM"), (
        f"UPPER should appear before TRIM in '{expr}'"
    )


def test_custom_expression_bypass():
    compiler = TransformCompiler()
    col = _col(
        transformations=ColumnTransformations(
            trim=True,
            custom_expression="MY_CUSTOM_FUNC(col)",
        )
    )
    result = compiler.compile_column(col, "sql", "snowflake_sql")
    assert result["expression"] == "MY_CUSTOM_FUNC(col)"
    assert "TRIM" not in result["expression"]


def test_full_sql_cte_structure():
    compiler = TransformCompiler()
    cols = [
        _col("id", "integer"),
        _col("name", "string"),
        _col("email", "string"),
    ]
    result = compiler.compile_table_sql(
        _table_spec(cols), "snowflake_sql", {"catalog": "", "name": "proj"}
    )
    sql = result["sql"]
    assert sql.strip().startswith("WITH cleaned AS"), f"CTE missing: {sql[:60]}"
    for col in cols:
        assert col.name in sql, f"Alias '{col.name}' missing from SQL"


def test_pyspark_structure():
    compiler = TransformCompiler()
    cols = [_col("id", "integer"), _col("name", "string"), _col("email", "string")]
    result = compiler.compile_table_pyspark(_table_spec(cols, name="orders"), {})
    code = result["code"]
    assert "from pyspark.sql import functions as F" in code
    assert "def clean_" in code
    assert code.count(".withColumn(") == 3, (
        f"Expected 3 .withColumn calls, got {code.count('.withColumn(')}"
    )


def test_dbt_output():
    compiler = TransformCompiler()
    cols = [
        _col("id", "integer", nullable=False, notes="Primary key"),
        _col("name", "string", nullable=True),
    ]
    spec = _table_spec(cols)
    spec["notes"] = {"id": "Primary key", "name": ""}
    result = compiler.compile_table_dbt(spec, "snowflake_sql", {"name": "proj"})
    assert "source(" in result["model_sql"], "dbt source() macro missing"
    assert "id" in result["schema_yml"], "Column 'id' missing from schema.yml"
    assert "not_null" in result["schema_yml"], "not_null test missing from schema.yml"
