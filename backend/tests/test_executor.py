from app.models.spec import Column, ColumnTransformations, RegexTransform
from app.services.executor import DuckDBExecutor


# ------------------------------------------------------------ execute_sql


def test_basic_execution():
    executor = DuckDBExecutor()
    data = [
        {"customer_name": f"  user_{i}  " if i % 2 == 0 else f"  User_{i}  "}
        for i in range(10)
    ]
    result = executor.execute_sql(
        "SELECT TRIM(UPPER(customer_name)) as customer_name FROM customers",
        data,
        "customers",
    )
    assert result["success"] is True
    assert result["row_count"] == 10
    for row in result["rows"]:
        val = row["customer_name"]
        assert val == val.strip(), f"Not trimmed: '{val}'"
        assert val == val.upper(), f"Not uppercase: '{val}'"


def test_null_handling():
    executor = DuckDBExecutor()
    data = [
        {"email": None if i in (2, 5, 8) else f"user{i}@example.com"}
        for i in range(10)
    ]
    result = executor.execute_sql(
        "SELECT COALESCE(email, 'UNKNOWN') as email FROM customers",
        data,
        "customers",
    )
    assert result["success"] is True
    assert result["row_count"] == 10
    assert all(r["email"] is not None for r in result["rows"])


def test_execute_returns_error_dict():
    executor = DuckDBExecutor()
    result = executor.execute_sql(
        "SELECT * FROM nonexistent_table_xyz", [], "customers"
    )
    assert result["success"] is False
    assert result["error"] != ""


# ------------------------------------------------------- generate_assertions


def test_generate_assertions_not_nullable():
    executor = DuckDBExecutor()
    col = Column(name="id", data_type="integer", nullable=False)
    assertions = executor.generate_assertions(col)
    types = [a["type"] for a in assertions]
    assert "not_null" in types


def test_generate_assertions_with_regex():
    executor = DuckDBExecutor()
    col = Column(
        name="phone",
        data_type="string",
        transformations=ColumnTransformations(
            regex=RegexTransform(enabled=True, pattern=r"\d+", replacement="")
        ),
    )
    assertions = executor.generate_assertions(col)
    types = [a["type"] for a in assertions]
    assert "no_error" in types


# --------------------------------------------------------- validate_table


def test_validate_table_full():
    executor = DuckDBExecutor()

    table_spec = {
        "project": {"name": "test_proj"},
        "table": {"name": "test_table", "layer": "bronze"},
        "columns": [
            Column(name="id", data_type="integer", nullable=False).model_dump(
                mode="json"
            ),
            Column(name="name", data_type="string", nullable=True).model_dump(
                mode="json"
            ),
        ],
        "notes": {},
    }

    # 20 rows, 2 have null id (indices 3 and 7)
    data = [
        {"id": None if i in (3, 7) else i, "name": f"  user {i}  "}
        for i in range(20)
    ]

    result = executor.validate_table(
        table_spec,
        "SELECT id, TRIM(name) as name FROM test_table",
        data,
    )

    # null ids violate the not_null assertion → overall fails
    assert result["passed"] is False
    assert "id" in result["columns"]
    assert "name" in result["columns"]

    # row_count_gte should pass (20 rows output)
    id_assertions = result["columns"]["id"]["assertions"]
    row_count_result = next(
        a for a in id_assertions if a["type"] == "row_count_gte"
    )
    assert row_count_result["passed"] is True
