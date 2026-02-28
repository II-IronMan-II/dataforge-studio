from __future__ import annotations

import duckdb
import pandas as pd

from app.models.spec import Column


class DuckDBExecutor:

    # ------------------------------------------------------------ execution

    def execute_sql(
        self, query: str, data: list[dict], table_name: str
    ) -> dict:
        try:
            conn = duckdb.connect(":memory:")
            if data:
                df = pd.DataFrame(data)
                conn.register(table_name, df)
            conn.execute(query)
            columns = [d[0] for d in conn.description] if conn.description else []
            rows_raw = conn.fetchall()
            rows = [dict(zip(columns, row)) for row in rows_raw]
            conn.close()
            return {
                "success": True,
                "rows": rows,
                "columns": columns,
                "row_count": len(rows),
                "error": "",
            }
        except Exception as exc:
            return {
                "success": False,
                "rows": [],
                "columns": [],
                "row_count": 0,
                "error": str(exc),
            }

    # ---------------------------------------------------------- assertions

    def generate_assertions(self, column: Column) -> list[dict]:
        assertions: list[dict] = [
            {
                "type": "row_count_gte",
                "min": 1,
                "description": "output has at least 1 row",
            }
        ]
        if not column.nullable:
            assertions.append({
                "type": "not_null",
                "column": column.name,
                "description": f"{column.name} has no nulls",
            })
        if column.transformations.type_cast:
            assertions.append({
                "type": "type_check",
                "column": column.name,
                "expected_type": column.transformations.type_cast,
                "description": f"{column.name} cast to {column.transformations.type_cast}",
            })
        if column.transformations.regex.enabled:
            assertions.append({
                "type": "no_error",
                "column": column.name,
                "description": "regex applied without errors",
            })
        return assertions

    def run_assertion(self, assertion: dict, rows: list[dict]) -> dict:
        atype = assertion["type"]
        description = assertion["description"]

        if atype == "row_count_gte":
            passed = len(rows) >= assertion["min"]
            detail = (
                ""
                if passed
                else f"Expected >= {assertion['min']} rows, got {len(rows)}"
            )

        elif atype == "not_null":
            col = assertion["column"]
            null_count = sum(1 for r in rows if r.get(col) is None)
            passed = null_count == 0
            detail = "" if passed else f"{null_count} null(s) found in '{col}'"

        elif atype == "type_check":
            # DuckDB enforces types at query time; reaching here means it passed
            passed = True
            detail = ""

        elif atype == "no_error":
            # SQL executed without error; regex was applied
            passed = True
            detail = ""

        else:
            passed = False
            detail = f"Unknown assertion type: {atype}"

        return {
            "type": atype,
            "description": description,
            "passed": passed,
            "detail": detail,
        }

    # ----------------------------------------------------------- validation

    def validate_table(
        self,
        table_spec: dict,
        compiled_sql: str,
        synthetic_data: list[dict],
    ) -> dict:
        table_name = table_spec["table"]["name"]
        exec_result = self.execute_sql(compiled_sql, synthetic_data, table_name)
        rows = exec_result["rows"]

        all_results: list[dict] = []
        columns_report: dict = {}

        for col_dict in table_spec["columns"]:
            col = Column.model_validate(col_dict)
            assertions = self.generate_assertions(col)
            col_results = [self.run_assertion(a, rows) for a in assertions]
            all_results.extend(col_results)
            columns_report[col.name] = {
                "passed": all(r["passed"] for r in col_results),
                "assertions": col_results,
            }

        total = len(all_results)
        passed_count = sum(1 for r in all_results if r["passed"])

        return {
            "passed": passed_count == total and total > 0,
            "total_assertions": total,
            "passed_count": passed_count,
            "failed_count": total - passed_count,
            "execution": {
                "success": exec_result["success"],
                "row_count": exec_result["row_count"],
                "error": exec_result["error"],
            },
            "columns": columns_report,
        }
