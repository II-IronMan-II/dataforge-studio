from __future__ import annotations

import textwrap

from app.models.spec import Column
from app.services.templates import TEMPLATES, TRANSFORM_ORDER

# Literal string used in templates to signal unsupported operations
_WARNING_PREFIX = "__WARNING__:"

# The F.col('{col}') pattern that appears in every PySpark template
_PYSPARK_COL_PATTERN = "F.col('{col}')"


class TransformCompiler:

    # --------------------------------------------------------- column level

    def compile_column(
        self, column: Column, target: str, dialect: str
    ) -> dict:
        t = column.transformations
        warnings: list[str] = []

        # Custom expression short-circuits all template logic
        if t.custom_expression:
            return {
                "column_name": column.name,
                "expression": t.custom_expression,
                "alias": column.name,
                "warnings": [],
                "target": target,
                "dialect": dialect,
            }

        tgt_dialect = "pyspark" if target == "pyspark" else dialect

        # Initial expression: raw column reference
        expr: str = (
            f"F.col('{column.name}')" if target == "pyspark" else column.name
        )
        # Tracks whether any PySpark template has been applied yet
        pyspark_applied = False

        def _apply(template_key: str, extra: dict | None = None) -> None:
            nonlocal expr, pyspark_applied
            extra = extra or {}
            tmpl = TEMPLATES.get(template_key, {}).get(tgt_dialect, "")
            if not tmpl:
                return
            if tmpl.startswith(_WARNING_PREFIX):
                warnings.append(tmpl[len(_WARNING_PREFIX):].lstrip())
                return  # skip — do not modify expr

            if target == "pyspark":
                if not pyspark_applied:
                    # First PySpark template: {col} = bare column name
                    filled = tmpl.format(col=column.name, **extra)
                    pyspark_applied = True
                else:
                    # Subsequent: replace F.col('{col}') with accumulated expr,
                    # then format remaining placeholders using col=expr
                    chain = tmpl.replace(_PYSPARK_COL_PATTERN, "{col}")
                    filled = chain.format(col=expr, **extra)
            else:
                filled = tmpl.format(col=expr, **extra)

            expr = filled

        for step in TRANSFORM_ORDER:
            if step == "trim":
                if t.trim:
                    _apply("trim")

            elif step == "strip_special":
                if t.strip_special_chars:
                    _apply("strip_special")

            elif step == "regex":
                if t.regex.enabled:
                    _apply(
                        "regex_replace",
                        {"pattern": t.regex.pattern, "repl": t.regex.replacement},
                    )

            elif step == "case_normalization":
                if t.case_normalization != "none":
                    _apply(f"case_{t.case_normalization}")

            elif step == "type_cast":
                if t.type_cast:
                    _apply(
                        f"cast_{t.type_cast}",
                        {"type": t.type_cast, "format": "YYYY-MM-DD"},
                    )

            elif step == "null_strategy":
                if t.null_strategy == "replace":
                    _apply("null_replace", {"replacement": t.null_replacement})
                elif t.null_strategy == "drop":
                    _apply("null_drop")
                elif t.null_strategy == "flag":
                    _apply("null_flag")

            elif step == "where_filter":
                if t.where_filter.enabled:
                    _apply("where_filter", {"condition": t.where_filter.condition})

            elif step == "conditional":
                if t.conditional.enabled and t.conditional.cases:
                    cases_str = " ".join(
                        f"WHEN {c.when} THEN '{c.then}'"
                        for c in t.conditional.cases
                    )
                    _apply(
                        "conditional_case",
                        {
                            "cases": cases_str,
                            "else_value": t.conditional.else_value,
                        },
                    )

            elif step == "delimiter_split":
                if t.delimiter_split.enabled:
                    _apply(
                        "delimiter_split",
                        {
                            "delimiter": t.delimiter_split.delimiter,
                            "index": t.delimiter_split.index,
                        },
                    )

        return {
            "column_name": column.name,
            "expression": expr,
            "alias": column.name,
            "warnings": warnings,
            "target": target,
            "dialect": dialect,
        }

    # ---------------------------------------------------------- table level

    def compile_table_sql(
        self, table_spec: dict, dialect: str, project_config: dict
    ) -> dict:
        table_name = table_spec["table"]["name"]
        layer = table_spec["table"]["layer"]
        catalog = project_config.get("catalog", "")

        source = (
            f"{catalog}.{layer}.{table_name}" if catalog
            else f"{layer}.{table_name}"
        )

        col_results = [
            self.compile_column(Column.model_validate(c), "sql", dialect)
            for c in table_spec["columns"]
        ]

        all_warnings = [w for r in col_results for w in r["warnings"]]

        select_lines = ",\n        ".join(
            f"{r['expression']} AS {r['alias']}" for r in col_results
        )

        sql = (
            f"WITH cleaned AS (\n"
            f"    SELECT\n"
            f"        {select_lines}\n"
            f"    FROM {source}\n"
            f")\n"
            f"SELECT * FROM cleaned"
        )

        return {
            "sql": sql,
            "dialect": dialect,
            "warnings": all_warnings,
            "columns": col_results,
        }

    def compile_table_pyspark(
        self, table_spec: dict, project_config: dict
    ) -> dict:
        table_name = table_spec["table"]["name"]

        col_results = [
            self.compile_column(Column.model_validate(c), "pyspark", "pyspark")
            for c in table_spec["columns"]
        ]

        all_warnings = [w for r in col_results for w in r["warnings"]]

        with_cols = "\n            ".join(
            f".withColumn('{r['alias']}', {r['expression']})"
            for r in col_results
        )

        code = textwrap.dedent(f"""\
            from pyspark.sql import DataFrame
            from pyspark.sql import functions as F


            def clean_{table_name}(
                df: DataFrame,
            ) -> DataFrame:
                return (
                    df
                    {with_cols}
                )
        """)

        return {
            "code": code,
            "warnings": all_warnings,
            "columns": col_results,
        }

    def compile_table_dbt(
        self, table_spec: dict, dialect: str, project_config: dict
    ) -> dict:
        table_name = table_spec["table"]["name"]
        layer = table_spec["table"]["layer"]
        notes: dict = table_spec.get("notes", {})

        col_results = [
            self.compile_column(Column.model_validate(c), "sql", dialect)
            for c in table_spec["columns"]
        ]

        all_warnings = [w for r in col_results for w in r["warnings"]]

        select_lines = ",\n    ".join(
            f"{r['expression']} AS {r['alias']}" for r in col_results
        )

        model_sql = (
            f"WITH source AS (\n"
            f"    SELECT * FROM {{{{ source('{layer}', '{table_name}') }}}}\n"
            f"),\n"
            f"cleaned AS (\n"
            f"    SELECT\n"
            f"    {select_lines}\n"
            f"    FROM source\n"
            f")\n"
            f"SELECT * FROM cleaned"
        )

        schema_yml = self._build_schema_yml(table_name, table_spec["columns"], notes)

        return {
            "model_sql": model_sql,
            "schema_yml": schema_yml,
            "warnings": all_warnings,
        }

    # ---------------------------------------------------------------- helpers

    @staticmethod
    def _build_schema_yml(
        table_name: str, columns: list[dict], notes: dict
    ) -> str:
        lines = [
            "version: 2",
            "",
            "models:",
            f"  - name: {table_name}",
            "    columns:",
        ]
        for col_dict in columns:
            col = Column.model_validate(col_dict)
            lines.append(f"      - name: {col.name}")
            note = notes.get(col.name, "")
            if note:
                lines.append(f'        description: "{note}"')
            if not col.nullable:
                lines.append("        tests:")
                lines.append("          - not_null")
        return "\n".join(lines) + "\n"
