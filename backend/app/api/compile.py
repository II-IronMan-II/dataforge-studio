from fastapi import APIRouter, HTTPException

from app.models.requests import CompileRequest
from app.services.compiler import TransformCompiler
from app.services.project_manager import ProjectManager

router = APIRouter()


def _load(req: CompileRequest) -> tuple[dict, dict]:
    """Return (table_spec, project_config), raising 404 on missing data."""
    pm = ProjectManager()
    try:
        table_spec = pm.get_table_spec(req.project_name, req.layer, req.table_name)
        project_config = pm.get_project(req.project_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return table_spec, project_config


@router.post("/sql")
async def compile_sql(req: CompileRequest):
    table_spec, project_config = _load(req)
    return TransformCompiler().compile_table_sql(table_spec, req.dialect, project_config)


@router.post("/pyspark")
async def compile_pyspark(req: CompileRequest):
    table_spec, project_config = _load(req)
    return TransformCompiler().compile_table_pyspark(table_spec, project_config)


@router.post("/dbt")
async def compile_dbt(req: CompileRequest):
    table_spec, project_config = _load(req)
    return TransformCompiler().compile_table_dbt(table_spec, req.dialect, project_config)


@router.post("/all")
async def compile_all(req: CompileRequest):
    table_spec, project_config = _load(req)
    compiler = TransformCompiler()
    return {
        "sql":     compiler.compile_table_sql(table_spec, req.dialect, project_config),
        "pyspark": compiler.compile_table_pyspark(table_spec, project_config),
        "dbt":     compiler.compile_table_dbt(table_spec, req.dialect, project_config),
    }
