from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.models.requests import ExecuteRequest
from app.services.executor import DuckDBExecutor
from app.services.project_manager import ProjectManager

router = APIRouter()


class ValidateRequest(BaseModel):
    project_name: str
    layer: str
    table_name: str
    compiled_sql: str


@router.post("/sql")
async def execute_sql(req: ExecuteRequest):
    return DuckDBExecutor().execute_sql(req.query, req.data, req.table_name)


@router.post("/validate")
async def validate(req: ValidateRequest):
    pm = ProjectManager()
    try:
        synthetic = pm.get_synthetic_data(req.project_name, req.layer, req.table_name)
        table_spec = pm.get_table_spec(req.project_name, req.layer, req.table_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    report = DuckDBExecutor().validate_table(
        table_spec, req.compiled_sql, synthetic["data"]
    )
    pm.save_validation_results(req.project_name, req.layer, req.table_name, report)
    return report
