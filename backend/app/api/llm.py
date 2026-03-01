from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.models.requests import GenerateDataRequest
from app.services.llm.factory import get_llm_provider
from app.services.project_manager import ProjectManager

router = APIRouter()


@router.get("/status")
async def llm_status():
    provider = get_llm_provider()
    info = provider.get_provider_info()
    return {**info, "available": provider.is_available()}


@router.post("/generate-data")
async def generate_data(req: GenerateDataRequest):
    pm = ProjectManager()
    try:
        spec = pm.get_table_spec(req.project_name, req.layer, req.table_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    schema: list[dict] = spec["columns"]
    notes: dict = spec["notes"]
    column_names = {col["name"] for col in schema}

    provider = get_llm_provider()
    try:
        rows = provider.generate_synthetic_data(schema, notes, req.row_count)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Validate every row has all column keys; collect missing as warnings
    warnings: list[str] = []
    for i, row in enumerate(rows):
        missing = column_names - row.keys()
        if missing:
            warnings.append(
                f"Row {i} missing columns: {', '.join(sorted(missing))}"
            )

    pm.save_synthetic_data(
        req.project_name,
        req.layer,
        req.table_name,
        rows,
        req.output_format,
    )

    return {
        "rows": rows,
        "row_count": len(rows),
        "format": req.output_format,
        "warnings": warnings,
    }
