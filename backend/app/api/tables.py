from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.models.requests import UpdateTransformationsRequest
from app.services.project_manager import ProjectManager

router = APIRouter()


def _pm() -> ProjectManager:
    return ProjectManager()


class NotesBody(BaseModel):
    notes: dict


@router.get("/{project_name}/tables/{layer}/{table_name}")
async def get_table_spec(project_name: str, layer: str, table_name: str):
    try:
        return _pm().get_table_spec(project_name, layer, table_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.put("/{project_name}/tables/{layer}/{table_name}/transformations")
async def save_transformations(
    project_name: str,
    layer: str,
    table_name: str,
    req: UpdateTransformationsRequest,
):
    pm = _pm()
    columns = [col.model_dump(mode="json") for col in req.columns]
    pm.save_transformations(project_name, layer, table_name, columns)
    return pm.get_table_spec(project_name, layer, table_name)


@router.put("/{project_name}/tables/{layer}/{table_name}/notes")
async def save_notes(
    project_name: str, layer: str, table_name: str, body: NotesBody
):
    _pm().save_column_notes(project_name, layer, table_name, body.notes)
    return {"notes": body.notes}


@router.get("/{project_name}/tables/{layer}/{table_name}/synthetic-data")
async def get_synthetic_data(project_name: str, layer: str, table_name: str):
    try:
        return _pm().get_synthetic_data(project_name, layer, table_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{project_name}/tables/{layer}/{table_name}/validation")
async def get_validation_results(project_name: str, layer: str, table_name: str):
    try:
        return _pm().get_validation_results(project_name, layer, table_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
