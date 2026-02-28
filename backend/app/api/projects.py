from fastapi import APIRouter, HTTPException

from app.models.requests import CreateProjectRequest, CreateTableRequest
from app.services.project_manager import ProjectManager

router = APIRouter()


def _pm() -> ProjectManager:
    return ProjectManager()


@router.post("/", status_code=201)
async def create_project(req: CreateProjectRequest):
    try:
        return _pm().create_project(req)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@router.get("/")
async def list_projects():
    return _pm().list_projects()


@router.get("/{project_name}")
async def get_project(project_name: str):
    try:
        return _pm().get_project(project_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{project_name}/tables")
async def list_tables(project_name: str):
    try:
        return _pm().list_tables(project_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{project_name}/tables", status_code=201)
async def create_table(project_name: str, req: CreateTableRequest):
    try:
        return _pm().create_table(project_name, req)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
