from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api import compile, execute, llm, projects, tables

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    llm_provider = os.getenv("LLM_PROVIDER", "phi3")
    projects_dir = os.getenv("PROJECTS_DIR", "./projects")
    logger.info(f"LLM provider: {llm_provider}")
    logger.info(f"Projects directory: {projects_dir}")
    yield


app = FastAPI(title="DataForge Studio", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={
            "error": type(exc).__name__,
            "detail": str(exc),
            "status_code": 500,
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}


app.include_router(projects.router, prefix="/api/projects")
app.include_router(tables.router, prefix="/api/tables")
app.include_router(execute.router, prefix="/api/execute")
app.include_router(compile.router, prefix="/api/compile")
app.include_router(llm.router, prefix="/api/llm")
