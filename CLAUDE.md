# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

DataForge Studio — local-first visual ETL transformation designer. Works entirely on synthetic data; never connects to real databases. AI generates only synthetic data. All transformation logic is deterministic template-based code generation.

## Commands

```bash
make dev-backend    # cd backend && venv/bin/uvicorn app.main:app --reload --port 8000
make dev-frontend   # cd frontend && npm run dev
make test           # cd backend && venv/bin/python -m pytest tests/ -v
make clean          # remove __pycache__ and .pytest_cache recursively
```

The venv lives at `backend/venv/`. `pytest` must be installed into it (`venv/bin/pip install pytest`). All requirements are already installed.

Alternatively, from the `backend/` directory:

```bash
venv/bin/python run.py       # starts uvicorn with startup banner
```

## Architecture

### Backend (FastAPI, Python)

`backend/app/models/spec.py` is the single source of truth for all domain types. Every other backend file imports from here.

Key model hierarchy:
- `ProjectConfig` — top-level project (platform, dialect, catalog, schema_layer)
- `TableSpec` — a table in a layer (bronze/silver/gold) with a list of `Column`
- `Column` — name, data_type, nullable, and a `ColumnTransformations` bag
- `ColumnTransformations` — all per-column transforms: trim, case, nulls, type cast, regex, where filter, conditional (CASE WHEN), delimiter split, custom expression

`backend/app/models/requests.py` — Pydantic request/response models for the API. Imports `Column` from `spec.py`.

`backend/app/main.py` — FastAPI entrypoint. CORS is open to `localhost:3000` and `localhost:5173`. Lifespan startup logs the active LLM provider and projects directory. All routes live under `/api/{resource}`.

Router files under `backend/app/api/`:
- `projects.py` — CRUD for projects and table creation, mounted at `/api/projects`
- `tables.py` — table spec/transformations/data, also mounted at `/api/projects` so full URLs are `/api/projects/{project_name}/tables/{layer}/{table_name}/...`
- `execute.py`, `compile.py`, `llm.py` — stubs

LLM provider implementations will live in `backend/app/services/llm/providers/`. The active provider is selected via the `LLM_PROVIDER` env var (default: `phi3`). Supported options: `phi3` (local), `ollama`, `openai`-compatible.

Projects are persisted to disk under the `PROJECTS_DIR` path (default: `./projects/`).

### Supported platforms / dialects

Platforms: `databricks`, `snowflake`, `bigquery`, `synapse`, `dbt`, `generic`
Dialects: `snowflake_sql`, `spark_sql`, `bigquery_sql`, `tsql`, `mysql`, `postgresql`, `ansi`

### Frontend (not yet scaffolded)

Will live in `frontend/src/` with subdirectories: `components/`, `store/`, `types/`, `utils/`.

## Environment

Copy `.env.example` to `.env` before running. Key variables:

| Variable | Default | Purpose |
|---|---|---|
| `LLM_PROVIDER` | `phi3` | Which LLM backend to use |
| `PROJECTS_DIR` | `./projects` | Where project JSON is stored |
| `OLLAMA_MODEL` / `OLLAMA_HOST` | — | When `LLM_PROVIDER=ollama` |
| `OPENAI_BASE_URL` / `OPENAI_API_KEY` / `OPENAI_MODEL` | — | When `LLM_PROVIDER=openai` |

## Python package structure note

`backend/app/` and its sub-packages (`models/`, `api/`, `services/`) each require an `__init__.py` to be importable. Add empty ones when scaffolding new packages.
