from __future__ import annotations

import asyncio
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, ValidationError

from etloom import __version__
from etloom.config.loader import load_connections, load_pipeline, validate_pipeline_text
from etloom.config.writer import pipeline_path, save_pipeline
from etloom.connectors.postgres import PostgresConnector
from etloom.engine.executor import PipelineExecutor
from etloom.metadata.store import MetadataStore

PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(os.environ.get("ETLOOM_PROJECT_ROOT", Path.cwd())).resolve()
PIPELINE_DIRECTORY = Path(
    os.environ.get("ETLOOM_PIPELINE_DIR", PROJECT_ROOT / "config" / "pipelines")
).resolve()
CONNECTIONS_FILE = Path(
    os.environ.get("ETLOOM_CONNECTIONS_FILE", PROJECT_ROOT / "config" / "connections.yaml")
).resolve()


class YAMLRequest(BaseModel):
    yaml_content: str


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date, Decimal, UUID)):
        return str(value)
    raise TypeError


class PipelineRepository:
    def list(self) -> list[dict[str, Any]]:
        pipelines: list[dict[str, Any]] = []
        for path in sorted(PIPELINE_DIRECTORY.glob("*.yaml")):
            if path.name.endswith(".template.yaml"):
                continue
            try:
                pipeline = load_pipeline(path)
                pipelines.append(
                    {
                        "id": pipeline.metadata.id,
                        "name": pipeline.metadata.name,
                        "description": pipeline.metadata.description,
                        "enabled": pipeline.metadata.enabled,
                        "source": (
                            f"{pipeline.input.object.schema_name}.{pipeline.input.object.table}"
                        ),
                        "target": (
                            f"{pipeline.output.object.schema_name}.{pipeline.output.object.table}"
                        ),
                        "dq_checks": len(pipeline.data_quality.checks),
                    }
                )
            except Exception as exc:
                pipelines.append(
                    {
                        "id": path.stem,
                        "name": path.stem,
                        "description": f"Invalid configuration: {exc}",
                        "enabled": False,
                        "source": "Invalid",
                        "target": "Invalid",
                        "dq_checks": 0,
                    }
                )
        return pipelines

    def read(self, pipeline_id: str) -> tuple[Path, str, Any]:
        path = pipeline_path(PIPELINE_DIRECTORY, pipeline_id)
        if not path.is_file():
            raise FileNotFoundError(pipeline_id)
        content = path.read_text(encoding="utf-8")
        return path, content, load_pipeline(path)


class RunManager:
    def __init__(self):
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="etloom-runner")
        self.active: set[str] = set()
        self.lock = threading.Lock()

    def connections(self):
        if not CONNECTIONS_FILE.is_file():
            raise FileNotFoundError(
                f"Create {CONNECTIONS_FILE} from config/connections.example.yaml"
            )
        return load_connections(CONNECTIONS_FILE)

    def metadata(self) -> MetadataStore:
        connections = self.connections()
        connection = connections.connections.get("control_postgres")
        if not connection:
            raise ValueError("connections.yaml must define control_postgres")
        store = MetadataStore(PostgresConnector(connection))
        store.initialize()
        return store

    def start(self, pipeline_id: str, content: str, pipeline: Any) -> UUID:
        connections = self.connections()
        with self.lock:
            if pipeline_id in self.active:
                raise RuntimeError(f"Pipeline {pipeline_id!r} already has an active run")
            self.active.add(pipeline_id)
        run_id = uuid4()

        def execute() -> None:
            try:
                PipelineExecutor(connections).run(pipeline, content, run_id=run_id)
            finally:
                with self.lock:
                    self.active.discard(pipeline_id)

        self.pool.submit(execute)
        return run_id


repository = PipelineRepository()
run_manager = RunManager()
app = FastAPI(title="ETLoom", version=__version__)
app.mount("/static", StaticFiles(directory=PACKAGE_ROOT / "static"), name="static")
templates = Jinja2Templates(directory=PACKAGE_ROOT / "templates")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"pipelines": repository.list(), "version": __version__},
    )


@app.get("/pipelines/{pipeline_id}", response_class=HTMLResponse)
def pipeline_page(request: Request, pipeline_id: str):
    try:
        _, content, pipeline = repository.read(pipeline_id)
    except FileNotFoundError as exc:
        raise HTTPException(404, "Pipeline not found") from exc
    return templates.TemplateResponse(
        request=request,
        name="pipeline_editor.html",
        context={
            "pipeline": pipeline,
            "pipeline_id": pipeline_id,
            "yaml_content": content,
            "version": __version__,
        },
    )


@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_page(request: Request, run_id: str):
    return templates.TemplateResponse(
        request=request,
        name="run_details.html",
        context={"run_id": run_id, "version": __version__},
    )


@app.get("/api/pipelines")
def list_pipelines():
    return repository.list()


@app.get("/api/pipelines/{pipeline_id}")
def get_pipeline(pipeline_id: str):
    try:
        _, content, pipeline = repository.read(pipeline_id)
    except FileNotFoundError as exc:
        raise HTTPException(404, "Pipeline not found") from exc
    return {"yaml_content": content, "configuration": pipeline.model_dump(by_alias=True)}


@app.post("/api/pipelines/validate")
def validate_pipeline(request: YAMLRequest):
    try:
        pipeline = validate_pipeline_text(request.yaml_content)
        return {
            "valid": True,
            "pipeline_id": pipeline.metadata.id,
            "message": "Configuration is valid",
        }
    except (ValidationError, ValueError) as exc:
        return {"valid": False, "errors": str(exc)}


@app.put("/api/pipelines/{pipeline_id}")
def update_pipeline(pipeline_id: str, request: YAMLRequest):
    try:
        path = save_pipeline(PIPELINE_DIRECTORY, pipeline_id, request.yaml_content)
        return {"saved": True, "path": str(path), "pipeline_id": pipeline_id}
    except (ValidationError, ValueError) as exc:
        raise HTTPException(422, str(exc)) from exc


@app.post("/api/pipelines/{pipeline_id}/runs", status_code=202)
def run_pipeline(pipeline_id: str):
    try:
        _, content, pipeline = repository.read(pipeline_id)
        run_id = run_manager.start(pipeline_id, content, pipeline)
        return {"run_id": str(run_id), "status": "QUEUED"}
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    except (ValidationError, ValueError) as exc:
        raise HTTPException(422, str(exc)) from exc


@app.get("/api/runs")
def list_runs(limit: int = 50):
    try:
        return jsonable_encoder(run_manager.metadata().recent_runs(min(limit, 100)))
    except Exception as exc:
        raise HTTPException(503, f"Control database unavailable: {exc}") from exc


@app.get("/api/runs/{run_id}")
def get_run(run_id: str):
    try:
        run = run_manager.metadata().get_run(run_id)
    except Exception as exc:
        raise HTTPException(503, f"Control database unavailable: {exc}") from exc
    if not run:
        raise HTTPException(404, "Run not found")
    return jsonable_encoder(run)


@app.get("/api/runs/{run_id}/events")
async def run_events(run_id: str):
    async def stream():
        last_event = 0
        while True:
            try:
                store = await asyncio.to_thread(run_manager.metadata)
                events = await asyncio.to_thread(store.events_after, run_id, last_event)
                for event in events:
                    last_event = event["event_id"]
                    yield f"id: {last_event}\ndata: {json.dumps(event, default=_json_default)}\n\n"
                run = await asyncio.to_thread(store.get_run, run_id)
                if run and run["status"] in {"SUCCESS", "FAILED", "CANCELLED"}:
                    yield f"event: complete\ndata: {json.dumps(run, default=_json_default)}\n\n"
                    break
            except Exception as exc:
                yield f"event: error\ndata: {json.dumps({'message': str(exc)})}\n\n"
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(stream(), media_type="text/event-stream")
