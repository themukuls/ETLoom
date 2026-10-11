from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from yamlflow.config.models import ConnectionsFile, PipelineFile, PostgresConnection
from yamlflow.connectors.postgres import PostgresConnector
from yamlflow.metadata.store import MetadataStore
from yamlflow.quality.engine import evaluate_quality
from yamlflow.transformations.engine import apply_transformations


@dataclass
class RunSummary:
    run_id: UUID
    pipeline_id: str
    status: str
    extracted_rows: int
    loaded_rows: int
    rejected_rows: int
    elapsed_seconds: float


class PipelineExecutor:
    def __init__(self, connections: ConnectionsFile, control_connection: str):
        self.connections = connections
        if control_connection not in connections.connections:
            raise ValueError(f"Unknown control connection {control_connection!r}")
        self.metadata = MetadataStore(
            PostgresConnector(connections.connections[control_connection])
        )

    def _connector(self, name: str) -> PostgresConnector:
        connection: PostgresConnection | None = self.connections.connections.get(name)
        if not connection:
            raise ValueError(f"Unknown connection {name!r}")
        return PostgresConnector(connection)

    @staticmethod
    def _retry(operation, runtime):
        """Retry transient PostgreSQL failures without retrying data/configuration errors."""
        from psycopg import InterfaceError, OperationalError

        delay = runtime.retry_delay_seconds
        for attempt in range(runtime.max_retries + 1):
            try:
                return operation()
            except (OperationalError, InterfaceError):
                if attempt == runtime.max_retries:
                    raise
                time.sleep(delay)
                delay *= runtime.retry_backoff_multiplier

    def run(self, pipeline: PipelineFile) -> RunSummary:
        if not pipeline.pipeline.enabled:
            raise ValueError(f"Pipeline {pipeline.pipeline.id!r} is disabled")

        self.metadata.initialize()
        run_id = uuid4()
        pipeline_id = pipeline.pipeline.id
        metrics: dict[str, Any] = {
            "extracted_rows": 0,
            "loaded_rows": 0,
            "rejected_rows": 0,
            "warnings": [],
        }
        started = time.monotonic()
        self.metadata.start_run(run_id, pipeline_id)

        source = self._connector(pipeline.source.connection)
        target = self._connector(pipeline.target.connection)
        checkpoint = self.metadata.get_checkpoint(pipeline_id)
        if checkpoint is None and pipeline.source.incremental:
            checkpoint = pipeline.source.incremental.initial_value
            if isinstance(checkpoint, str):
                try:
                    checkpoint = datetime.fromisoformat(checkpoint.replace("Z", "+00:00"))
                except ValueError:
                    pass
        upper_bound = None
        if pipeline.source.incremental:
            upper_bound = source.maximum_value(pipeline.source, pipeline.source.incremental.column)

        try:
            if pipeline.target.load.mode == "replace":
                self._retry(lambda: target.truncate(pipeline.target), pipeline.runtime)
            for batch in source.extract_batches(pipeline.source, checkpoint, upper_bound):
                metrics["extracted_rows"] += len(batch)
                transformed = apply_transformations(batch, pipeline.transformations)
                quality = evaluate_quality(transformed, pipeline.quality)
                self.metadata.quarantine(run_id, pipeline_id, quality.rejected)
                metrics["rejected_rows"] += len(quality.rejected)
                metrics["warnings"].extend(quality.warnings)
                loaded = self._retry(
                    lambda rows=quality.valid_rows: target.load(pipeline.target, rows),
                    pipeline.runtime,
                )
                metrics["loaded_rows"] += loaded

            if pipeline.source.incremental and upper_bound is not None:
                self.metadata.save_checkpoint(pipeline_id, upper_bound)
            elapsed = round(time.monotonic() - started, 3)
            metrics["elapsed_seconds"] = elapsed
            self.metadata.finish_run(run_id, "SUCCESS", metrics)
            return RunSummary(
                run_id,
                pipeline_id,
                "SUCCESS",
                elapsed_seconds=elapsed,
                **{key: metrics[key] for key in ("extracted_rows", "loaded_rows", "rejected_rows")},
            )
        except Exception as exc:
            elapsed = round(time.monotonic() - started, 3)
            metrics["elapsed_seconds"] = elapsed
            self.metadata.finish_run(run_id, "FAILED", metrics, str(exc))
            raise
