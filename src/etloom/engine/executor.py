from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from psycopg import InterfaceError, OperationalError

from etloom.config.models import ConnectionsFile, DQCheck, PipelineFile, PostgresConnection
from etloom.connectors.postgres import PostgresConnector
from etloom.metadata.store import MetadataStore
from etloom.quality.engine import DQContext, DQResult, QualityFailure, run_check
from etloom.transformations.engine import apply_transformations

LOGGER = logging.getLogger("etloom")
EventCallback = Callable[[dict[str, Any]], None]


@dataclass
class RunSummary:
    run_id: UUID
    pipeline_id: str
    status: str
    extracted_rows: int
    transformed_rows: int
    loaded_rows: int
    rejected_rows: int
    elapsed_seconds: float

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["run_id"] = str(self.run_id)
        return result


class PipelineExecutor:
    def __init__(self, connections: ConnectionsFile):
        self.connections = connections

    def _connector(self, name: str) -> PostgresConnector:
        connection: PostgresConnection | None = self.connections.connections.get(name)
        if not connection:
            raise ValueError(f"Unknown connection {name!r}")
        return PostgresConnector(connection)

    @staticmethod
    def _retry(operation, pipeline: PipelineFile):
        retry = pipeline.runtime.retry
        maximum = retry.maximum_attempts if retry.enabled else 1
        delay = retry.initial_delay_seconds
        for attempt in range(maximum):
            try:
                return operation()
            except (OperationalError, InterfaceError):
                if attempt == maximum - 1:
                    raise
                time.sleep(delay)
                delay = min(delay * retry.backoff_multiplier, retry.maximum_delay_seconds)

    def run(
        self,
        pipeline: PipelineFile,
        yaml_content: str = "",
        run_id: UUID | None = None,
        callback: EventCallback | None = None,
    ) -> RunSummary:
        if not pipeline.metadata.enabled:
            raise ValueError(f"Pipeline {pipeline.metadata.id!r} is disabled")
        run_id = run_id or uuid4()
        pipeline_id = pipeline.metadata.id
        metadata = MetadataStore(self._connector(pipeline.checkpoint.connection))
        metadata.initialize()
        quarantine_store = MetadataStore(self._connector(pipeline.quarantine.connection))
        if pipeline.quarantine.connection != pipeline.checkpoint.connection:
            quarantine_store.initialize()
        metadata.start_run(run_id, pipeline_id, yaml_content)
        started = time.monotonic()
        metrics: dict[str, Any] = {
            "extracted_rows": 0,
            "transformed_rows": 0,
            "loaded_rows": 0,
            "rejected_rows": 0,
            "dq_checks": 0,
        }
        source = self._connector(pipeline.input.connection)
        target = self._connector(pipeline.output.connection)
        active_step: str | None = None
        active_step_started = 0.0

        def emit(
            message: str,
            level: str = "INFO",
            step: str | None = None,
            status: str | None = None,
            **details: Any,
        ) -> None:
            getattr(LOGGER, level.lower(), LOGGER.info)(message)
            event = {
                "run_id": str(run_id),
                "level": level,
                "step_id": step,
                "status": status,
                "message": message,
                "details": details,
            }
            metadata.event(run_id, level, message, step, status, details)
            if callback:
                callback(event)

        def begin_step(step_id: str, name: str) -> None:
            nonlocal active_step, active_step_started
            active_step, active_step_started = step_id, time.monotonic()
            metadata.start_step(run_id, step_id, name)
            emit(name, step=step_id, status="RUNNING")

        def finish_step(
            status: str = "SUCCESS",
            input_rows: int | None = None,
            output_rows: int | None = None,
            error: str | None = None,
        ) -> None:
            nonlocal active_step
            if not active_step:
                return
            step_id = active_step
            duration = int((time.monotonic() - active_step_started) * 1000)
            metadata.finish_step(
                run_id,
                step_id,
                status,
                duration,
                input_rows,
                output_rows,
                error,
            )
            emit(
                f"{step_id.replace('_', ' ').title()} {status.lower()}",
                "ERROR" if status == "FAILED" else "INFO",
                step_id,
                status,
                duration_ms=duration,
            )
            active_step = None

        def dq_context(staging_table: str | None = None) -> DQContext:
            return DQContext(
                source_schema=pipeline.input.object.schema_name,
                source_table=pipeline.input.object.table,
                target_schema=pipeline.output.object.schema_name,
                target_table=pipeline.output.object.table,
                staging_schema=pipeline.output.write.staging.schema_name,
                staging_table=staging_table,
                pipeline_id=pipeline_id,
                run_id=str(run_id),
                previous_watermark=(checkpoint or {}).get("watermark"),
                current_watermark=(upper_cursor or {}).get("watermark"),
            )

        def execute_checks(
            checks: list[DQCheck], default_connection: str, context: DQContext
        ) -> list[DQResult]:
            results: list[DQResult] = []
            for check in checks:
                result = run_check(check, self._connector, default_connection, context)
                metrics["dq_checks"] += 1
                results.append(result)
                level = "INFO" if result.passed else "WARNING"
                emit(
                    f"DQ {check.id}: {'passed' if result.passed else 'failed'} ({result.message})",
                    level,
                    active_step,
                    "SUCCESS" if result.passed else "FAILED",
                    check_id=check.id,
                    observed=result.observed,
                )
                if result.passed:
                    continue
                if check.on_failure == "fail":
                    raise QualityFailure(f"DQ check {check.id!r} failed: {result.message}")
                if check.on_failure == "quarantine":
                    quarantine_store.quarantine(
                        run_id, pipeline_id, check.id, result.message, result.failing_rows
                    )
                    metrics["rejected_rows"] += len(result.failing_rows)
            return results

        checkpoint: dict[str, Any] | None = None
        upper_cursor: dict[str, Any] | None = None
        staging_table = f"{pipeline.output.write.staging.table_prefix}_{run_id.hex[:10]}"
        try:
            begin_step("validate", "Validate configuration and connections")
            references = {
                pipeline.input.connection,
                pipeline.output.connection,
                pipeline.checkpoint.connection,
                pipeline.quarantine.connection,
            }
            for name in references:
                self._connector(name).test_connection()
            finish_step()

            checkpoint = metadata.get_checkpoint(pipeline_id)
            incremental = pipeline.input.extraction.incremental
            if checkpoint is None and incremental and incremental.initial_value is not None:
                initial = incremental.initial_value
                if isinstance(initial, str):
                    try:
                        initial = datetime.fromisoformat(initial.replace("Z", "+00:00"))
                    except ValueError:
                        pass
                checkpoint = {"watermark": initial, "tie_breaker": None}
            upper_cursor = source.maximum_cursor(pipeline.input)

            begin_step("pre_extract_dq", "Run pre-extract data-quality checks")
            execute_checks(
                [c for c in pipeline.data_quality.checks if c.stage == "pre_extract"],
                pipeline.input.connection,
                dq_context(),
            )
            finish_step()

            begin_step("process_batches", "Extract, transform, validate, and load batches")
            if pipeline.output.write.mode == "replace":
                self._retry(lambda: target.truncate_target(pipeline.output), pipeline)
            batch_number = 0
            for batch in source.extract_batches(pipeline.input, checkpoint, upper_cursor):
                batch_number += 1
                metrics["extracted_rows"] += len(batch)
                emit(
                    f"Extracted batch {batch_number}: {len(batch)} rows",
                    step=active_step,
                    status="RUNNING",
                    batch=batch_number,
                    rows=len(batch),
                )
                transformed = apply_transformations(batch, pipeline.transformations)
                metrics["transformed_rows"] += len(transformed)
                target.prepare_staging(pipeline.output, staging_table, transformed)
                batch_checks = [
                    check
                    for check in pipeline.data_quality.checks
                    if check.stage in {"post_extract", "post_transform", "pre_load"}
                ]
                dq_results = execute_checks(
                    batch_checks, pipeline.output.connection, dq_context(staging_table)
                )
                rejected_keys: set[tuple[Any, ...]] = set()
                rejected_rows: set[str] = set()
                keys = pipeline.output.write.merge_keys
                for result in dq_results:
                    for row in result.failing_rows:
                        if keys:
                            rejected_keys.add(tuple(row.get(key) for key in keys))
                        else:
                            rejected_rows.add(json.dumps(row, sort_keys=True, default=str))
                if keys:
                    valid_rows = [
                        row
                        for row in transformed
                        if tuple(row.get(key) for key in keys) not in rejected_keys
                    ]
                else:
                    valid_rows = [
                        row
                        for row in transformed
                        if json.dumps(row, sort_keys=True, default=str) not in rejected_rows
                    ]
                loaded = self._retry(
                    lambda rows=valid_rows: target.load(pipeline.output, rows), pipeline
                )
                metrics["loaded_rows"] += loaded
                target.drop_staging(pipeline.output, staging_table)
                emit(
                    f"Loaded batch {batch_number}: {loaded} rows",
                    step=active_step,
                    status="RUNNING",
                    batch=batch_number,
                    rows=loaded,
                )
            finish_step(input_rows=metrics["extracted_rows"], output_rows=metrics["loaded_rows"])

            begin_step("post_load_dq", "Run post-load data-quality checks")
            execute_checks(
                [c for c in pipeline.data_quality.checks if c.stage == "post_load"],
                pipeline.output.connection,
                dq_context(),
            )
            finish_step()

            begin_step("reconciliation", "Reconcile source and target")
            execute_checks(
                [c for c in pipeline.data_quality.checks if c.stage == "reconciliation"],
                pipeline.output.connection,
                dq_context(),
            )
            finish_step()

            begin_step("checkpoint", "Save successful checkpoint")
            if pipeline.checkpoint.enabled and upper_cursor:
                metadata.save_checkpoint(pipeline_id, upper_cursor)
            finish_step()

            elapsed = round(time.monotonic() - started, 3)
            metrics["elapsed_seconds"] = elapsed
            metadata.finish_run(run_id, "SUCCESS", metrics)
            emit("Pipeline completed successfully", step="complete", status="SUCCESS", **metrics)
            return RunSummary(
                run_id,
                pipeline_id,
                "SUCCESS",
                metrics["extracted_rows"],
                metrics["transformed_rows"],
                metrics["loaded_rows"],
                metrics["rejected_rows"],
                elapsed,
            )
        except Exception as exc:
            if active_step:
                finish_step("FAILED", error=str(exc))
            elapsed = round(time.monotonic() - started, 3)
            metrics["elapsed_seconds"] = elapsed
            metadata.finish_run(run_id, "FAILED", metrics, str(exc))
            emit("Pipeline failed", "ERROR", active_step, "FAILED", error=str(exc))
            raise
