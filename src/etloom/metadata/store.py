from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from etloom.connectors.postgres import PostgresConnector


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _json(value: Any) -> str:
    return json.dumps(value, default=_json_default)


class MetadataStore:
    def __init__(self, connector: PostgresConnector):
        self.connector = connector

    def initialize(self) -> None:
        statements = [
            "CREATE SCHEMA IF NOT EXISTS etloom",
            """
            CREATE TABLE IF NOT EXISTS etloom.pipeline_runs (
                run_id UUID PRIMARY KEY,
                pipeline_id TEXT NOT NULL,
                status TEXT NOT NULL,
                current_step TEXT,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ,
                extracted_rows BIGINT NOT NULL DEFAULT 0,
                transformed_rows BIGINT NOT NULL DEFAULT 0,
                loaded_rows BIGINT NOT NULL DEFAULT 0,
                rejected_rows BIGINT NOT NULL DEFAULT 0,
                error_message TEXT,
                metrics JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS etloom.pipeline_run_steps (
                run_id UUID NOT NULL,
                step_id TEXT NOT NULL,
                step_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ,
                duration_ms BIGINT,
                input_rows BIGINT,
                output_rows BIGINT,
                error_message TEXT,
                PRIMARY KEY (run_id, step_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS etloom.run_events (
                event_id BIGSERIAL PRIMARY KEY,
                run_id UUID NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                level TEXT NOT NULL,
                step_id TEXT,
                status TEXT,
                message TEXT NOT NULL,
                details JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS run_events_run_id_event_id_idx
            ON etloom.run_events (run_id, event_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS etloom.pipeline_checkpoints (
                pipeline_id TEXT PRIMARY KEY,
                checkpoint JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS etloom.rejected_records (
                id BIGSERIAL PRIMARY KEY,
                run_id UUID NOT NULL,
                pipeline_id TEXT NOT NULL,
                check_id TEXT NOT NULL,
                rejection_reason TEXT NOT NULL,
                source_record JSONB NOT NULL,
                rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS etloom.pipeline_versions (
                pipeline_id TEXT NOT NULL,
                version BIGSERIAL,
                configuration_checksum TEXT NOT NULL,
                yaml_content TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (pipeline_id, version),
                UNIQUE (pipeline_id, configuration_checksum)
            )
            """,
        ]
        with self.connector.connect() as connection, connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
            connection.commit()

    def start_run(self, run_id: UUID, pipeline_id: str, yaml_content: str = "") -> None:
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO etloom.pipeline_runs (run_id, pipeline_id, status, current_step)
                VALUES (%s, %s, 'RUNNING', 'initializing')
                """,
                (run_id, pipeline_id),
            )
            if yaml_content:
                checksum = hashlib.sha256(yaml_content.encode()).hexdigest()
                cursor.execute(
                    """
                    INSERT INTO etloom.pipeline_versions
                        (pipeline_id, configuration_checksum, yaml_content)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (pipeline_id, configuration_checksum) DO NOTHING
                    """,
                    (pipeline_id, checksum, yaml_content),
                )
            connection.commit()

    def update_run_step(self, run_id: UUID, step_id: str) -> None:
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE etloom.pipeline_runs SET current_step = %s WHERE run_id = %s",
                (step_id, run_id),
            )
            connection.commit()

    def finish_run(
        self, run_id: UUID, status: str, metrics: dict[str, Any], error: str | None = None
    ) -> None:
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE etloom.pipeline_runs
                SET status = %s, current_step = NULL, finished_at = NOW(),
                    extracted_rows = %s, transformed_rows = %s,
                    loaded_rows = %s, rejected_rows = %s,
                    error_message = %s, metrics = %s::jsonb
                WHERE run_id = %s
                """,
                (
                    status,
                    metrics.get("extracted_rows", 0),
                    metrics.get("transformed_rows", 0),
                    metrics.get("loaded_rows", 0),
                    metrics.get("rejected_rows", 0),
                    error,
                    _json(metrics),
                    run_id,
                ),
            )
            connection.commit()

    def start_step(self, run_id: UUID, step_id: str, name: str) -> None:
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO etloom.pipeline_run_steps
                    (run_id, step_id, step_name, status, started_at)
                VALUES (%s, %s, %s, 'RUNNING', NOW())
                ON CONFLICT (run_id, step_id) DO UPDATE
                SET status = 'RUNNING', started_at = NOW(), finished_at = NULL,
                    error_message = NULL
                """,
                (run_id, step_id, name),
            )
            connection.commit()
        self.update_run_step(run_id, step_id)

    def finish_step(
        self,
        run_id: UUID,
        step_id: str,
        status: str,
        duration_ms: int,
        input_rows: int | None = None,
        output_rows: int | None = None,
        error: str | None = None,
    ) -> None:
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE etloom.pipeline_run_steps
                SET status = %s, finished_at = NOW(), duration_ms = %s,
                    input_rows = %s, output_rows = %s, error_message = %s
                WHERE run_id = %s AND step_id = %s
                """,
                (status, duration_ms, input_rows, output_rows, error, run_id, step_id),
            )
            connection.commit()

    def event(
        self,
        run_id: UUID,
        level: str,
        message: str,
        step_id: str | None = None,
        status: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> int:
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO etloom.run_events
                    (run_id, level, step_id, status, message, details)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                RETURNING event_id
                """,
                (run_id, level, step_id, status, message, _json(details or {})),
            )
            event_id = cursor.fetchone()["event_id"]
            connection.commit()
            return event_id

    def get_checkpoint(self, pipeline_id: str) -> dict[str, Any] | None:
        rows = self.connector.query(
            "SELECT checkpoint FROM etloom.pipeline_checkpoints WHERE pipeline_id = %s",
            (pipeline_id,),
        )
        if not rows:
            return None
        checkpoint = rows[0]["checkpoint"]
        if checkpoint.get("watermark_type") == "datetime" and checkpoint.get("watermark"):
            checkpoint["watermark"] = datetime.fromisoformat(checkpoint["watermark"])
        return checkpoint

    def save_checkpoint(self, pipeline_id: str, checkpoint: dict[str, Any]) -> None:
        payload = dict(checkpoint)
        if isinstance(payload.get("watermark"), datetime):
            payload["watermark_type"] = "datetime"
            payload["watermark"] = payload["watermark"].isoformat()
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO etloom.pipeline_checkpoints (pipeline_id, checkpoint)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (pipeline_id) DO UPDATE
                SET checkpoint = EXCLUDED.checkpoint, updated_at = NOW()
                """,
                (pipeline_id, _json(payload)),
            )
            connection.commit()

    def quarantine(
        self,
        run_id: UUID,
        pipeline_id: str,
        check_id: str,
        reason: str,
        records: list[dict[str, Any]],
    ) -> None:
        if not records:
            return
        values = [(run_id, pipeline_id, check_id, reason, _json(record)) for record in records]
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO etloom.rejected_records
                    (run_id, pipeline_id, check_id, rejection_reason, source_record)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                """,
                values,
            )
            connection.commit()

    def get_run(self, run_id: UUID | str) -> dict[str, Any] | None:
        rows = self.connector.query(
            "SELECT * FROM etloom.pipeline_runs WHERE run_id = %s", (run_id,)
        )
        if not rows:
            return None
        run = rows[0]
        run["steps"] = self.connector.query(
            """
            SELECT * FROM etloom.pipeline_run_steps
            WHERE run_id = %s ORDER BY started_at NULLS LAST
            """,
            (run_id,),
        )
        return run

    def recent_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        return self.connector.query(
            "SELECT * FROM etloom.pipeline_runs ORDER BY started_at DESC LIMIT %s", (limit,)
        )

    def events_after(self, run_id: UUID | str, event_id: int = 0) -> list[dict[str, Any]]:
        return self.connector.query(
            """
            SELECT * FROM etloom.run_events
            WHERE run_id = %s AND event_id > %s
            ORDER BY event_id
            """,
            (run_id, event_id),
        )
