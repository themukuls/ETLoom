from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from yamlflow.connectors.postgres import PostgresConnector


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


class MetadataStore:
    def __init__(self, connector: PostgresConnector):
        self.connector = connector

    def initialize(self) -> None:
        statements = [
            """
            CREATE SCHEMA IF NOT EXISTS yamlflow
            """,
            """
            CREATE TABLE IF NOT EXISTS yamlflow.pipeline_runs (
                run_id UUID PRIMARY KEY,
                pipeline_id TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ,
                extracted_rows BIGINT NOT NULL DEFAULT 0,
                loaded_rows BIGINT NOT NULL DEFAULT 0,
                rejected_rows BIGINT NOT NULL DEFAULT 0,
                error_message TEXT,
                metrics JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS yamlflow.pipeline_checkpoints (
                pipeline_id TEXT PRIMARY KEY,
                checkpoint JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS yamlflow.rejected_records (
                id BIGSERIAL PRIMARY KEY,
                run_id UUID NOT NULL,
                pipeline_id TEXT NOT NULL,
                rule_name TEXT NOT NULL,
                reason TEXT NOT NULL,
                record JSONB NOT NULL,
                rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
        ]
        with self.connector.connect() as connection, connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
            connection.commit()

    def start_run(self, run_id: UUID, pipeline_id: str) -> None:
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO yamlflow.pipeline_runs (run_id, pipeline_id, status)
                VALUES (%s, %s, 'RUNNING')
                """,
                (run_id, pipeline_id),
            )
            connection.commit()

    def finish_run(
        self, run_id: UUID, status: str, metrics: dict[str, Any], error: str | None = None
    ) -> None:
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE yamlflow.pipeline_runs
                SET status = %s, finished_at = NOW(), extracted_rows = %s,
                    loaded_rows = %s, rejected_rows = %s, error_message = %s,
                    metrics = %s::jsonb
                WHERE run_id = %s
                """,
                (
                    status,
                    metrics.get("extracted_rows", 0),
                    metrics.get("loaded_rows", 0),
                    metrics.get("rejected_rows", 0),
                    error,
                    json.dumps(metrics, default=_json_default),
                    run_id,
                ),
            )
            connection.commit()

    def get_checkpoint(self, pipeline_id: str) -> Any:
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT checkpoint FROM yamlflow.pipeline_checkpoints WHERE pipeline_id = %s",
                (pipeline_id,),
            )
            row = cursor.fetchone()
        if not row:
            return None
        checkpoint = row["checkpoint"].get("value")
        checkpoint_type = row["checkpoint"].get("type")
        if checkpoint_type == "datetime" and checkpoint is not None:
            return datetime.fromisoformat(checkpoint)
        return checkpoint

    def save_checkpoint(self, pipeline_id: str, value: Any) -> None:
        payload = {
            "value": value.isoformat() if isinstance(value, datetime) else value,
            "type": "datetime" if isinstance(value, datetime) else type(value).__name__,
        }
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO yamlflow.pipeline_checkpoints (pipeline_id, checkpoint)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (pipeline_id) DO UPDATE
                SET checkpoint = EXCLUDED.checkpoint, updated_at = NOW()
                """,
                (pipeline_id, json.dumps(payload, default=_json_default)),
            )
            connection.commit()

    def quarantine(self, run_id: UUID, pipeline_id: str, rejected: list[Any]) -> None:
        if not rejected:
            return
        values = [
            (
                run_id,
                pipeline_id,
                item.rule_name,
                item.reason,
                json.dumps(item.record, default=_json_default),
            )
            for item in rejected
        ]
        with self.connector.connect() as connection, connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO yamlflow.rejected_records
                    (run_id, pipeline_id, rule_name, reason, record)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                """,
                values,
            )
            connection.commit()
