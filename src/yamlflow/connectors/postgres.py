from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any

from yamlflow.config.models import PostgresConnection, SourceConfig, TargetConfig

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return f'"{value}"'


class PostgresConnector:
    def __init__(self, config: PostgresConnection):
        self.config = config

    @contextmanager
    def connect(self):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError(
                "Install PostgreSQL support with: pip install 'psycopg[binary]'"
            ) from exc
        with psycopg.connect(**self.config.dsn_parameters(), row_factory=dict_row) as connection:
            yield connection

    def test_connection(self) -> None:
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()

    def maximum_value(self, source: SourceConfig, column: str) -> Any:
        table = f"{quote_identifier(source.schema_name)}.{quote_identifier(source.table)}"
        query = f"SELECT MAX({quote_identifier(column)}) AS maximum FROM {table}"
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            return row["maximum"] if row else None

    def extract_batches(
        self,
        source: SourceConfig,
        checkpoint: Any = None,
        upper_bound: Any = None,
    ) -> Iterator[list[dict[str, Any]]]:
        columns = ", ".join(quote_identifier(column) for column in source.columns)
        table = f"{quote_identifier(source.schema_name)}.{quote_identifier(source.table)}"
        where: list[str] = []
        parameters: list[Any] = []

        if source.incremental and checkpoint is not None:
            lower = checkpoint
            if source.incremental.lookback_seconds and isinstance(lower, datetime):
                lower -= timedelta(seconds=source.incremental.lookback_seconds)
            where.append(f"{quote_identifier(source.incremental.column)} >= %s")
            parameters.append(lower)
        if source.incremental and upper_bound is not None:
            where.append(f"{quote_identifier(source.incremental.column)} <= %s")
            parameters.append(upper_bound)

        query = f"SELECT {columns} FROM {table}"
        if where:
            query += " WHERE " + " AND ".join(where)
        order = source.extraction.order_by
        if source.incremental and source.incremental.column not in order:
            order = [source.incremental.column, *order]
        if order:
            query += " ORDER BY " + ", ".join(quote_identifier(column) for column in order)

        with self.connect() as connection:
            with connection.cursor(name="yamlflow_extract") as cursor:
                cursor.execute(query, parameters)
                while batch := cursor.fetchmany(source.extraction.chunk_size):
                    yield [dict(row) for row in batch]

    def load(self, target: TargetConfig, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        columns = list(rows[0])
        if any(list(row) != columns for row in rows):
            raise ValueError("All target records must contain the same ordered columns")
        table = f"{quote_identifier(target.schema_name)}.{quote_identifier(target.table)}"
        column_sql = ", ".join(quote_identifier(column) for column in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"

        if target.load.mode == "merge":
            keys = target.load.keys
            update_columns = [column for column in columns if column not in keys]
            conflict = ", ".join(quote_identifier(key) for key in keys)
            if update_columns:
                updates = ", ".join(
                    f"{quote_identifier(column)} = EXCLUDED.{quote_identifier(column)}"
                    for column in update_columns
                )
                query += f" ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
            else:
                query += f" ON CONFLICT ({conflict}) DO NOTHING"
        values = [tuple(row[column] for column in columns) for row in rows]
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.executemany(query, values)
            connection.commit()
        return len(rows)

    def truncate(self, target: TargetConfig) -> None:
        table = f"{quote_identifier(target.schema_name)}.{quote_identifier(target.table)}"
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table}")
            connection.commit()
