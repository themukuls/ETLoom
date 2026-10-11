from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any

from etloom.config.models import InputConfig, OutputConfig, PostgresConnection

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return f'"{value}"'


def qualified_name(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


class PostgresConnector:
    def __init__(self, config: PostgresConnection):
        self.config = config

    @contextmanager
    def connect(self):
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(**self.config.dsn_parameters(), row_factory=dict_row) as connection:
            yield connection

    def test_connection(self) -> None:
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()

    def query(self, query: str, parameters: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(query, parameters or ())
            return [dict(row) for row in cursor.fetchall()] if cursor.description else []

    def source_name(self, source: InputConfig) -> str:
        return qualified_name(source.object.schema_name, source.object.table)

    def target_name(self, target: OutputConfig) -> str:
        return qualified_name(target.object.schema_name, target.object.table)

    def maximum_cursor(self, source: InputConfig) -> dict[str, Any] | None:
        incremental = source.extraction.incremental
        if not incremental:
            return None
        watermark = quote_identifier(incremental.watermark_column)
        selected = [f"{watermark} AS watermark"]
        order = [f"{watermark} DESC"]
        if incremental.tie_breaker:
            tie = quote_identifier(incremental.tie_breaker.column)
            selected.append(f"{tie} AS tie_breaker")
            order.append(f"{tie} DESC")
        query = f"SELECT {', '.join(selected)} FROM {self.source_name(source)}"
        if source.filter:
            query += f" WHERE ({source.filter.sql})"
        query += f" ORDER BY {', '.join(order)} LIMIT 1"
        rows = self.query(query)
        return rows[0] if rows else None

    def extract_batches(
        self,
        source: InputConfig,
        checkpoint: dict[str, Any] | None = None,
        upper_cursor: dict[str, Any] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        columns = ", ".join(quote_identifier(column) for column in source.columns)
        where: list[str] = []
        parameters: list[Any] = []
        incremental = source.extraction.incremental

        if source.filter:
            where.append(f"({source.filter.sql})")

        if source.extraction.mode == "incremental" and incremental:
            watermark = quote_identifier(incremental.watermark_column)
            if checkpoint and checkpoint.get("watermark") is not None:
                lower = checkpoint["watermark"]
                if incremental.lookback_seconds and isinstance(lower, datetime):
                    lower -= timedelta(seconds=incremental.lookback_seconds)
                    where.append(f"{watermark} >= %s")
                    parameters.append(lower)
                elif incremental.tie_breaker and checkpoint.get("tie_breaker") is not None:
                    tie = quote_identifier(incremental.tie_breaker.column)
                    where.append(f"({watermark} > %s OR ({watermark} = %s AND {tie} > %s))")
                    parameters.extend([lower, lower, checkpoint["tie_breaker"]])
                else:
                    where.append(f"{watermark} > %s")
                    parameters.append(lower)
            if upper_cursor and upper_cursor.get("watermark") is not None:
                where.append(f"{watermark} <= %s")
                parameters.append(upper_cursor["watermark"])

        query = f"SELECT {columns} FROM {self.source_name(source)}"
        if where:
            query += " WHERE " + " AND ".join(where)
        if source.extraction.order_by:
            order = [
                f"{quote_identifier(item.column)} "
                f"{'ASC' if item.direction == 'ascending' else 'DESC'}"
                for item in source.extraction.order_by
            ]
            query += " ORDER BY " + ", ".join(order)

        with self.connect() as connection:
            with connection.cursor(name="etloom_extract") as cursor:
                cursor.execute(query, parameters)
                while batch := cursor.fetchmany(source.extraction.batch_size):
                    yield [dict(row) for row in batch]

    def prepare_staging(
        self, target: OutputConfig, staging_table: str, rows: Sequence[dict[str, Any]]
    ) -> None:
        schema = target.write.staging.schema_name
        staging = qualified_name(schema, staging_table)
        target_table = self.target_name(target)
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(schema)}")
            cursor.execute(f"DROP TABLE IF EXISTS {staging}")
            cursor.execute(
                f"CREATE UNLOGGED TABLE {staging} (LIKE {target_table} INCLUDING DEFAULTS)"
            )
            connection.commit()
        self.insert_rows(schema, staging_table, rows)

    def insert_rows(self, schema: str, table: str, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        columns = list(rows[0])
        target = qualified_name(schema, table)
        column_sql = ", ".join(quote_identifier(column) for column in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        values = [tuple(row.get(column) for column in columns) for row in rows]
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.executemany(
                f"INSERT INTO {target} ({column_sql}) VALUES ({placeholders})", values
            )
            connection.commit()
        return len(rows)

    def load(self, target: OutputConfig, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        columns = list(rows[0])
        table = self.target_name(target)
        column_sql = ", ".join(quote_identifier(column) for column in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"
        if target.write.mode == "merge":
            conflict = ", ".join(quote_identifier(key) for key in target.write.merge_keys)
            update_columns = target.write.update_columns or [
                column for column in columns if column not in target.write.merge_keys
            ]
            updates = ", ".join(
                f"{quote_identifier(column)} = EXCLUDED.{quote_identifier(column)}"
                for column in update_columns
            )
            query += f" ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
        values = [tuple(row.get(column) for column in columns) for row in rows]
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.executemany(query, values)
            connection.commit()
        return len(rows)

    def truncate_target(self, target: OutputConfig) -> None:
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {self.target_name(target)}")
            connection.commit()

    def drop_staging(self, target: OutputConfig, staging_table: str) -> None:
        staging = qualified_name(target.write.staging.schema_name, staging_table)
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {staging}")
            connection.commit()
