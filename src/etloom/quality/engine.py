from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from etloom.config.models import DQCheck, DQExpectation
from etloom.connectors.postgres import PostgresConnector, qualified_name

PLACEHOLDER = re.compile(r"\{\{\s*([a-z_]+)\s*\}\}")


class QualityFailure(RuntimeError):
    pass


@dataclass
class DQContext:
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    staging_schema: str | None = None
    staging_table: str | None = None
    pipeline_id: str = ""
    run_id: str = ""
    execution_date: date = field(default_factory=date.today)
    previous_watermark: Any = None
    current_watermark: Any = None


@dataclass
class DQResult:
    check_id: str
    passed: bool
    message: str
    observed: Any = None
    failing_rows: list[dict[str, Any]] = field(default_factory=list)


def render_sql(sql: str, context: DQContext) -> tuple[str, list[Any]]:
    identifiers = {
        "source": qualified_name(context.source_schema, context.source_table),
        "target": qualified_name(context.target_schema, context.target_table),
    }
    if context.staging_schema and context.staging_table:
        identifiers["staging"] = qualified_name(context.staging_schema, context.staging_table)
    values = {
        "run_id": context.run_id,
        "pipeline_id": context.pipeline_id,
        "execution_date": context.execution_date,
        "previous_watermark": context.previous_watermark,
        "current_watermark": context.current_watermark,
    }
    parameters: list[Any] = []

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in identifiers:
            return identifiers[name]
        if name in values:
            parameters.append(values[name])
            return "%s"
        raise ValueError(f"Unsupported SQL placeholder: {name}")

    return PLACEHOLDER.sub(replace, sql), parameters


def _compare(actual: Any, expectation: DQExpectation, expected: Any | None = None) -> bool:
    expected = expectation.value if expected is None else expected
    operations: dict[str, Callable[[], bool]] = {
        "equals": lambda: actual == expected,
        "not_equals": lambda: actual != expected,
        "greater_than": lambda: actual > expected,
        "greater_than_or_equal": lambda: actual >= expected,
        "less_than": lambda: actual < expected,
        "less_than_or_equal": lambda: actual <= expected,
        "between": lambda: expectation.value[0] <= actual <= expectation.value[1],
        "row_count_equals": lambda: actual == expected,
    }
    return operations[expectation.operator]()


def run_check(
    check: DQCheck,
    resolve_connector: Callable[[str], PostgresConnector],
    default_connection: str,
    context: DQContext,
) -> DQResult:
    if check.type == "compare_sql":
        source_query, source_parameters = render_sql(check.source.sql, context)
        target_query, target_parameters = render_sql(check.target.sql, context)
        source_rows = resolve_connector(check.source.connection).query(
            source_query, source_parameters
        )
        target_rows = resolve_connector(check.target.connection).query(
            target_query, target_parameters
        )
        if len(source_rows) != 1 or len(target_rows) != 1:
            raise QualityFailure(f"Compare check {check.id!r} must return one row per query")
        source_value = source_rows[0][check.expectation.source_column]
        target_value = target_rows[0][check.expectation.target_column]
        passed = _compare(source_value, check.expectation, target_value)
        return DQResult(
            check.id,
            passed,
            f"source={source_value!r}, target={target_value!r}",
            {"source": source_value, "target": target_value},
        )

    query, parameters = render_sql(check.sql, context)
    connector = resolve_connector(check.connection or default_connection)
    rows = connector.query(query, parameters)
    if check.type == "failing_rows_sql":
        actual = len(rows)
        passed = _compare(actual, check.expectation)
        return DQResult(
            check.id,
            passed,
            f"failing_rows={actual}",
            actual,
            failing_rows=rows,
        )

    if len(rows) != 1:
        raise QualityFailure(f"Scalar check {check.id!r} must return exactly one row")
    column = check.expectation.column
    if not column or column not in rows[0]:
        raise QualityFailure(f"Scalar check {check.id!r} did not return column {column!r}")
    actual = rows[0][column]
    passed = _compare(actual, check.expectation)
    return DQResult(check.id, passed, f"{column}={actual!r}", actual)
