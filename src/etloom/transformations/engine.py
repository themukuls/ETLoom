from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from etloom.config.models import TransformationSpec


def _cast(value: Any, data_type: str) -> Any:
    if value is None:
        return None
    converters = {
        "string": str,
        "integer": int,
        "float": float,
        "decimal": Decimal,
        "boolean": bool,
        "date": lambda item: (
            item.date() if isinstance(item, datetime) else date.fromisoformat(str(item))
        ),
        "timestamp": lambda item: (
            item if isinstance(item, datetime) else datetime.fromisoformat(str(item))
        ),
    }
    if data_type not in converters:
        raise ValueError(f"Unsupported cast type: {data_type}")
    return converters[data_type](value)


def _derive(row: dict[str, Any], step: TransformationSpec) -> None:
    operation = step.operation
    output = step.output_column
    if operation.name == "subtract":
        values = []
        for column in operation.columns:
            value = row.get(column)
            if value is None:
                value = operation.null_handling.get(column, 0)
            values.append(value)
        row[output] = values[0] - values[1]
    elif operation.name == "coalesce":
        row[output] = next(
            (row.get(column) for column in operation.columns if row.get(column) is not None),
            operation.value,
        )
    elif operation.name == "date":
        value = row.get(operation.column)
        row[output] = (
            value.date() if isinstance(value, datetime) else date.fromisoformat(str(value))
        )
    elif operation.name == "constant":
        row[output] = operation.value


def apply_transformations(
    rows: list[dict[str, Any]], transformations: list[TransformationSpec]
) -> list[dict[str, Any]]:
    result = [row.copy() for row in rows]
    for step in transformations:
        if step.type == "derive_column":
            for row in result:
                _derive(row, step)
        elif step.type == "rename_columns":
            for row in result:
                for old, new in step.columns.items():
                    if old in row:
                        row[new] = row.pop(old)
        elif step.type == "cast_columns":
            for row in result:
                for column, data_type in step.columns.items():
                    row[column] = _cast(row.get(column), data_type)
        elif step.type == "deduplicate":
            if step.retain:
                reverse = step.retain.direction == "descending"
                result.sort(key=lambda row: row.get(step.retain.order_by), reverse=reverse)
            unique: dict[tuple[Any, ...], dict[str, Any]] = {}
            for row in result:
                key = tuple(row.get(column) for column in step.keys)
                if key not in unique:
                    unique[key] = row
            result = list(unique.values())
    return result
