from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from yamlflow.config.models import TransformationSpec


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


def _compare(value: Any, operator: str, expected: Any) -> bool:
    operations = {
        "equals": lambda: value == expected,
        "not_equals": lambda: value != expected,
        "in": lambda: value in expected,
        "not_in": lambda: value not in expected,
        "greater_than": lambda: value is not None and value > expected,
        "greater_or_equal": lambda: value is not None and value >= expected,
    }
    if operator not in operations:
        raise ValueError(f"Unsupported filter operator: {operator}")
    return operations[operator]()


def _derive(row: dict[str, Any], params: dict[str, Any]) -> None:
    operation = params["operation"]
    output = params["output_column"]
    columns = params.get("columns", [])
    if operation == "subtract":
        left = row.get(columns[0]) or 0
        right = row.get(columns[1]) or 0
        row[output] = left - right
    elif operation == "coalesce":
        row[output] = next(
            (row.get(column) for column in columns if row.get(column) is not None),
            params.get("default"),
        )
    elif operation == "date":
        value = row.get(columns[0])
        row[output] = (
            value.date() if isinstance(value, datetime) else date.fromisoformat(str(value))
        )
    elif operation == "constant":
        row[output] = params.get("value")
    else:
        raise ValueError(f"Unsupported derive operation: {operation}")


def apply_transformations(
    rows: list[dict[str, Any]], transformations: list[TransformationSpec]
) -> list[dict[str, Any]]:
    result = [row.copy() for row in rows]
    for step in transformations:
        params = step.params
        if step.type == "filter":
            result = [
                row
                for row in result
                if _compare(row.get(params["column"]), params["operator"], params.get("value"))
            ]
        elif step.type == "cast":
            for row in result:
                for column, data_type in params["columns"].items():
                    row[column] = _cast(row.get(column), data_type)
        elif step.type == "rename":
            for row in result:
                for old, new in params["columns"].items():
                    if old in row:
                        row[new] = row.pop(old)
        elif step.type == "derive":
            for row in result:
                _derive(row, params)
        elif step.type == "deduplicate":
            keys = params["keys"]
            unique: dict[tuple[Any, ...], dict[str, Any]] = {}
            for row in result:
                unique[tuple(row.get(key) for key in keys)] = row
            result = list(unique.values())
        else:
            raise ValueError(f"Unsupported transformation: {step.type}")
    return result
