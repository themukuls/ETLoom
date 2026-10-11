from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from yamlflow.config.models import QualityConfig, QualityRule


class QualityFailure(RuntimeError):
    pass


@dataclass
class RejectedRecord:
    rule_name: str
    reason: str
    record: dict[str, Any]


@dataclass
class QualityResult:
    valid_rows: list[dict[str, Any]]
    rejected: list[RejectedRecord] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _passes(row: dict[str, Any], rule: QualityRule) -> bool:
    values = [row.get(column) for column in rule.columns]
    if rule.type == "not_null":
        return all(value is not None for value in values)
    if rule.type == "range":
        minimum, maximum = rule.params.get("minimum"), rule.params.get("maximum")
        return all(
            value is not None
            and (minimum is None or value >= minimum)
            and (maximum is None or value <= maximum)
            for value in values
        )
    if rule.type == "accepted_values":
        accepted = set(rule.params["values"])
        return all(value in accepted for value in values)
    return True


def evaluate_quality(rows: list[dict[str, Any]], config: QualityConfig) -> QualityResult:
    active = [True] * len(rows)
    rejected: list[RejectedRecord] = []
    warnings: list[str] = []

    for rule in config.row_level:
        if rule.type == "uniqueness":
            seen: set[tuple[Any, ...]] = set()
            failures = []
            for index, row in enumerate(rows):
                key = tuple(row.get(column) for column in rule.columns)
                if key in seen:
                    failures.append(index)
                seen.add(key)
        else:
            failures = [index for index, row in enumerate(rows) if not _passes(row, rule)]

        if failures and rule.on_failure == "fail":
            raise QualityFailure(f"Rule {rule.name!r} failed for {len(failures)} record(s)")
        if failures and rule.on_failure == "warn":
            warnings.append(f"Rule {rule.name!r} failed for {len(failures)} record(s)")
        if rule.on_failure == "quarantine":
            for index in failures:
                if active[index]:
                    active[index] = False
                    rejected.append(
                        RejectedRecord(rule.name, "row-level quality failure", rows[index])
                    )

    valid = [row for index, row in enumerate(rows) if active[index]]
    for rule in config.dataset_level:
        if rule.type == "row_count":
            minimum = rule.params.get("minimum", 0)
            maximum = rule.params.get("maximum")
            passed = len(valid) >= minimum and (maximum is None or len(valid) <= maximum)
        elif rule.type == "uniqueness":
            keys = [tuple(row.get(column) for column in rule.columns) for row in valid]
            passed = len(keys) == len(set(keys))
        else:
            continue
        if not passed and rule.on_failure == "fail":
            raise QualityFailure(f"Dataset rule {rule.name!r} failed")
        if not passed:
            warnings.append(f"Dataset rule {rule.name!r} failed")

    return QualityResult(valid_rows=valid, rejected=rejected, warnings=warnings)
