from datetime import UTC, datetime
from decimal import Decimal

from etloom.config.models import TransformationOperation, TransformationSpec
from etloom.transformations.engine import apply_transformations


def test_derive_date_subtract_and_deduplicate():
    timestamp = datetime(2026, 10, 9, 10, 0, tzinfo=UTC)
    rows = [
        {
            "id": "1",
            "amount": Decimal("100"),
            "refund": Decimal("10"),
            "created": timestamp,
            "updated": 2,
        },
        {
            "id": "1",
            "amount": Decimal("80"),
            "refund": None,
            "created": timestamp,
            "updated": 1,
        },
    ]
    steps = [
        TransformationSpec(
            id="net",
            type="derive_column",
            output_column="net",
            operation=TransformationOperation(
                name="subtract",
                columns=["amount", "refund"],
                null_handling={"refund": 0},
            ),
        ),
        TransformationSpec(
            id="day",
            type="derive_column",
            output_column="created_date",
            operation=TransformationOperation(name="date", column="created"),
        ),
        TransformationSpec(
            id="dedupe",
            type="deduplicate",
            keys=["id"],
            retain={"order_by": "updated", "direction": "descending"},
        ),
    ]
    result = apply_transformations(rows, steps)
    assert len(result) == 1
    assert result[0]["net"] == Decimal("90")
    assert result[0]["created_date"] == timestamp.date()
