import unittest
from datetime import UTC, datetime
from decimal import Decimal

from yamlflow.config.models import TransformationSpec
from yamlflow.transformations.engine import apply_transformations


class TransformationTests(unittest.TestCase):
    def test_filter_derive_and_deduplicate(self):
        timestamp = datetime(2026, 10, 9, 10, 0, tzinfo=UTC)
        rows = [
            {
                "id": "1",
                "status": "SUCCESS",
                "amount": Decimal("100"),
                "refund": Decimal("10"),
                "created": timestamp,
            },
            {
                "id": "1",
                "status": "SUCCESS",
                "amount": Decimal("100"),
                "refund": Decimal("10"),
                "created": timestamp,
            },
            {
                "id": "2",
                "status": "FAILED",
                "amount": Decimal("50"),
                "refund": None,
                "created": timestamp,
            },
        ]
        steps = [
            TransformationSpec(
                name="success",
                type="filter",
                params={"column": "status", "operator": "equals", "value": "SUCCESS"},
            ),
            TransformationSpec(
                name="net",
                type="derive",
                params={
                    "operation": "subtract",
                    "columns": ["amount", "refund"],
                    "output_column": "net",
                },
            ),
            TransformationSpec(
                name="day",
                type="derive",
                params={
                    "operation": "date",
                    "columns": ["created"],
                    "output_column": "created_date",
                },
            ),
            TransformationSpec(name="dedupe", type="deduplicate", params={"keys": ["id"]}),
        ]
        result = apply_transformations(rows, steps)
        self.assertEqual(1, len(result))
        self.assertEqual(Decimal("90"), result[0]["net"])
        self.assertEqual(timestamp.date(), result[0]["created_date"])


if __name__ == "__main__":
    unittest.main()
