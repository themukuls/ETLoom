import unittest

from yamlflow.config.models import QualityConfig, QualityRule
from yamlflow.quality.engine import QualityFailure, evaluate_quality


class QualityTests(unittest.TestCase):
    def test_quarantines_invalid_rows(self):
        config = QualityConfig(
            row_level=[
                QualityRule(
                    name="id_required", type="not_null", columns=["id"], on_failure="quarantine"
                )
            ]
        )
        result = evaluate_quality([{"id": "1"}, {"id": None}], config)
        self.assertEqual([{"id": "1"}], result.valid_rows)
        self.assertEqual(1, len(result.rejected))

    def test_fail_rule_stops_pipeline(self):
        config = QualityConfig(
            row_level=[
                QualityRule(
                    name="amount_positive",
                    type="range",
                    columns=["amount"],
                    params={"minimum": 0},
                    on_failure="fail",
                )
            ]
        )
        with self.assertRaises(QualityFailure):
            evaluate_quality([{"amount": -1}], config)


if __name__ == "__main__":
    unittest.main()
