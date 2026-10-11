from datetime import date

from etloom.config.models import DQCheck, DQExpectation
from etloom.quality.engine import DQContext, render_sql, run_check


class FakeConnector:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def query(self, query, parameters=None):
        self.calls.append((query, parameters))
        return self.rows


def context():
    return DQContext(
        source_schema="source",
        source_table="transactions",
        target_schema="analytics",
        target_table="clean_transactions",
        staging_schema="etloom_staging",
        staging_table="run_123",
        execution_date=date(2026, 10, 11),
        current_watermark="2026-10-11T00:00:00Z",
    )


def test_sql_renderer_allows_known_placeholders():
    query, parameters = render_sql(
        "SELECT * FROM {{ source }} WHERE updated_at <= {{ current_watermark }}", context()
    )
    assert '"source"."transactions"' in query
    assert "%s" in query
    assert parameters == ["2026-10-11T00:00:00Z"]


def test_scalar_sql_check_passes():
    connector = FakeConnector([{"failed_count": 0}])
    check = DQCheck(
        id="no_nulls",
        name="No null keys",
        stage="post_load",
        type="scalar_sql",
        sql="SELECT 0 AS failed_count FROM {{ target }} LIMIT 1",
        expectation=DQExpectation(column="failed_count", operator="equals", value=0),
    )
    result = run_check(check, lambda _: connector, "target", context())
    assert result.passed is True


def test_failing_rows_are_returned_for_quarantine():
    connector = FakeConnector([{"id": "bad"}])
    check = DQCheck(
        id="bad_rows",
        name="Bad rows",
        stage="post_transform",
        type="failing_rows_sql",
        sql="SELECT * FROM {{ staging }} WHERE amount < 0",
        expectation=DQExpectation(operator="row_count_equals", value=0),
        on_failure="quarantine",
    )
    result = run_check(check, lambda _: connector, "target", context())
    assert result.passed is False
    assert result.failing_rows == [{"id": "bad"}]
