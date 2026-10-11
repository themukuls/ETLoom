import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from etloom.config.loader import load_connections, load_pipeline, validate_pipeline_text
from etloom.config.writer import save_pipeline

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def database_passwords(monkeypatch):
    for name in ("SOURCE_DB_PASSWORD", "TARGET_DB_PASSWORD", "CONTROL_DB_PASSWORD"):
        monkeypatch.setenv(name, "test-password")


def test_example_configuration_is_valid():
    connections = load_connections(ROOT / "config/connections.example.yaml")
    pipeline = load_pipeline(ROOT / "config/pipelines/clean_transactions.yaml")
    assert "source_postgres" in connections.connections
    assert pipeline.metadata.id == "clean_transactions"
    assert pipeline.output.write.mode == "merge"
    assert len(pipeline.data_quality.checks) == 5


def test_pipeline_template_is_valid():
    pipeline = load_pipeline(ROOT / "config/pipelines/pipeline.template.yaml")
    assert pipeline.api_version == "etloom/v1"
    assert len(pipeline.data_quality.checks) == 6


def test_unknown_fields_are_rejected():
    content = (ROOT / "config/pipelines/clean_transactions.yaml").read_text(encoding="utf-8")
    content = content.replace("  enabled: true", "  enabled: true\n  surprise: true", 1)
    with pytest.raises(ValidationError):
        validate_pipeline_text(content)


def test_atomic_writer_validates_id():
    content = (ROOT / "config/pipelines/clean_transactions.yaml").read_text(encoding="utf-8")
    with tempfile.TemporaryDirectory() as directory:
        target = save_pipeline(Path(directory), "clean_transactions", content)
        assert target.is_file()
        assert load_pipeline(target).metadata.id == "clean_transactions"
        with pytest.raises(ValueError):
            save_pipeline(Path(directory), "different_id", content)


def test_atomic_writer_preserves_comments():
    content = (ROOT / "config/pipelines/clean_transactions.yaml").read_text(encoding="utf-8")
    content = "# retained editor comment\n" + content
    with tempfile.TemporaryDirectory() as directory:
        target = save_pipeline(Path(directory), "clean_transactions", content)
        assert target.read_text(encoding="utf-8").startswith("# retained editor comment\n")
