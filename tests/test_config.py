import os
import tempfile
import unittest
from pathlib import Path

from pydantic import ValidationError

from yamlflow.config.loader import load_connections, load_pipeline

ROOT = Path(__file__).resolve().parents[1]


class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        for name in ("SOURCE_DB_PASSWORD", "TARGET_DB_PASSWORD", "CONTROL_DB_PASSWORD"):
            os.environ[name] = "test-password"

    def test_example_configuration_is_valid(self):
        connections = load_connections(ROOT / "config/connections.example.yaml")
        pipeline = load_pipeline(ROOT / "config/pipelines/clean_transactions.yaml")
        self.assertIn("source_postgres", connections.connections)
        self.assertEqual("clean_transactions", pipeline.pipeline.id)
        self.assertEqual("merge", pipeline.target.load.mode)

    def test_unknown_fields_are_rejected(self):
        content = """
version: 1
pipeline:
  id: invalid_pipeline
  name: Invalid
  owner: test
  surprise: true
source:
  connection: source
  schema: public
  table: records
  columns: [id]
target:
  connection: target
  schema: public
  table: records
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pipeline.yaml"
            path.write_text(content, encoding="utf-8")
            with self.assertRaises(ValidationError):
                load_pipeline(path)


if __name__ == "__main__":
    unittest.main()
