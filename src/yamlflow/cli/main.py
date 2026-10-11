from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from pydantic import ValidationError

from yamlflow.config.loader import load_connections, load_pipeline
from yamlflow.engine.executor import PipelineExecutor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="yamlflow", description="YAML-driven PostgreSQL ETL")
    subcommands = parser.add_subparsers(dest="command", required=True)

    validate = subcommands.add_parser("validate", help="Validate connection and pipeline YAML")
    validate.add_argument("pipeline", type=Path)
    validate.add_argument("--connections", type=Path, default=Path("config/connections.yaml"))

    run = subcommands.add_parser("run", help="Execute a pipeline")
    run.add_argument("pipeline", type=Path)
    run.add_argument("--connections", type=Path, default=Path("config/connections.yaml"))
    run.add_argument("--control-connection", default="control_postgres")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        connections = load_connections(args.connections)
        pipeline = load_pipeline(args.pipeline)
        for reference in (pipeline.source.connection, pipeline.target.connection):
            if reference not in connections.connections:
                raise ValueError(f"Pipeline references unknown connection {reference!r}")
        if args.command == "validate":
            print(f"Valid pipeline: {pipeline.pipeline.id}")
            return
        summary = PipelineExecutor(connections, args.control_connection).run(pipeline)
        output = asdict(summary)
        output["run_id"] = str(output["run_id"])
        print(json.dumps(output, indent=2))
    except (ValidationError, ValueError, FileNotFoundError) as exc:
        raise SystemExit(f"Configuration error: {exc}") from exc


if __name__ == "__main__":
    main()
