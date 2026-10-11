from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from pydantic import ValidationError

from etloom.config.loader import load_connections, load_pipeline
from etloom.engine.executor import PipelineExecutor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etloom", description="ETLoom data platform")
    subcommands = parser.add_subparsers(dest="command", required=True)

    validate = subcommands.add_parser("validate", help="Validate an ETLoom pipeline YAML")
    validate.add_argument("pipeline", type=Path)

    run = subcommands.add_parser("run", help="Execute an ETLoom pipeline")
    run.add_argument("pipeline", type=Path)
    run.add_argument("--connections", type=Path, default=Path("config/connections.yaml"))

    serve = subcommands.add_parser("serve", help="Start the ETLoom web application")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--reload", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        if args.command == "serve":
            import uvicorn

            uvicorn.run("etloom.web.app:app", host=args.host, port=args.port, reload=args.reload)
            return

        pipeline = load_pipeline(args.pipeline)
        if args.command == "validate":
            print(f"Valid ETLoom pipeline: {pipeline.metadata.id}")
            return

        connections = load_connections(args.connections)
        yaml_content = args.pipeline.read_text(encoding="utf-8")
        summary = PipelineExecutor(connections).run(pipeline, yaml_content)
        print(json.dumps(summary.to_dict(), indent=2))
    except (ValidationError, ValueError, FileNotFoundError) as exc:
        raise SystemExit(f"Configuration error: {exc}") from exc


if __name__ == "__main__":
    main()
