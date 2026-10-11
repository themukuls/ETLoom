# YAMLFlow ETL

YAMLFlow is a lightweight, self-hosted ETL engine built with Python and PostgreSQL. Pipelines define their source, transformations, quality rules, incremental strategy, and target in version-controlled YAML.

The initial release intentionally focuses on a reliable execution engine rather than a drag-and-drop UI. A future UI can generate the same portable YAML specification.

## Features

- Strict YAML validation with Pydantic
- Environment-variable secrets, never credentials committed to YAML
- PostgreSQL source, target, and control plane
- Chunked extraction and stable upper-bound watermarks
- Lookback windows for late-arriving records
- Idempotent PostgreSQL upserts
- Allow-listed transformations without `eval`
- Row-level quarantine and dataset-level quality gates
- Durable checkpoints, run history, metrics, and rejected records
- Docker Compose demonstration and GitHub Actions CI

## Architecture

```text
YAML/CLI -> Config validation -> PostgreSQL extractor -> Transformations
         -> Quality rules -> PostgreSQL target
         -> Control DB (runs, checkpoints, rejected records)
```

YAMLFlow provides at-least-once extraction and an idempotent merge target. The checkpoint advances only after every target batch succeeds. If a run crashes after loading data but before saving its checkpoint, the next run reprocesses the lookback window and safely upserts the same keys.

## Quick start

Requirements: Python 3.11+, Docker, and Docker Compose.

```bash
git clone <repository-url>
cd yamlflow-etl

python -m venv .venv
```

Activate the environment on Windows:

```powershell
.venv\Scripts\activate
```

On macOS or Linux:

```bash
source .venv/bin/activate
```

Install YAMLFlow:

```bash
pip install -e ".[dev]"
```

Start PostgreSQL with seeded source data and an empty target:

```bash
docker compose up -d
```

Create local configuration:

```bash
cp .env.example .env
cp config/connections.example.yaml config/connections.yaml
```

Load environment variables in your terminal, or configure `.env` through VS Code. Then validate and execute:

```bash
yamlflow validate config/pipelines/clean_transactions.yaml
yamlflow run config/pipelines/clean_transactions.yaml
```

Expected target records:

```sql
SELECT *
FROM analytics.clean_transactions
ORDER BY transaction_id;
```

Only the two valid successful transactions are loaded. The failed payment is filtered out, while records with a missing merchant or negative amount are written to `etl_control.yamlflow.rejected_records`.

## YAML contract

See [`config/pipelines/clean_transactions.yaml`](config/pipelines/clean_transactions.yaml) for a complete pipeline and [`config/connections.example.yaml`](config/connections.example.yaml) for connection definitions.

Supported transformations in v0.1:

| Transformation | Purpose |
|---|---|
| `filter` | Keep records matching an allow-listed comparison |
| `cast` | Convert columns to supported Python/PostgreSQL-friendly types |
| `rename` | Rename one or more columns |
| `derive` | Subtract, coalesce, derive a date, or set a constant |
| `deduplicate` | Retain one record per configured key within a batch |

Supported quality rules:

| Rule | Purpose |
|---|---|
| `not_null` | Require values |
| `range` | Enforce minimum/maximum values |
| `accepted_values` | Restrict categorical values |
| `uniqueness` | Validate key uniqueness |
| `row_count` | Validate dataset size |

Failure actions are `warn`, `quarantine`, and `fail`.

## Local development

```bash
ruff check .
pytest
```

Reset the complete demonstration environment:

```bash
docker compose down -v
docker compose up -d
```

## Security decisions

- SQL identifiers are validated and quoted.
- Values use parameterized queries.
- YAML cannot execute Python code.
- Secrets are resolved from environment variables.
- Source credentials should be read-only in non-demo deployments.
- The project does not expose arbitrary SQL or arbitrary network connections through a public UI.

## Roadmap

- CSV and REST connectors
- PostgreSQL-backed scheduler and worker queue
- FastAPI control plane
- Web-based pipeline builder
- Schema-drift policies and column-level lineage
- Prometheus metrics and OpenTelemetry traces
- Connector plugin SDK

## License

Apache License 2.0.

