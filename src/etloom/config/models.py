from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class ConnectionOptions(StrictModel):
    sslmode: str = "prefer"
    connect_timeout_seconds: int = Field(default=10, ge=1, le=120)
    application_name: str = "etloom"


class ConnectionPool(StrictModel):
    minimum_connections: int = Field(default=1, ge=1)
    maximum_connections: int = Field(default=5, ge=1)

    @model_validator(mode="after")
    def validate_size(self) -> ConnectionPool:
        if self.minimum_connections > self.maximum_connections:
            raise ValueError("minimum_connections cannot exceed maximum_connections")
        return self


class PostgresConnection(StrictModel):
    type: Literal["postgresql"]
    host: str
    port: int = 5432
    database: str
    username: str
    password: str
    options: ConnectionOptions = Field(default_factory=ConnectionOptions)
    pool: ConnectionPool = Field(default_factory=ConnectionPool)

    def dsn_parameters(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.username,
            "password": self.password,
            "sslmode": self.options.sslmode,
            "connect_timeout": self.options.connect_timeout_seconds,
            "application_name": self.options.application_name,
        }


class ConnectionsFile(StrictModel):
    api_version: Literal["etloom/v1"]
    kind: Literal["connections"]
    connections: dict[str, PostgresConnection]


class PipelineMetadata(StrictModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    name: str
    description: str = ""
    owner: str
    tags: list[str] = Field(default_factory=list)
    enabled: bool = True


class ParameterSpec(StrictModel):
    type: Literal["string", "integer", "float", "boolean", "date", "timestamp"]
    required: bool = False
    default: Any = None


class DatabaseObject(StrictModel):
    schema_name: str = Field(alias="schema")
    table: str


class InputFilter(StrictModel):
    sql: str


class TieBreaker(StrictModel):
    column: str


class IncrementalConfig(StrictModel):
    strategy: Literal["watermark"] = "watermark"
    watermark_column: str
    initial_value: str | int | float | None = None
    lookback_seconds: int = Field(default=0, ge=0, le=86400)
    tie_breaker: TieBreaker | None = None


class OrderBy(StrictModel):
    column: str
    direction: Literal["ascending", "descending"] = "ascending"


class ExtractionConfig(StrictModel):
    mode: Literal["full", "incremental"] = "full"
    incremental: IncrementalConfig | None = None
    order_by: list[OrderBy] = Field(default_factory=list)
    batch_size: int = Field(default=5000, ge=1, le=100000)
    fetch_timeout_seconds: int = Field(default=300, ge=1)

    @model_validator(mode="after")
    def validate_incremental(self) -> ExtractionConfig:
        if self.mode == "incremental" and self.incremental is None:
            raise ValueError("incremental extraction requires incremental settings")
        return self


class InputConfig(StrictModel):
    type: Literal["postgresql"]
    connection: str
    object: DatabaseObject
    columns: list[str]
    filter: InputFilter | None = None
    extraction: ExtractionConfig = Field(default_factory=ExtractionConfig)


class TransformationOperation(StrictModel):
    name: Literal["subtract", "coalesce", "date", "constant"]
    columns: list[str] = Field(default_factory=list)
    column: str | None = None
    value: Any = None
    null_handling: dict[str, Any] = Field(default_factory=dict)


class RetainConfig(StrictModel):
    order_by: str
    direction: Literal["ascending", "descending"] = "descending"


class TransformationSpec(StrictModel):
    id: str
    type: Literal["derive_column", "deduplicate", "rename_columns", "cast_columns"]
    output_column: str | None = None
    operation: TransformationOperation | None = None
    keys: list[str] = Field(default_factory=list)
    retain: RetainConfig | None = None
    columns: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_shape(self) -> TransformationSpec:
        if self.type == "derive_column" and (not self.output_column or not self.operation):
            raise ValueError("derive_column requires output_column and operation")
        if self.type == "deduplicate" and not self.keys:
            raise ValueError("deduplicate requires keys")
        return self


class StagingConfig(StrictModel):
    enabled: bool = True
    schema_name: str = Field(default="etloom_staging", alias="schema")
    table_prefix: str


class TransactionConfig(StrictModel):
    atomic: bool = True
    isolation_level: Literal["read_committed", "repeatable_read", "serializable"] = "read_committed"


class SchemaPolicy(StrictModel):
    create_table_if_missing: bool = False
    allow_new_columns: bool = False
    allow_missing_columns: bool = False
    allow_type_changes: bool = False


class WriteConfig(StrictModel):
    mode: Literal["append", "merge", "replace"] = "append"
    merge_keys: list[str] = Field(default_factory=list)
    update_columns: list[str] = Field(default_factory=list)
    staging: StagingConfig
    transaction: TransactionConfig = Field(default_factory=TransactionConfig)
    schema_policy: SchemaPolicy = Field(default_factory=SchemaPolicy)

    @model_validator(mode="after")
    def validate_merge(self) -> WriteConfig:
        if self.mode == "merge" and not self.merge_keys:
            raise ValueError("merge mode requires merge_keys")
        return self


class OutputConfig(StrictModel):
    type: Literal["postgresql"]
    connection: str
    object: DatabaseObject
    write: WriteConfig


class DQDefaults(StrictModel):
    timeout_seconds: int = Field(default=60, ge=1)
    on_error: Literal["fail", "warn"] = "fail"


class DQExpectation(StrictModel):
    column: str | None = None
    operator: Literal[
        "equals",
        "not_equals",
        "greater_than",
        "greater_than_or_equal",
        "less_than",
        "less_than_or_equal",
        "between",
        "row_count_equals",
    ]
    value: Any = None
    source_column: str | None = None
    target_column: str | None = None


class DQQuery(StrictModel):
    connection: str
    sql: str


class DQCheck(StrictModel):
    id: str
    name: str
    stage: Literal[
        "pre_extract",
        "post_extract",
        "post_transform",
        "pre_load",
        "post_load",
        "reconciliation",
    ]
    type: Literal["scalar_sql", "failing_rows_sql", "compare_sql"]
    connection: str | None = None
    sql: str | None = None
    source: DQQuery | None = None
    target: DQQuery | None = None
    expectation: DQExpectation
    severity: Literal["info", "warning", "error"] = "error"
    on_failure: Literal["fail", "warn", "quarantine"] = "fail"

    @model_validator(mode="after")
    def validate_query(self) -> DQCheck:
        if self.type == "compare_sql":
            if not self.source or not self.target:
                raise ValueError("compare_sql requires source and target queries")
        elif not self.sql:
            raise ValueError(f"{self.type} requires sql")
        if self.on_failure == "quarantine" and self.type != "failing_rows_sql":
            raise ValueError("quarantine is supported only for failing_rows_sql")
        return self


class DataQualityConfig(StrictModel):
    defaults: DQDefaults = Field(default_factory=DQDefaults)
    checks: list[DQCheck] = Field(default_factory=list)


class QuarantineConfig(StrictModel):
    enabled: bool = True
    connection: str
    object: DatabaseObject
    include: list[str] = Field(default_factory=list)


class CheckpointConfig(StrictModel):
    enabled: bool = True
    connection: str
    update_policy: Literal["after_successful_commit"] = "after_successful_commit"


class RetryConfig(StrictModel):
    enabled: bool = True
    maximum_attempts: int = Field(default=3, ge=1, le=10)
    initial_delay_seconds: float = Field(default=2, ge=0)
    backoff_multiplier: float = Field(default=2, ge=1)
    maximum_delay_seconds: float = Field(default=60, ge=0)


class FailurePolicy(StrictModel):
    rollback_target: bool = True
    preserve_staging_data: bool = True
    advance_checkpoint: bool = False


class ResourceConfig(StrictModel):
    maximum_memory_mb: int = Field(default=1024, ge=128)
    maximum_parallel_batches: int = Field(default=1, ge=1)


class RuntimeConfig(StrictModel):
    timeout_seconds: int = Field(default=1800, ge=1)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    failure_policy: FailurePolicy = Field(default_factory=FailurePolicy)
    resources: ResourceConfig = Field(default_factory=ResourceConfig)


class LoggingConfig(StrictModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    format: Literal["text", "json"] = "json"


class MetricsConfig(StrictModel):
    enabled: bool = True
    capture: list[str] = Field(default_factory=list)


class AuditConfig(StrictModel):
    enabled: bool = True
    retain_run_history_days: int = Field(default=90, ge=1)


class ObservabilityConfig(StrictModel):
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    audit: AuditConfig = Field(default_factory=AuditConfig)


class PipelineFile(StrictModel):
    api_version: Literal["etloom/v1"]
    kind: Literal["pipeline"]
    metadata: PipelineMetadata
    parameters: dict[str, ParameterSpec] = Field(default_factory=dict)
    input: InputConfig
    transformations: list[TransformationSpec] = Field(default_factory=list)
    output: OutputConfig
    data_quality: DataQualityConfig = Field(default_factory=DataQualityConfig)
    quarantine: QuarantineConfig
    checkpoint: CheckpointConfig
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    @model_validator(mode="after")
    def validate_references(self) -> PipelineFile:
        if self.input.extraction.mode == "incremental":
            cursor = self.input.extraction.incremental.watermark_column
            if cursor not in self.input.columns:
                raise ValueError(f"watermark column {cursor!r} must be selected from input")
        return self
