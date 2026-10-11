from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PostgresConnection(StrictModel):
    type: Literal["postgres"]
    host: str
    port: int = 5432
    database: str
    username: str
    password: str
    sslmode: str = "prefer"
    connect_timeout_seconds: int = Field(default=10, ge=1, le=120)

    def dsn_parameters(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.username,
            "password": self.password,
            "sslmode": self.sslmode,
            "connect_timeout": self.connect_timeout_seconds,
        }


class ConnectionsFile(StrictModel):
    version: Literal[1]
    connections: dict[str, PostgresConnection]


class PipelineIdentity(StrictModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    name: str
    description: str = ""
    owner: str
    tags: list[str] = Field(default_factory=list)
    enabled: bool = True


class IncrementalConfig(StrictModel):
    strategy: Literal["watermark"] = "watermark"
    column: str
    initial_value: str | int | float | None = None
    lookback_seconds: int = Field(default=0, ge=0, le=86400)


class ExtractionConfig(StrictModel):
    chunk_size: int = Field(default=5000, ge=1, le=100000)
    order_by: list[str] = Field(default_factory=list)


class SourceConfig(StrictModel):
    connection: str
    type: Literal["table"] = "table"
    schema_name: str = Field(alias="schema")
    table: str
    columns: list[str]
    incremental: IncrementalConfig | None = None
    extraction: ExtractionConfig = Field(default_factory=ExtractionConfig)

    @field_validator("columns")
    @classmethod
    def require_columns(cls, value: list[str]) -> list[str]:
        if not value or len(value) != len(set(value)):
            raise ValueError("source columns must be non-empty and unique")
        return value


class TransformationSpec(StrictModel):
    name: str
    type: Literal["filter", "cast", "rename", "derive", "deduplicate"]
    params: dict[str, Any] = Field(default_factory=dict)


class QualityRule(StrictModel):
    name: str
    type: Literal["not_null", "range", "accepted_values", "uniqueness", "row_count"]
    columns: list[str] = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)
    on_failure: Literal["warn", "quarantine", "fail"] = "fail"

    @model_validator(mode="after")
    def validate_columns(self) -> QualityRule:
        if self.type != "row_count" and not self.columns:
            raise ValueError(f"quality rule {self.name!r} requires columns")
        return self


class QualityConfig(StrictModel):
    row_level: list[QualityRule] = Field(default_factory=list)
    dataset_level: list[QualityRule] = Field(default_factory=list)


class LoadConfig(StrictModel):
    mode: Literal["append", "merge", "replace"] = "append"
    keys: list[str] = Field(default_factory=list)
    batch_size: int = Field(default=5000, ge=1, le=100000)

    @model_validator(mode="after")
    def merge_requires_keys(self) -> LoadConfig:
        if self.mode == "merge" and not self.keys:
            raise ValueError("merge load mode requires at least one key")
        return self


class TargetConfig(StrictModel):
    connection: str
    type: Literal["table"] = "table"
    schema_name: str = Field(alias="schema")
    table: str
    load: LoadConfig = Field(default_factory=LoadConfig)


class RuntimeConfig(StrictModel):
    max_retries: int = Field(default=3, ge=0, le=10)
    retry_delay_seconds: float = Field(default=2.0, ge=0, le=300)
    retry_backoff_multiplier: float = Field(default=2.0, ge=1, le=10)


class PipelineFile(StrictModel):
    version: Literal[1]
    pipeline: PipelineIdentity
    source: SourceConfig
    transformations: list[TransformationSpec] = Field(default_factory=list)
    quality: QualityConfig = Field(default_factory=QualityConfig)
    target: TargetConfig
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)

    @model_validator(mode="after")
    def validate_cross_references(self) -> PipelineFile:
        if self.source.incremental:
            cursor = self.source.incremental.column
            if cursor not in self.source.columns:
                raise ValueError(f"incremental column {cursor!r} must be selected from source")
        missing_keys = set(self.target.load.keys) - set(self.source.columns)
        if missing_keys:
            raise ValueError(f"target keys missing from source columns: {sorted(missing_keys)}")
        return self
