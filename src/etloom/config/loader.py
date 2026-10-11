from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, TypeVar

import yaml
from pydantic import BaseModel

from etloom.config.models import ConnectionsFile, PipelineFile

T = TypeVar("T", bound=BaseModel)
ENV_PATTERN = re.compile(r"\$\{([A-Z][A-Z0-9_]*)(?::-([^}]*))?\}")


def load_local_environment(path: str | Path = ".env") -> None:
    """Load simple KEY=VALUE entries without replacing existing environment variables."""
    env_path = Path(path)
    if not env_path.is_file():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip().strip("'\"")
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            os.environ.setdefault(name, value)


def _expand_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if not isinstance(value, str):
        return value

    def replace(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        raise ValueError(f"Required environment variable {name!r} is not set")

    return ENV_PATTERN.sub(replace, value)


def load_yaml_text(content: str, model: type[T], expand_environment: bool = True) -> T:
    raw = yaml.safe_load(content)
    if not isinstance(raw, dict):
        raise ValueError("Expected a YAML mapping")
    if expand_environment:
        raw = _expand_env(raw)
    return model.model_validate(raw)


def _load(path: str | Path, model: type[T], expand_environment: bool = True) -> T:
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    return load_yaml_text(file_path.read_text(encoding="utf-8"), model, expand_environment)


def load_connections(path: str | Path) -> ConnectionsFile:
    load_local_environment()
    return _load(path, ConnectionsFile)


def load_pipeline(path: str | Path) -> PipelineFile:
    return _load(path, PipelineFile)


def validate_pipeline_text(content: str) -> PipelineFile:
    return load_yaml_text(content, PipelineFile, expand_environment=False)
