from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, TypeVar

import yaml
from pydantic import BaseModel

from yamlflow.config.models import ConnectionsFile, PipelineFile

T = TypeVar("T", bound=BaseModel)
ENV_PATTERN = re.compile(r"\$\{([A-Z][A-Z0-9_]*)(?::-([^}]*))?\}")


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


def _load(path: str | Path, model: type[T]) -> T:
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    raw = yaml.safe_load(file_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Expected a YAML mapping in {file_path}")
    return model.model_validate(_expand_env(raw))


def load_connections(path: str | Path) -> ConnectionsFile:
    return _load(path, ConnectionsFile)


def load_pipeline(path: str | Path) -> PipelineFile:
    return _load(path, PipelineFile)
