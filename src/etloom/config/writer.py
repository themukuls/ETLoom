from __future__ import annotations

import os
import tempfile
from pathlib import Path

from etloom.config.loader import validate_pipeline_text


def pipeline_path(config_directory: Path, pipeline_id: str) -> Path:
    if (
        not pipeline_id
        or not pipeline_id.replace("_", "").isalnum()
        or not pipeline_id[0].isalpha()
    ):
        raise ValueError("Invalid pipeline ID")
    root = config_directory.resolve()
    target = (root / f"{pipeline_id}.yaml").resolve()
    if target.parent != root:
        raise ValueError("Pipeline path escapes the configuration directory")
    return target


def save_pipeline(config_directory: Path, pipeline_id: str, content: str) -> Path:
    pipeline = validate_pipeline_text(content)
    if pipeline.metadata.id != pipeline_id:
        raise ValueError("Pipeline ID in YAML must match the requested pipeline ID")
    target = pipeline_path(config_directory, pipeline_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized = content.rstrip() + "\n"
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{pipeline_id}-", dir=target.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as temporary:
            temporary.write(normalized)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, target)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)
    return target
