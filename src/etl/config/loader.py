from __future__ import annotations

from pathlib import Path
import yaml

def load_yaml(path: str | Path) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {p}")
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}