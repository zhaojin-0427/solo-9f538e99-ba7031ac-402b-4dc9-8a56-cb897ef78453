"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_MAX_UPLOAD_BYTES = 10 * 1024 * 1024
DEFAULT_MAX_ROWS = 200_000


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path | None = None
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD_BYTES
    max_rows: int = DEFAULT_MAX_ROWS

    @property
    def database_path(self) -> Path:
        return self.db_path if self.db_path is not None else self.data_dir / "app.db"

    @property
    def versions_dir(self) -> Path:
        return self.data_dir / "versions"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)

    @property
    def max_upload_mb(self) -> float:
        return self.max_upload_bytes / (1024 * 1024)

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Settings":
        values = os.environ if env is None else env
        data_dir = Path(values.get("WATER_DATA_DIR", "./data")).expanduser()
        raw_db = values.get("WATER_DB_PATH", "").strip()
        db_path = Path(raw_db).expanduser() if raw_db else None
        max_upload_mb = _positive_int(values.get("WATER_MAX_UPLOAD_MB"), 10)
        max_rows = _positive_int(values.get("WATER_MAX_ROWS"), DEFAULT_MAX_ROWS)
        return cls(
            data_dir=data_dir,
            db_path=db_path,
            max_upload_bytes=max_upload_mb * 1024 * 1024,
            max_rows=max_rows,
        )


def _positive_int(raw: str | None, fallback: int) -> int:
    if raw is None or not str(raw).strip():
        return fallback
    try:
        value = int(str(raw).strip())
    except ValueError:
        return fallback
    return value if value > 0 else fallback
