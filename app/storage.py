"""Immutable version-file storage with atomic, crash-safe writes.

Each published version is a complete CSV file named after its version number.
Files are never modified after publication: a new upload always creates a new
file.  A download therefore either reads the whole old file or the whole new
file, never a mixture.
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path


class StorageError(RuntimeError):
    """Raised when a snapshot file cannot be written."""


class Storage:
    POINTER_NAME = "CURRENT"

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self.versions_dir = self.data_dir / "versions"
        self.staging_dir = self.data_dir / "staging"

    def prepare(self) -> None:
        for directory in (self.versions_dir, self.staging_dir):
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                if not directory.is_dir():
                    raise StorageError(f"无法创建数据目录：{exc}") from exc
                # Read-only volume: the directory already exists, which is
                # enough to serve downloads from it.

    # -- current-version pointer -------------------------------------------

    def pointer_path(self) -> Path:
        return self.data_dir / self.POINTER_NAME

    def read_current_pointer(self) -> int | None:
        try:
            text = self.pointer_path().read_text(encoding="ascii").strip()
        except OSError:
            return None
        try:
            return int(text)
        except ValueError:
            return None

    def write_current_pointer(self, version_no: int) -> None:
        """Atomically record the latest committed version on disk.

        The database is authoritative; this pointer only exists so a download
        can still identify a known-committed version when the database is
        temporarily unreadable.  It is written *after* the commit and only
        increases, so it can lag behind the database but can never point at a
        version whose registration rolled back.
        """

        existing = self.read_current_pointer()
        if existing is not None and existing >= version_no:
            return
        self.prepare()
        fd, tmp_name = tempfile.mkstemp(
            prefix=".current.", suffix=".tmp", dir=str(self.staging_dir)
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="ascii") as handle:
                handle.write(f"{version_no}\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self.pointer_path())
            _fsync_dir(self.data_dir)
        except OSError as exc:
            _unlink_quietly(tmp_path)
            raise StorageError(f"写入当前版本指针失败：{exc}") from exc

    def version_filename(self, version_no: int) -> str:
        return f"v{version_no:04d}.csv"

    def version_path(self, version_no: int) -> Path:
        return self.versions_dir / self.version_filename(version_no)

    def write_version(self, version_no: int, content: bytes) -> Path:
        """Atomically write one immutable version file and fsync it.

        The data is staged in ``staging/`` on the same filesystem, flushed to
        disk and only then renamed into place.  A reader can never observe a
        partially written file.
        """

        self.prepare()
        final_path = self.version_path(version_no)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".v{version_no:04d}.", suffix=".tmp", dir=str(self.staging_dir)
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, final_path)
            _fsync_dir(self.versions_dir)
        except BaseException as exc:
            _unlink_quietly(tmp_path)
            # The final name is unique to this version number and was only
            # placed there by this call, so removing it cannot affect an
            # already-published older version.
            _unlink_quietly(final_path)
            raise StorageError(f"写入版本文件失败：{exc}") from exc
        return final_path

    def remove_version(self, version_no: int) -> None:
        """Best-effort cleanup for a version whose DB registration failed."""

        _unlink_quietly(self.version_path(version_no))

    def reconcile(
        self, registered: set[int], *, min_age_seconds: int = 0
    ) -> list[Path]:
        """Quarantine ``vNNNN.csv`` files with no database row.

        Such a file can only be left behind by a crash between the atomic write
        and the commit.  It is moved to ``orphans/`` rather than deleted, so a
        damaged database never destroys the actual laboratory data; moving it
        out of ``versions/`` keeps later version numbers from colliding with
        stale bytes.

        ``min_age_seconds`` protects a publish that is in flight in another
        process: its file exists for a moment before the database row commits.
        Startup uses a generous grace period to avoid stealing that file.
        """

        self.prepare()
        moved: list[Path] = []
        orphan_dir = self.data_dir / "orphans"
        now = time.time()
        for path in sorted(self.versions_dir.glob("v*.csv")):
            try:
                version_no = int(path.stem[1:])
            except ValueError:
                continue
            if version_no in registered:
                continue
            try:
                if min_age_seconds and now - path.stat().st_mtime < min_age_seconds:
                    continue
                orphan_dir.mkdir(parents=True, exist_ok=True)
                target = orphan_dir / path.name
                if target.exists():
                    target = orphan_dir / f"{path.name}.{os.getpid()}"
                os.replace(path, target)
                moved.append(target)
            except OSError:
                continue
        for tmp in self.staging_dir.glob("*.tmp"):
            _unlink_quietly(tmp)
        return moved

    def exists(self, version_no: int) -> bool:
        return self.version_path(version_no).is_file()


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _unlink_quietly(path: Path) -> bool:
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return False
