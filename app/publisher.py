"""Publishing a validated snapshot as the new current version.

Invariants enforced here:

* All-or-nothing: a snapshot becomes visible only after every row is stored
  and the current-version pointer is updated in the same SQLite transaction.
* Old version survives failures: the version file is written first (atomically,
  under a brand-new name); if the database registration fails, the new file is
  removed and the transaction rolled back, leaving the previous version file
  and the current-version pointer untouched.  After a successful commit the
  file belongs to the live snapshot and is never removed by failure handling.
* Idempotent uploads: a byte-identical (or data-identical) file never creates a
  second version, even when two uploads race.
* Downloads can never mix versions: version files are immutable and named by
  version number, so a reader that opened version N reads all of N.
* The on-disk ``CURRENT`` pointer never goes backwards: concurrent publishes
  update it under a lock and only when the new version is higher, so once both
  uploads have returned it names the last committed snapshot even if their
  post-commit pointer writes finish out of order.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import NoReturn

from .csv_snapshot import (
    ParsedSnapshot,
    SnapshotError,
    observations_from_rows,
    parse_snapshot,
)
from .db import (
    Version,
    connect,
    find_version_by_source_sha,
    get_current_version,
    get_version,
    insert_event,
    next_version_no,
    observations_for_version,
    set_current_version,
    utc_now,
    write_transaction,
)
from .storage import Storage, StorageError

log = logging.getLogger(__name__)

OUTCOME_PUBLISHED = "published"
OUTCOME_DUPLICATE = "duplicate"
OUTCOME_REJECTED = "rejected"
OUTCOME_FAILED = "failed"


class PublishError(RuntimeError):
    """Raised when a snapshot could not be published.

    ``previous`` is the version that remains current after the failure.
    """

    def __init__(self, message: str, previous: Version | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.previous = previous


@dataclass(frozen=True)
class PublishResult:
    outcome: str
    message: str
    version: Version | None = None
    existing_version: Version | None = None
    event_id: int | None = None
    warnings: list[dict] = field(default_factory=list)
    change_summary: dict = field(default_factory=dict)
    error_details: dict = field(default_factory=dict)

    @property
    def published(self) -> bool:
        return self.outcome == OUTCOME_PUBLISHED

    @property
    def duplicate(self) -> bool:
        return self.outcome == OUTCOME_DUPLICATE


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class Publisher:
    """Coordinates validation, immutable file storage and database state."""

    def __init__(
        self,
        db_path: Path,
        storage: Storage,
        *,
        max_rows: int | None = None,
    ) -> None:
        self.db_path = Path(db_path)
        self.storage = Storage(storage.data_dir)
        self.max_rows = max_rows
        # Test seams: callables may raise to simulate disk or database faults.
        self.before_register = None
        self.before_file_write = None

    # -- public API ---------------------------------------------------------

    def publish(self, filename: str, raw: bytes) -> PublishResult:
        source_sha = sha256_hex(raw)
        try:
            snapshot = parse_snapshot(raw, max_rows=self.max_rows)
        except SnapshotError as exc:
            self._record_event(
                filename=filename,
                source_sha=source_sha,
                outcome=OUTCOME_REJECTED,
                message=exc.message,
                version_no=None,
                details=exc.details,
            )
            return PublishResult(
                outcome=OUTCOME_REJECTED,
                message=exc.message,
                error_details=exc.details,
            )

        content = snapshot.to_csv_bytes()
        content_sha = sha256_hex(content)
        data_sha = snapshot.data_fingerprint()

        try:
            conn = connect(self.db_path)
        except sqlite3.Error as exc:
            self._fail_to_open(filename, source_sha, exc)

        try:
            duplicate = self._find_duplicate(conn, source_sha, content_sha, data_sha)
            if duplicate is not None:
                return self._duplicate_result(
                    conn, filename, source_sha, duplicate
                )

            change_summary = self._compare_with_current(conn, snapshot)
            return self._commit_new_version(
                conn,
                filename=filename,
                source_sha=source_sha,
                content=content,
                content_sha=content_sha,
                data_sha=data_sha,
                snapshot=snapshot,
                change_summary=change_summary,
            )
        except sqlite3.Error as exc:
            previous = self._safe_current()
            message = (
                f"发布失败：数据库不可用（{exc}）。"
                + (
                    f"当前版本仍为 v{previous.version_no}，可继续下载。"
                    if previous
                    else "尚无已发布版本。"
                )
            )
            log.exception("database unavailable while publishing")
            self._record_event(
                filename=filename,
                source_sha=source_sha,
                outcome=OUTCOME_FAILED,
                message=message,
                version_no=None,
                details={"error": str(exc)},
            )
            raise PublishError(message, previous=previous) from exc
        finally:
            conn.close()

    def _fail_to_open(
        self, filename: str, source_sha: str, exc: sqlite3.Error
    ) -> NoReturn:
        """Report an unopenable database; old version files remain intact."""

        previous = self._safe_current()
        message = (
            f"发布失败：数据库不可用（{exc}）。"
            + (
                f"当前版本仍为 v{previous.version_no}，可继续下载。"
                if previous
                else "未发布新版本；已发布的版本文件不受影响。"
            )
        )
        log.exception("database could not be opened while publishing")
        self._record_event(
            filename=filename,
            source_sha=source_sha,
            outcome=OUTCOME_FAILED,
            message=message,
            version_no=None,
            details={"error": str(exc)},
        )
        raise PublishError(message, previous=previous) from exc

    def _duplicate_result(
        self,
        conn: sqlite3.Connection,
        filename: str,
        source_sha: str,
        existing: Version,
        *,
        identical_bytes: bool | None = None,
    ) -> PublishResult:
        if identical_bytes is None:
            identical_bytes = existing.source_sha256 == source_sha
        same_as = "同一文件" if identical_bytes else "内容相同但字节不同的文件"
        message = (
            f"{same_as}已发布为版本 v{existing.version_no}，"
            "未创建新版本，当前版本保持不变。"
        )
        event_id = insert_event(
            conn,
            filename=filename,
            source_sha256=source_sha,
            outcome=OUTCOME_DUPLICATE,
            message=message,
            version_no=existing.version_no,
            details_json=json.dumps(
                {"existing_version": existing.version_no}, ensure_ascii=False
            ),
        )
        return PublishResult(
            outcome=OUTCOME_DUPLICATE,
            message=message,
            existing_version=existing,
            event_id=event_id,
        )

    def _find_duplicate(
        self,
        conn: sqlite3.Connection,
        source_sha: str,
        content_sha: str,
        data_sha: str,
    ) -> Version | None:
        by_source = find_version_by_source_sha(conn, source_sha)
        if by_source is not None:
            return by_source
        for column, value in (
            ("data_sha256", data_sha),
            ("content_sha256", content_sha),
        ):
            row = conn.execute(
                f"SELECT version_no FROM versions WHERE {column} = ? "
                "ORDER BY version_no LIMIT 1",
                (value,),
            ).fetchone()
            if row is not None:
                return get_version(conn, int(row["version_no"]))
        return None

    def _compare_with_current(
        self, conn: sqlite3.Connection, snapshot: ParsedSnapshot
    ) -> dict:
        """Summarize how the snapshot differs from the current dataset.

        Differences at the same station/time are *revisions*, not failures: a
        complete snapshot legitimately supersedes earlier values.  They are
        surfaced to the administrator for review.
        """

        current = get_current_version(conn)
        if current is None:
            return {
                "previous_version": None,
                "added": snapshot.row_count,
                "changed": 0,
                "removed": 0,
            }
        previous = {
            (row["station"], row["sampled_at"]): row["result_mg_l"]
            for row in observations_for_version(conn, current.version_no)
        }
        new = {
            (obs.station, obs.sampled_at): obs.result_mg_l
            for obs in snapshot.observations
        }
        added = len(new.keys() - previous.keys())
        removed = len(previous.keys() - new.keys())
        changed = sum(
            1
            for key in new.keys() & previous.keys()
            if Decimal(new[key]) != Decimal(previous[key])
        )
        return {
            "previous_version": current.version_no,
            "added": added,
            "changed": changed,
            "removed": removed,
        }

    def _commit_new_version(
        self,
        conn: sqlite3.Connection,
        *,
        filename: str,
        source_sha: str,
        content: bytes,
        content_sha: str,
        data_sha: str,
        snapshot: ParsedSnapshot,
        change_summary: dict,
    ) -> PublishResult:
        version_no: int | None = None
        file_written = False
        committed = False
        event_id: int | None = None

        try:
            with write_transaction(conn) as tx:
                # A concurrent upload of the same file may have won the lock
                # while we were validating; re-check under the write lock.
                duplicate = self._find_duplicate(
                    tx, source_sha, content_sha, data_sha
                )
                if duplicate is not None:
                    return self._duplicate_result_committed(
                        tx, filename, source_sha, duplicate
                    )

                version_no = next_version_no(tx)

                if self.before_file_write is not None:
                    self.before_file_write(version_no)
                self.storage.write_version(version_no, content)
                file_written = True

                if self.before_register is not None:
                    self.before_register(version_no)

                created_at = utc_now()
                tx.execute(
                    "INSERT INTO versions"
                    "(version_no, filename, stored_name, source_sha256, content_sha256,"
                    " data_sha256, row_count, byte_size, created_at) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        version_no,
                        filename,
                        self.storage.version_filename(version_no),
                        source_sha,
                        content_sha,
                        data_sha,
                        len(snapshot.observations),
                        len(content),
                        created_at,
                    ),
                )
                tx.executemany(
                    "INSERT INTO observations"
                    "(version_no, station, sampled_at, result_mg_l, source_unit,"
                    " source_value, source_line) VALUES(?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            version_no,
                            obs.station,
                            obs.sampled_at,
                            obs.result_mg_l,
                            obs.source_unit,
                            obs.source_value,
                            obs.source_line,
                        )
                        for obs in snapshot.observations
                    ],
                )
                set_current_version(tx, version_no)
                event_id = insert_event(
                    tx,
                    filename=filename,
                    source_sha256=source_sha,
                    outcome=OUTCOME_PUBLISHED,
                    message=f"已发布版本 v{version_no}（{snapshot.row_count} 行）",
                    version_no=version_no,
                    details_json=json.dumps(
                        {
                            "row_count": snapshot.row_count,
                            "warnings": len(snapshot.warnings),
                            "change_summary": change_summary,
                            "merged_duplicates": len(snapshot.warnings),
                        },
                        ensure_ascii=False,
                    ),
                )

            # From here on the version is durable: the immutable file exists and
            # its database row and current pointer committed atomically.  Nothing
            # below may roll it back or delete the file.
            committed = True

            # Record the committed pointer on disk too.  This happens after the
            # commit, so it can only lag, never lead, the authoritative state;
            # it is monotonic under a lock, so the newest commit wins even when
            # two publishes finish out of order.  The pointer is advisory, and
            # the version is already durable: no failure here may be reported
            # as a failed publish or remove the committed file.
            try:
                self.storage.write_current_pointer(version_no)
            except Exception:
                log.exception("could not update on-disk current-version pointer")

            # Build the response from the values captured inside the transaction.
            # A database that becomes unreadable after the commit must not turn
            # a successful publish into a reported failure.
            version = Version(
                version_no=version_no,
                filename=filename,
                stored_name=self.storage.version_filename(version_no),
                source_sha256=source_sha,
                content_sha256=content_sha,
                data_sha256=data_sha,
                row_count=len(snapshot.observations),
                byte_size=len(content),
                created_at=created_at,
            )
            return PublishResult(
                outcome=OUTCOME_PUBLISHED,
                message=(
                    f"已发布版本 v{version.version_no}，共 {version.row_count} 行，"
                    "已设为当前数据集。"
                ),
                version=version,
                event_id=event_id,
                warnings=snapshot.warnings,
                change_summary=change_summary,
            )
        except (StorageError, sqlite3.Error, OSError, PublishError) as exc:
            # Something failed before the commit: ``write_transaction`` has
            # rolled back, so the current pointer still names the previous
            # version.  Delete the freshly written file only in that case -- it
            # belongs to no committed version, and leaving it would let a later
            # version number collide with stale bytes.  After a successful commit
            # the file is the live snapshot and must never be removed here.
            if not committed and file_written and version_no is not None:
                self.storage.remove_version(version_no)
            previous = self._safe_current()
            message = (
                "发布失败："
                + (exc.message if isinstance(exc, PublishError) else str(exc))
                + (
                    f"。当前版本仍为 v{previous.version_no}，可继续下载。"
                    if previous
                    else "。尚无已发布版本。"
                )
            )
            log.exception("publishing snapshot failed")
            self._record_event(
                filename=filename,
                source_sha=source_sha,
                outcome=OUTCOME_FAILED,
                message=message,
                version_no=None,
                details={"error": str(exc)},
            )
            raise PublishError(message, previous=previous) from exc

    def _duplicate_result_committed(
        self,
        tx: sqlite3.Connection,
        filename: str,
        source_sha: str,
        existing: Version,
    ) -> PublishResult:
        same_as = "同一文件" if existing.source_sha256 == source_sha else "内容相同的文件"
        message = (
            f"{same_as}已发布为版本 v{existing.version_no}，"
            "未创建新版本，当前版本保持不变。"
        )
        event_id = insert_event(
            tx,
            filename=filename,
            source_sha256=source_sha,
            outcome=OUTCOME_DUPLICATE,
            message=message,
            version_no=existing.version_no,
            details_json=json.dumps(
                {"existing_version": existing.version_no}, ensure_ascii=False
            ),
        )
        return PublishResult(
            outcome=OUTCOME_DUPLICATE,
            message=message,
            existing_version=existing,
            event_id=event_id,
        )

    def _safe_current(self) -> Version | None:
        try:
            conn = connect(self.db_path)
            try:
                return get_current_version(conn)
            finally:
                conn.close()
        except sqlite3.Error:
            return None

    def _record_event(
        self,
        *,
        filename: str,
        source_sha: str,
        outcome: str,
        message: str,
        version_no: int | None,
        details: dict,
    ) -> None:
        """Best-effort audit log; never mask the original failure."""

        try:
            conn = connect(self.db_path)
        except sqlite3.Error:
            return
        try:
            insert_event(
                conn,
                filename=filename,
                source_sha256=source_sha,
                outcome=outcome,
                message=message,
                version_no=version_no,
                details_json=json.dumps(details, ensure_ascii=False),
            )
        except sqlite3.Error:
            log.exception("could not record upload event")
        finally:
            conn.close()


def load_current_snapshot(
    conn: sqlite3.Connection,
) -> tuple[Version | None, list]:
    """Read the pointer and its rows inside one read transaction.

    SQLite in WAL mode gives this read a stable snapshot even if a publish
    commits concurrently, so the returned version and rows always belong
    together.
    """

    conn.execute("BEGIN")
    try:
        version = get_current_version(conn)
        rows = (
            observations_for_version(conn, version.version_no)
            if version is not None
            else []
        )
        conn.execute("COMMIT")
        return version, observations_from_rows(rows)
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
