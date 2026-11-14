"""SQLite schema and small data-access helpers.

Connections are opened in autocommit mode (``isolation_level=None``) so that
write transactions can be started explicitly with ``BEGIN IMMEDIATE``.  That
serializes publishers across processes and guarantees the conflict check, the
version-number allocation and the pointer update see one consistent state.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS versions (
    version_no      INTEGER PRIMARY KEY,
    filename        TEXT NOT NULL,
    stored_name     TEXT NOT NULL UNIQUE,
    source_sha256   TEXT NOT NULL UNIQUE,
    content_sha256  TEXT NOT NULL,
    data_sha256     TEXT NOT NULL,
    row_count       INTEGER NOT NULL,
    byte_size       INTEGER NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS observations (
    version_no      INTEGER NOT NULL REFERENCES versions(version_no),
    station         TEXT NOT NULL,
    sampled_at      TEXT NOT NULL,
    result_mg_l     TEXT NOT NULL,
    source_unit     TEXT NOT NULL,
    source_value    TEXT NOT NULL,
    source_line     INTEGER NOT NULL,
    PRIMARY KEY (version_no, station, sampled_at)
);

CREATE INDEX IF NOT EXISTS idx_observations_station_time
    ON observations(station, sampled_at);

CREATE TABLE IF NOT EXISTS upload_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL,
    filename        TEXT NOT NULL,
    source_sha256   TEXT,
    outcome         TEXT NOT NULL,
    message         TEXT NOT NULL,
    version_no      INTEGER,
    details_json    TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS meta (
    key             TEXT PRIMARY KEY,
    value           TEXT NOT NULL
);
"""

CURRENT_VERSION_KEY = "current_version"

log = logging.getLogger(__name__)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(db_path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    """Open a connection.

    ``readonly=True`` is for read-only deployments and UI read paths: if the
    database volume cannot be written, reads fall back to SQLite's read-only
    URI (and, as a last resort, an immutable snapshot).  Writes always use the
    default mode and fail loudly when the database is not writable.
    """

    try:
        conn = _open_rw(db_path)
    except sqlite3.OperationalError:
        if not readonly:
            raise
        log.warning("database is not writable; reading %s read-only", db_path)
        return _open_readonly(db_path)
    return conn


def _open_rw(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path),
        timeout=30.0,
        isolation_level=None,
        check_same_thread=True,
    )
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
    except sqlite3.OperationalError:
        conn.close()
        raise
    return conn


def _open_readonly(db_path: Path) -> sqlite3.Connection:
    """Fallback for a read-only filesystem.

    ``mode=ro`` still needs to write the ``-shm``/``-wal`` sidecars, so the
    connection only succeeds when the directory is writable.  We therefore
    probe with a real query and fall back to ``immutable=1``, which reads the
    main database file without touching sidecars.  Reads are only used for
    display and for locating the current version file; every published version
    is immutable, so a slightly stale pointer can never yield a mixed file.
    """

    last_error: sqlite3.OperationalError | None = None
    for uri in (
        f"file:{db_path}?mode=ro",
        f"file:{db_path}?mode=ro&immutable=1",
    ):
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(uri, uri=True, timeout=30.0, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
            return conn
        except sqlite3.OperationalError as exc:
            if conn is not None:
                conn.close()
            last_error = exc
    assert last_error is not None
    raise last_error


def init_db(db_path: Path) -> None:
    """Create the schema and apply forward-only migrations.

    If the database already exists but its volume is currently read-only, keep
    serving: schema creation is only needed for a brand-new database, so an
    operational error here is tolerated when the file is present.  A missing
    database on an unwritable volume is a real configuration fault and raises.
    """

    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        if not db_path.parent.is_dir():
            raise
    try:
        conn = connect(db_path)
        try:
            conn.executescript(SCHEMA)
            _migrate(conn)
        finally:
            conn.close()
    except sqlite3.Error:
        if db_path.is_file():
            log.warning("database is read-only; skipping schema creation")
            return
        raise


def _migrate(conn: sqlite3.Connection) -> None:
    """Bring an older database up to the current schema.

    ``CREATE TABLE IF NOT EXISTS`` does not add columns to an existing table,
    so databases created by an earlier build need explicit additions.  Every
    step is idempotent and only adds nullable/indexed structures.
    """

    with write_transaction(conn) as tx:
        columns = {
            row["name"]
            for row in tx.execute("PRAGMA table_info(versions)").fetchall()
        }
        if "data_sha256" not in columns:
            # Old rows predate the data fingerprint.  Adding a column cannot be
            # NOT NULL without a default, so it is added nullable and backfilled
            # from the canonical file digest, which is a safe duplicate key.
            tx.execute("ALTER TABLE versions ADD COLUMN data_sha256 TEXT")
            tx.execute(
                "UPDATE versions SET data_sha256 = content_sha256 "
                "WHERE data_sha256 IS NULL"
            )


@contextmanager
def write_transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run a block inside ``BEGIN IMMEDIATE`` and commit or roll back."""

    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        raise


# ---------------------------------------------------------------------------
# Versions


@dataclass(frozen=True)
class Version:
    version_no: int
    filename: str
    stored_name: str
    source_sha256: str
    content_sha256: str
    data_sha256: str
    row_count: int
    byte_size: int
    created_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Version":
        return cls(
            version_no=row["version_no"],
            filename=row["filename"],
            stored_name=row["stored_name"],
            source_sha256=row["source_sha256"],
            content_sha256=row["content_sha256"],
            data_sha256=row["data_sha256"],
            row_count=row["row_count"],
            byte_size=row["byte_size"],
            created_at=row["created_at"],
        )


_VERSION_COLUMNS = (
    "version_no, filename, stored_name, source_sha256, content_sha256, "
    "data_sha256, row_count, byte_size, created_at"
)


def get_version(conn: sqlite3.Connection, version_no: int) -> Version | None:
    row = conn.execute(
        f"SELECT {_VERSION_COLUMNS} FROM versions WHERE version_no = ?",
        (version_no,),
    ).fetchone()
    return Version.from_row(row) if row else None


def find_version_by_source_sha(
    conn: sqlite3.Connection, source_sha256: str
) -> Version | None:
    row = conn.execute(
        f"SELECT {_VERSION_COLUMNS} FROM versions WHERE source_sha256 = ?",
        (source_sha256,),
    ).fetchone()
    return Version.from_row(row) if row else None


def list_versions(conn: sqlite3.Connection, limit: int | None = None) -> list[Version]:
    sql = f"SELECT {_VERSION_COLUMNS} FROM versions ORDER BY version_no DESC"
    params: tuple[Any, ...] = ()
    if limit is not None:
        sql += " LIMIT ?"
        params = (limit,)
    return [Version.from_row(row) for row in conn.execute(sql, params)]


def get_current_version(conn: sqlite3.Connection) -> Version | None:
    row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (CURRENT_VERSION_KEY,)
    ).fetchone()
    if row is None:
        return None
    try:
        version_no = int(row["value"])
    except (TypeError, ValueError):
        return None
    return get_version(conn, version_no)


def set_current_version(conn: sqlite3.Connection, version_no: int) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (CURRENT_VERSION_KEY, str(version_no)),
    )


def next_version_no(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(version_no), 0) + 1 AS n FROM versions").fetchone()
    return int(row["n"])


def observations_for_version(
    conn: sqlite3.Connection, version_no: int, limit: int | None = None
) -> list[sqlite3.Row]:
    sql = (
        "SELECT station, sampled_at, result_mg_l, source_unit, source_value, source_line "
        "FROM observations WHERE version_no = ? ORDER BY station, sampled_at, source_line"
    )
    params: list[Any] = [version_no]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return list(conn.execute(sql, tuple(params)))


# ---------------------------------------------------------------------------
# Upload events


@dataclass(frozen=True)
class UploadEvent:
    id: int
    created_at: str
    filename: str
    source_sha256: str | None
    outcome: str
    message: str
    version_no: int | None
    details_json: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "UploadEvent":
        return cls(
            id=row["id"],
            created_at=row["created_at"],
            filename=row["filename"],
            source_sha256=row["source_sha256"],
            outcome=row["outcome"],
            message=row["message"],
            version_no=row["version_no"],
            details_json=row["details_json"],
        )


def insert_event(
    conn: sqlite3.Connection,
    *,
    filename: str,
    source_sha256: str | None,
    outcome: str,
    message: str,
    version_no: int | None,
    details_json: str,
) -> int:
    cursor = conn.execute(
        "INSERT INTO upload_events"
        "(created_at, filename, source_sha256, outcome, message, version_no, details_json) "
        "VALUES(?, ?, ?, ?, ?, ?, ?)",
        (
            utc_now(),
            filename,
            source_sha256,
            outcome,
            message,
            version_no,
            details_json,
        ),
    )
    return int(cursor.lastrowid)


def get_event(conn: sqlite3.Connection, event_id: int) -> UploadEvent | None:
    row = conn.execute(
        "SELECT id, created_at, filename, source_sha256, outcome, message, "
        "version_no, details_json FROM upload_events WHERE id = ?",
        (event_id,),
    ).fetchone()
    return UploadEvent.from_row(row) if row else None


def list_events(conn: sqlite3.Connection, limit: int = 10) -> list[UploadEvent]:
    rows = conn.execute(
        "SELECT id, created_at, filename, source_sha256, outcome, message, "
        "version_no, details_json FROM upload_events ORDER BY id DESC LIMIT ?",
        (limit,),
    )
    return [UploadEvent.from_row(row) for row in rows]
