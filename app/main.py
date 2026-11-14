"""FastAPI application for publishing ammonia-nitrogen snapshots."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import (
    HTMLResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import Settings
from .csv_snapshot import observations_from_rows, render_snapshot_csv
from .db import (
    connect,
    get_current_version,
    get_event,
    get_version,
    init_db,
    list_events,
    list_versions,
    observations_for_version,
)
from .publisher import OUTCOME_DUPLICATE, PublishError, Publisher, load_current_snapshot
from .storage import Storage, StorageError

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

OUTCOME_LABELS = {
    "published": "已发布",
    "duplicate": "重复上传（未新建版本）",
    "rejected": "校验未通过",
    "failed": "发布失败",
}

MAX_EVENT_BYTES = 256 * 1024


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or Settings.from_env()
    resolved.ensure_dirs()
    init_db(resolved.database_path)

    storage = Storage(resolved.data_dir)
    storage.prepare()
    _reconcile_orphans(resolved.database_path, storage)
    _bootstrap_pointer(resolved.database_path, storage)
    publisher = Publisher(resolved.database_path, storage, max_rows=resolved.max_rows)

    app = FastAPI(
        title="河道氨氮快照发布平台",
        description="将实验室 CSV 校验、换算为 mg/L 并发布为当前数据集。",
    )
    app.state.settings = resolved
    app.state.publisher = publisher
    app.state.storage = storage

    static_dir = BASE_DIR / "static"
    if static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    def connect_read():
        """Read-only connection for UI and downloads.

        Publishing always connects read-write; page reads should still work
        when the data volume is mounted read-only.
        """

        return connect(resolved.database_path, readonly=True)

    def home_context(conn: sqlite3.Connection, **extra) -> dict:
        context = {
            "current": get_current_version(conn),
            "versions": list_versions(conn, limit=10),
            "events": list_events(conn, limit=8),
            "outcome_labels": OUTCOME_LABELS,
            "max_upload_mb": resolved.max_upload_mb,
        }
        context.update(extra)
        return context

    def safe_home_context(**extra) -> dict:
        """Build page context, falling back to an empty page if the DB is down."""

        try:
            conn = connect_read()
            try:
                return home_context(conn, **extra)
            finally:
                conn.close()
        except sqlite3.Error:
            log.exception("could not read database for page context")
            return {
                "current": None,
                "versions": [],
                "events": [],
                "outcome_labels": OUTCOME_LABELS,
                "max_upload_mb": resolved.max_upload_mb,
                "storage_unavailable": True,
                **extra,
            }

    @app.exception_handler(sqlite3.Error)
    async def sqlite_error_handler(request: Request, exc: sqlite3.Error):
        log.exception("database error while handling %s", request.url.path)
        return templates.TemplateResponse(
            request,
            "unavailable.html",
            {"message": "数据库暂时无法访问，请稍后重试。"},
            status_code=503,
        )

    # -- pages -------------------------------------------------------------

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request, event: int | None = None):
        event_record = None
        event_details: dict = {}
        context = {"current": None, "versions": [], "events": []}
        try:
            conn = connect_read()
            try:
                context = home_context(conn)
                event_record = get_event(conn, event) if event is not None else None
            finally:
                conn.close()
            if (
                event_record is not None
                and len(event_record.details_json) <= MAX_EVENT_BYTES
            ):
                try:
                    event_details = json.loads(event_record.details_json)
                except json.JSONDecodeError:
                    event_details = {}
        except sqlite3.Error:
            log.exception("could not load home page")
            context = safe_home_context()
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                **context,
                "event": event_record,
                "event_details": event_details,
            },
            status_code=200,
        )

    @app.get("/versions", response_class=HTMLResponse)
    def versions_page(request: Request):
        current = None
        versions: list = []
        events: list = []
        try:
            conn = connect_read()
            try:
                current = get_current_version(conn)
                versions = list_versions(conn, limit=200)
                events = list_events(conn, limit=50)
            finally:
                conn.close()
        except sqlite3.Error:
            log.exception("could not load version history")
            fallback = safe_home_context()
            current = fallback["current"]
        return templates.TemplateResponse(
            request,
            "versions.html",
            {
                "current": current,
                "versions": versions,
                "events": events,
                "outcome_labels": OUTCOME_LABELS,
            },
            status_code=200,
        )

    @app.get("/versions/{version_no}", response_class=HTMLResponse)
    def version_detail(request: Request, version_no: int, page: int = 1):
        page_size = 100
        conn = connect_read()
        try:
            current = get_current_version(conn)
            version = get_version(conn, version_no)
            if version is None:
                return templates.TemplateResponse(
                    request,
                    "not_found.html",
                    {"message": f"版本 v{version_no} 不存在。", "current": current},
                    status_code=404,
                )
            total = conn.execute(
                "SELECT COUNT(*) AS n FROM observations WHERE version_no = ?",
                (version_no,),
            ).fetchone()["n"]
            page = max(1, page)
            offset = (page - 1) * page_size
            rows = conn.execute(
                "SELECT station, sampled_at, result_mg_l, source_unit, source_value "
                "FROM observations WHERE version_no = ? "
                "ORDER BY station, sampled_at, source_line LIMIT ? OFFSET ?",
                (version_no, page_size, offset),
            ).fetchall()
        finally:
            conn.close()
        pages = max(1, (total + page_size - 1) // page_size)
        return templates.TemplateResponse(
            request,
            "version_detail.html",
            {
                "current": current,
                "version": version,
                "rows": rows,
                "total": total,
                "page": page,
                "pages": pages,
            },
            status_code=200,
        )

    # -- upload ------------------------------------------------------------

    @app.post("/upload", response_class=HTMLResponse)
    async def upload(request: Request, file: UploadFile = File(...)):
        filename = (file.filename or "upload.csv").strip() or "upload.csv"
        raw = await _read_limited(file, resolved.max_upload_bytes)

        if isinstance(raw, str):
            context = safe_home_context()
            return templates.TemplateResponse(
                request,
                "index.html",
                {
                    **context,
                    "upload_error": raw,
                    "uploaded_filename": filename,
                },
                status_code=413,
            )

        try:
            result = publisher.publish(filename, raw)
        except PublishError as exc:
            context = safe_home_context()
            return templates.TemplateResponse(
                request,
                "index.html",
                {
                    **context,
                    "upload_error": exc.message,
                    "uploaded_filename": filename,
                    "previous": exc.previous,
                },
                status_code=500,
            )
        except Exception:  # unexpected: never leave the browser with a raw 500
            log.exception("unexpected error while publishing")
            context = safe_home_context()
            current = context.get("current")
            return templates.TemplateResponse(
                request,
                "index.html",
                {
                    **context,
                    "upload_error": "服务器处理上传时发生内部错误，本次未发布，当前版本保持不变。",
                    "uploaded_filename": filename,
                    "previous": current,
                },
                status_code=500,
            )

        if result.outcome in ("published", OUTCOME_DUPLICATE) and result.event_id:
            return RedirectResponse(url=f"/?event={result.event_id}", status_code=303)

        # Rejections are rendered in place so the administrator sees the row
        # level reasons immediately.
        context = safe_home_context()
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                **context,
                "upload_error": result.message,
                "uploaded_filename": filename,
                "upload_result": result,
                "event_details": result.error_details,
            },
            status_code=422,
        )

    # -- download ----------------------------------------------------------

    @app.get("/download/current")
    def download_current():
        """Download the current version.

        The version number is read and its rows are loaded inside a single
        read transaction, and the immutable file name is derived from that
        version number.  A concurrent publish cannot produce a mixed file.
        """

        try:
            conn = connect_read()
            try:
                current, observations = load_current_snapshot(conn)
            finally:
                conn.close()
        except sqlite3.Error:
            # The database is not readable right now.  The newest immutable
            # version file is still a complete, self-consistent snapshot, so
            # serve it rather than leaving the administrator with nothing.
            log.exception("database unavailable during download")
            fallback = _fallback_version_file(storage)
            if fallback is None:
                return PlainTextResponse(
                    "当前版本暂时不可用，数据库与版本文件均无法读取。",
                    status_code=503,
                    media_type="text/plain; charset=utf-8",
                )
            version_no, path = fallback
            return _file_response(path, version_no, "snapshot.csv", path.stat().st_size)

        if current is None:
            return PlainTextResponse(
                "尚未发布任何版本。",
                status_code=404,
                media_type="text/plain; charset=utf-8",
            )

        path = storage.version_path(current.version_no)
        try:
            size = path.stat().st_size
        except OSError:
            size = -1
        if size == current.byte_size:
            return _file_response(
                path, current.version_no, current.filename, current.byte_size
            )
        # The immutable file is missing or damaged: rebuild the same bytes from
        # the rows of exactly this version, which were read atomically above.
        content = render_snapshot_csv(observations)
        return _csv_response(content, current.version_no, current.filename)

    @app.get("/download/{version_no}")
    def download_version(version_no: int):
        try:
            conn = connect_read()
            try:
                version = get_version(conn, version_no)
                if version is None:
                    return PlainTextResponse(
                        f"版本 v{version_no} 不存在。",
                        status_code=404,
                        media_type="text/plain; charset=utf-8",
                    )
                rows = observations_for_version(conn, version_no)
            finally:
                conn.close()
        except sqlite3.Error:
            # Only serve versions the pointer proves were committed; never an
            # unregistered file left behind by a failed publish.
            log.exception("database unavailable during version download")
            if version_no <= (storage.read_current_pointer() or 0):
                path = storage.version_path(version_no)
                if path.is_file():
                    return _file_response(
                        path, version_no, "snapshot.csv", path.stat().st_size
                    )
            return PlainTextResponse(
                f"版本 v{version_no} 暂时不可用。",
                status_code=503,
                media_type="text/plain; charset=utf-8",
            )
        content = render_snapshot_csv(observations_from_rows(rows))
        return _csv_response(content, version.version_no, version.filename)

    # -- api ---------------------------------------------------------------

    @app.get("/api/current")
    def api_current():
        conn = connect_read()
        try:
            current = get_current_version(conn)
        finally:
            conn.close()
        if current is None:
            return {"version": None, "row_count": 0, "created_at": None}
        return {
            "version": current.version_no,
            "filename": current.filename,
            "row_count": current.row_count,
            "created_at": current.created_at,
            "download_url": "/download/current",
        }

    @app.get("/healthz")
    def healthz():
        try:
            conn = connect_read()
            try:
                conn.execute("SELECT 1").fetchone()
            finally:
                conn.close()
        except sqlite3.Error:
            return PlainTextResponse("database unavailable", status_code=503)
        return {"status": "ok"}

    return app


def _fallback_version_file(storage: Storage) -> tuple[int, Path] | None:
    """Return the last *committed* version file when the database is unreadable.

    The on-disk pointer is written only after a successful commit, so it never
    names a version whose registration rolled back or a file left by a crashed
    publish.  A missing pointer means there is no safe fallback.
    """

    version_no = storage.read_current_pointer()
    if version_no is None:
        return None
    path = storage.version_path(version_no)
    if not path.is_file():
        return None
    return version_no, path


def _bootstrap_pointer(db_path: Path, storage: Storage) -> None:
    """Seed the on-disk pointer for databases created before it existed."""

    try:
        conn = connect(db_path, readonly=True)
        try:
            current = get_current_version(conn)
        finally:
            conn.close()
    except sqlite3.Error:
        return
    if current is None:
        return
    try:
        storage.write_current_pointer(current.version_no)
    except StorageError:
        log.warning("could not bootstrap current-version pointer")


def _reconcile_orphans(db_path: Path, storage: Storage) -> None:
    """Move version files left by a crash between write and commit aside.

    Startup must never fail because of cleanup: a read-only volume simply skips
    it, and unregistered files are never served by the download endpoints.
    """

    try:
        conn = connect(db_path, readonly=True)
        try:
            registered = {
                int(row["version_no"])
                for row in conn.execute("SELECT version_no FROM versions")
            }
        finally:
            conn.close()
    except sqlite3.Error:
        log.warning("could not reconcile version files; database unreadable")
        return
    try:
        moved = storage.reconcile(registered, min_age_seconds=60)
    except StorageError:
        log.exception("could not reconcile version files")
        return
    if moved:
        log.warning("moved %d unregistered version file(s) to orphans/", len(moved))


def _download_headers(version_no: int, source_filename: str) -> dict[str, str]:
    base = Path(source_filename).stem or "snapshot"
    # Keep the download name filesystem-friendly and header-safe.
    base = "".join(
        ch if ch.isalnum() or ch in "-_." else "_" for ch in base
    ).strip("._") or "snapshot"
    filename = f"ammonia_n_{base}_v{version_no:04d}.csv"
    return {
        "Content-Disposition": (
            f'attachment; filename="{_ascii_fallback(filename)}"; '
            f"filename*=UTF-8''{quote(filename, safe='')}"
        ),
        "X-Snapshot-Version": str(version_no),
        "Cache-Control": "no-store",
    }


def _csv_response(content: bytes, version_no: int, source_filename: str) -> Response:
    return Response(
        content=content,
        media_type="text/csv; charset=utf-8",
        headers=_download_headers(version_no, source_filename),
    )


def _file_response(
    path: Path, version_no: int, source_filename: str, expected_size: int
) -> Response:
    headers = _download_headers(version_no, source_filename)
    headers["Content-Length"] = str(expected_size)

    def iterator():
        with path.open("rb") as handle:
            while chunk := handle.read(64 * 1024):
                yield chunk

    return StreamingResponse(
        iterator(), media_type="text/csv; charset=utf-8", headers=headers
    )


def _ascii_fallback(name: str) -> str:
    cleaned = name.encode("ascii", "ignore").decode("ascii")
    cleaned = cleaned.replace('"', "").replace("\\", "").strip()
    return cleaned or "snapshot.csv"


async def _read_limited(file: UploadFile, limit: int) -> bytes | str:
    """Read the upload, enforcing a hard size cap.

    Returns the bytes, or an error string when the file is too large.
    """

    chunks: list[bytes] = []
    total = 0
    try:
        while True:
            chunk = await file.read(64 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                return f"文件过大，上限 {limit / (1024 * 1024):g} MB。"
            chunks.append(chunk)
    finally:
        await file.close()
    return b"".join(chunks)


app = create_app()
