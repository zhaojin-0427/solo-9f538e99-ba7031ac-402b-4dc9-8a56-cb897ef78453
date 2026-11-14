import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture
def client(tmp_path):
    settings = Settings(data_dir=tmp_path / "data")
    app = create_app(settings)
    with TestClient(app) as test_client:
        test_client.settings = settings
        yield test_client


def upload(client, filename, content):
    return client.post(
        "/upload",
        files={"file": (filename, content, "text/csv")},
        follow_redirects=False,
    )


GOOD = (
    "站点,采样时间,结果,单位\n"
    "闸口站,2026-11-02 08:00,0.42,mg/L\n"
    "桥头站,2026-11-02 08:00,480,μg/L\n"
).encode("utf-8")


def test_index_renders_without_versions(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "尚未发布任何版本" in response.text


def test_upload_publishes_and_redirects_to_result(client):
    response = upload(client, "lab.csv", GOOD)
    assert response.status_code == 303
    location = response.headers["location"]
    assert location.startswith("/?event=")

    page = client.get(location)
    assert page.status_code == 200
    assert "已发布版本 v1" in page.text
    assert "0.48" in client.get("/download/current").text


def test_download_current_has_mg_l_values_and_version_header(client):
    upload(client, "lab.csv", GOOD)
    response = client.get("/download/current")
    assert response.status_code == 200
    assert response.headers["X-Snapshot-Version"] == "1"
    assert "attachment" in response.headers["Content-Disposition"]
    assert "Cache-Control" in response.headers
    body = response.content.decode("utf-8")
    lines = body.strip().splitlines()
    assert lines[0] == "站点,采样时间,结果(mg/L),来源单位,原始结果"
    assert any("闸口站" in line and ",0.42,mg/L," in line for line in lines)
    assert any("桥头站" in line and ",0.48,ug/L," in line for line in lines)


def test_download_before_any_publish_is_404(client):
    assert client.get("/download/current").status_code == 404


def test_duplicate_upload_reports_duplicate_without_new_version(client):
    first = upload(client, "lab.csv", GOOD)
    assert first.status_code == 303
    second = upload(client, "lab.csv", GOOD)
    assert second.status_code == 303

    page = client.get(second.headers["location"])
    assert "重复上传" in page.text
    assert "未创建新版本" in page.text

    api = client.get("/api/current").json()
    assert api["version"] == 1


def test_conflict_upload_shows_conflict_and_keeps_old_download(client):
    upload(client, "lab.csv", GOOD)
    conflict = (
        "站点,采样时间,结果,单位\n"
        "闸口站,2026-11-02 08:00,0.42,mg/L\n"
        "闸口站,2026-11-02 08:00,1.2,mg/L\n"
    ).encode("utf-8")
    response = upload(client, "conflict.csv", conflict)
    assert response.status_code == 422
    assert "校验未通过" in response.text
    assert "同站同采样时间冲突" in response.text

    api = client.get("/api/current").json()
    assert api["version"] == 1
    assert "0.42" in client.get("/download/current").text


def test_row_errors_are_displayed(client):
    bad = (
        "站点,采样时间,结果,单位\n"
        ",2026-11-02 08:00,0.3,mg/L\n"
        "闸口站,,0.3,mg/L\n"
    ).encode("utf-8")
    response = upload(client, "bad.csv", bad)
    assert response.status_code == 422
    assert "数据错误" in response.text
    assert "站点不能为空" in response.text
    assert "采样时间不能为空" in response.text


def test_oversized_upload_is_rejected(client, monkeypatch):
    client.settings = Settings(
        data_dir=client.settings.data_dir, max_upload_bytes=2048
    )
    app = create_app(client.settings)
    with TestClient(app) as small_client:
        big = ("站点,采样时间,结果,单位\n" + "闸口站,2026-11-02 08:00,1,mg/L\n" * 200).encode()
        response = small_client.post(
            "/upload", files={"file": ("big.csv", big, "text/csv")}
        )
        assert response.status_code == 413
        assert "文件过大" in response.text
        assert small_client.get("/api/current").json()["version"] is None


def test_version_detail_and_history_pages(client):
    upload(client, "lab.csv", GOOD)
    assert client.get("/versions").status_code == 200
    detail = client.get("/versions/1")
    assert detail.status_code == 200
    assert "闸口站" in detail.text
    assert "SHA-256（发布内容）" in detail.text
    assert client.get("/versions/99").status_code == 404


def test_download_specific_version_keeps_old_data(client):
    upload(client, "lab.csv", GOOD)
    revised = (
        "站点,采样时间,结果,单位\n"
        "闸口站,2026-11-09 08:00,0.51,mg/L\n"
    ).encode("utf-8")
    upload(client, "next.csv", revised)

    v1 = client.get("/download/1")
    v2 = client.get("/download/2")
    assert "2026-11-02" in v1.text and "2026-11-09" not in v1.text
    assert "2026-11-09" in v2.text
    assert v1.headers["X-Snapshot-Version"] == "1"
    current = client.get("/download/current")
    assert current.headers["X-Snapshot-Version"] == "2"


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_upload_without_file_field_returns_422(client):
    assert client.post("/upload").status_code == 422


def test_download_falls_back_to_newest_file_when_database_unreadable(client, monkeypatch):
    upload(client, "lab.csv", GOOD)
    import app.main as main_module

    def broken_connect(*args, **kwargs):
        raise __import__("sqlite3").OperationalError("simulated outage")

    monkeypatch.setattr(main_module, "connect", broken_connect)
    response = client.get("/download/current")
    assert response.status_code == 200
    assert "闸口站" in response.text
    assert response.headers["X-Snapshot-Version"] == "1"


def test_home_page_degrades_when_database_unreadable(client, monkeypatch):
    upload(client, "lab.csv", GOOD)
    import sqlite3

    import app.main as main_module

    def broken_connect(*args, **kwargs):
        raise sqlite3.OperationalError("simulated outage")

    monkeypatch.setattr(main_module, "connect", broken_connect)
    response = client.get("/")
    assert response.status_code == 200
    assert "数据库暂时无法读取" in response.text or "尚未发布任何版本" in response.text


def test_upload_failure_keeps_old_version_downloadable(client):
    upload(client, "lab.csv", GOOD)

    def boom(version_no):
        raise __import__("sqlite3").OperationalError("simulated database failure")

    client.app.state.publisher.before_register = boom
    try:
        response = client.post(
            "/upload",
            files={
                "file": (
                    "next.csv",
                    "站点,采样时间,结果,单位\n闸口站,2026-11-09 08:00,0.9,mg/L\n".encode(
                        "utf-8"
                    ),
                )
            },
        )
    finally:
        client.app.state.publisher.before_register = None

    assert response.status_code == 500
    assert "发布失败" in response.text
    assert "当前版本仍为 v1" in response.text
    assert "simulated database failure" in response.text

    download = client.get("/download/current")
    assert download.status_code == 200
    assert download.headers["X-Snapshot-Version"] == "1"
    assert "0.42" in download.text
    assert "2026-11-09" not in download.text


def test_degraded_download_never_serves_unregistered_file(client):
    """A file left by a failed publish must not leak when the DB is down."""

    import sqlite3

    import app.main as main_module

    upload(client, "lab.csv", GOOD)
    storage = client.app.state.storage
    # Simulate a crash between atomic file write and commit.
    storage.write_version(2, "站点,采样时间,结果(mg/L),来源单位,原始结果\n坏站,2026-11-09 00:00:00,9.9,mg/L,9.9\n".encode("utf-8"))

    def broken_connect(*args, **kwargs):
        raise sqlite3.OperationalError("simulated outage")

    original = main_module.connect
    # Keep publisher/db reads intact at import level by patching only app.main's
    # reference, which every page and download handler uses.
    main_module.connect = broken_connect
    try:
        current = client.get("/download/current")
        specific = client.get("/download/2")
    finally:
        main_module.connect = original

    assert current.status_code == 200
    assert current.headers["X-Snapshot-Version"] == "1"
    assert "坏站" not in current.text
    assert specific.status_code == 503


def test_pointer_is_written_after_publish(client):
    upload(client, "lab.csv", GOOD)
    assert client.app.state.storage.read_current_pointer() == 1
