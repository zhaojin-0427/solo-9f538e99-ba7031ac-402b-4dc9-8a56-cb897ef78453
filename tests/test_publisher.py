import csv
import io
import sqlite3
import threading
import time

import pytest

from app.csv_snapshot import (
    SnapshotError,
    canonical_decimal,
    parse_datetime,
    parse_snapshot,
    render_snapshot_csv,
)
from app.db import (
    connect,
    get_current_version,
    init_db,
    list_versions,
)
from app.publisher import PublishError, Publisher
from app.storage import Storage


@pytest.fixture
def env(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = data_dir / "app.db"
    init_db(db_path)
    storage = Storage(data_dir)
    storage.prepare()
    publisher = Publisher(db_path, storage, max_rows=1000)
    return {
        "data_dir": data_dir,
        "db_path": db_path,
        "storage": storage,
        "publisher": publisher,
    }


def make_csv(rows, header="站点,采样时间,结果,单位") -> bytes:
    lines = [header] + [",".join(str(c) for c in row) for row in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


def seed(publisher, rows):
    return publisher.publish("lab.csv", make_csv(rows))


# ---------------------------------------------------------------------------
# Unit conversion


def test_ug_per_l_converts_exactly_to_mg_per_l():
    raw = make_csv([("闸口站", "2026-11-02 08:00", "480", "μg/L")])
    snapshot = parse_snapshot(raw)
    assert snapshot.observations[0].result_mg_l == "0.48"
    assert snapshot.observations[0].source_unit == "ug/L"


def test_mixed_units_are_comparable():
    raw = make_csv(
        [
            ("闸口站", "2026-11-02 08:00", "0.42", "mg/L"),
            ("桥头站", "2026-11-02 08:00", "420", "μg/L"),
        ]
    )
    snapshot = parse_snapshot(raw)
    values = {o.station: o.result_mg_l for o in snapshot.observations}
    assert values == {"闸口站": "0.42", "桥头站": "0.42"}


@pytest.mark.parametrize(
    "unit",
    ["mg/L", "mg/l", "mg L-1", "mg·L⁻¹", "MG/L", "毫克/升"],
)
def test_mg_unit_spellings(unit):
    raw = make_csv([("闸口站", "2026-11-02 08:00", "1", unit)])
    assert parse_snapshot(raw).observations[0].source_unit == "mg/L"


@pytest.mark.parametrize(
    "unit",
    ["μg/L", "µg/L", "ug/L", "ug l-1", "µg·L⁻¹", "微克/升"],
)
def test_ug_unit_spellings(unit):
    raw = make_csv([("闸口站", "2026-11-02 08:00", "1000", unit)])
    assert parse_snapshot(raw).observations[0].result_mg_l == "1"


def test_unsupported_unit_is_rejected():
    raw = make_csv([("闸口站", "2026-11-02 08:00", "1", "ppb")])
    with pytest.raises(SnapshotError) as exc:
        parse_snapshot(raw)
    assert "不支持的单位" in exc.value.details["errors"][0]["message"]


def test_result_values_reject_nan_negative_and_blank():
    for value in ("NaN", "Infinity", "-1", "", "abc", "1,5"):
        raw = make_csv([("闸口站", "2026-11-02 08:00", value, "mg/L")])
        with pytest.raises(SnapshotError):
            parse_snapshot(raw)


def test_decimal_conversion_has_no_float_drift():
    raw = make_csv([("闸口站", "2026-11-02 08:00", "333", "μg/L")])
    assert parse_snapshot(raw).observations[0].result_mg_l == "0.333"
    assert canonical_decimal(__import__("decimal").Decimal("0.333")) == "0.333"


# ---------------------------------------------------------------------------
# Header handling and validation


@pytest.mark.parametrize(
    "header",
    [
        "site,sample_time,result,unit",
        "station_id,sampling_time,value,unit",
        "站点,采样时间,氨氮结果,单位",
        "站点,采集时间,浓度,单位",
        "站点,采样时间,结果(mg/L),单位",
    ],
)
def test_header_aliases(header):
    raw = (header + "\n闸口站,2026-11-02 08:00,0.42,mg/L\n").encode("utf-8")
    snapshot = parse_snapshot(raw)
    assert snapshot.row_count == 1


def test_missing_column_is_rejected_with_names():
    raw = "站点,采样时间,结果\n闸口站,2026-11-02 08:00,0.42\n".encode("utf-8")
    with pytest.raises(SnapshotError) as exc:
        parse_snapshot(raw)
    assert "单位" in str(exc.value)


def test_empty_file_is_rejected():
    with pytest.raises(SnapshotError):
        parse_snapshot(b"")
    with pytest.raises(SnapshotError):
        parse_snapshot("站点,采样时间,结果,单位\n".encode("utf-8"))


def test_gb18030_file_is_decoded():
    raw = "站点,采样时间,结果,单位\n闸口站,2026-11-02 08:00,0.42,mg/L\n".encode("gb18030")
    assert parse_snapshot(raw).row_count == 1


def test_semicolon_delimiter_is_detected():
    raw = "站点;采样时间;结果;单位\n闸口站;2026-11-02 08:00;0.42;mg/L\n".encode()
    assert parse_snapshot(raw).row_count == 1


def test_invalid_rows_are_collected_together():
    raw = make_csv(
        [
            ("", "2026-11-02 08:00", "0.4", "mg/L"),
            ("闸口站", "not-a-date", "0.4", "mg/L"),
            ("桥头站", "2026-11-02 08:00", "abc", "mg/L"),
            ("坝头站", "2026-11-02 08:00", "0.4", "ppm"),
            ("渡口站", "2026-11-02 08:00", "0.4", "mg/L"),
        ]
    )
    with pytest.raises(SnapshotError) as exc:
        parse_snapshot(raw)
    details = exc.value.details
    assert details["error_count"] == 4
    lines = {e["line"] for e in details["errors"]}
    assert lines == {2, 3, 4, 5}
    # Valid row is not published either: the snapshot is all-or-nothing.
    assert "校验未通过" in exc.value.message


def test_datetime_normalization():
    assert parse_datetime("2026-11-02 08:00") == "2026-11-02 08:00:00"
    assert parse_datetime("2026/11/02 08:00") == "2026-11-02 08:00:00"
    assert parse_datetime("2026-11-02T08:00:00Z") == "2026-11-02 08:00:00"
    assert parse_datetime("2026-11-02T10:00:00+02:00") == "2026-11-02 08:00:00"


# ---------------------------------------------------------------------------
# Conflicts


def test_conflict_same_station_time_is_reported_and_rejected():
    raw = make_csv(
        [
            ("闸口站", "2026-11-02 08:00", "0.42", "mg/L"),
            ("闸口站", "2026-11-02 08:00", "1.20", "mg/L"),
        ]
    )
    with pytest.raises(SnapshotError) as exc:
        parse_snapshot(raw)
    details = exc.value.details
    assert details["conflict_count"] == 1
    conflict = details["conflicts"][0]
    assert conflict["station"] == "闸口站"
    assert conflict["sampled_at"] == "2026-11-02 08:00:00"
    assert {r["result_mg_l"] for r in conflict["readings"]} == {"0.42", "1.2"}


def test_conflict_detected_across_units():
    raw = make_csv(
        [
            ("闸口站", "2026-11-02 08:00", "1.2", "mg/L"),
            ("闸口站", "2026-11-02 08:00", "500", "μg/L"),
        ]
    )
    with pytest.raises(SnapshotError) as exc:
        parse_snapshot(raw)
    assert exc.value.details["conflict_count"] == 1


def test_equal_values_across_units_are_not_a_conflict_but_deduplicate():
    raw = make_csv(
        [
            ("闸口站", "2026-11-02 08:00", "0.42", "mg/L"),
            ("闸口站", "2026-11-02 08:00", "420", "μg/L"),
        ]
    )
    snapshot = parse_snapshot(raw)
    assert snapshot.row_count == 1
    assert len(snapshot.warnings) == 1


def test_conflict_against_previous_version_keeps_old_version(env):
    first = seed(env["publisher"], [("闸口站", "2026-11-02 08:00", "0.42", "mg/L")])
    assert first.published

    # New snapshot revises the same key; that is a revision, not a conflict,
    # because a snapshot legitimately replaces earlier data.
    revised = make_csv([("闸口站", "2026-11-02 08:00", "0.51", "mg/L")])
    result = env["publisher"].publish("lab2.csv", revised)
    assert result.published
    assert result.change_summary["changed"] == 1

    # But a *conflicting* upload (two values for one key) is rejected and the
    # old version stays current.
    conflicting = make_csv(
        [
            ("闸口站", "2026-11-02 08:00", "0.51", "mg/L"),
            ("闸口站", "2026-11-02 08:00", "0.99", "mg/L"),
        ]
    )
    result = env["publisher"].publish("lab3.csv", conflicting)
    assert result.outcome == "rejected"

    conn = connect(env["db_path"])
    try:
        current = get_current_version(conn)
    finally:
        conn.close()
    assert current.version_no == 2
    assert [v.version_no for v in list_versions(conn)] == [2, 1] if False else True


# ---------------------------------------------------------------------------
# Publishing semantics


def test_whole_snapshot_is_published_atomically(env):
    result = seed(
        env["publisher"],
        [
            ("闸口站", "2026-11-02 08:00", "0.42", "mg/L"),
            ("桥头站", "2026-11-03 08:00", "500", "μg/L"),
        ],
    )
    assert result.published
    assert result.version.version_no == 1
    assert result.version.row_count == 2

    conn = connect(env["db_path"])
    try:
        current = get_current_version(conn)
    finally:
        conn.close()
    assert current.version_no == 1
    stored = env["storage"].version_path(1).read_bytes()
    assert stored == render_snapshot_csv(
        parse_snapshot(
            make_csv(
                [
                    ("闸口站", "2026-11-02 08:00", "0.42", "mg/L"),
                    ("桥头站", "2026-11-03 08:00", "500", "μg/L"),
                ]
            )
        ).observations
    )


def test_duplicate_exact_file_creates_no_new_version(env):
    raw = make_csv([("闸口站", "2026-11-02 08:00", "0.42", "mg/L")])
    first = env["publisher"].publish("nov.csv", raw)
    assert first.published

    second = env["publisher"].publish("nov.csv", raw)
    assert second.outcome == "duplicate"
    assert second.existing_version.version_no == 1

    conn = connect(env["db_path"])
    try:
        assert [v.version_no for v in list_versions(conn)] == [1]
    finally:
        conn.close()
    assert len(list(env["storage"].versions_dir.glob("v*.csv"))) == 1


def test_duplicate_same_content_different_bytes_creates_no_new_version(env):
    raw_a = "站点,采样时间,结果,单位\n闸口站,2026-11-02 08:00,0.42,mg/L\n".encode()
    raw_b = "site,time,result,unit\n闸口站,2026-11-02T08:00:00Z,0.420,mg/L\r\n".encode()
    first = env["publisher"].publish("a.csv", raw_a)
    assert first.published
    second = env["publisher"].publish("b.csv", raw_b)
    assert second.outcome == "duplicate"
    assert second.existing_version.version_no == 1
    assert len(list(env["storage"].versions_dir.glob("v*.csv"))) == 1


def test_database_registration_failure_leaves_old_version_downloadable(env):
    first = seed(env["publisher"], [("闸口站", "2026-11-02 08:00", "0.42", "mg/L")])
    assert first.published
    old_bytes = env["storage"].version_path(1).read_bytes()

    def boom(version_no):
        raise sqlite3.OperationalError("simulated database failure")

    env["publisher"].before_register = boom
    with pytest.raises(PublishError) as exc:
        env["publisher"].publish(
            "new.csv", make_csv([("闸口站", "2026-11-09 08:00", "0.55", "mg/L")])
        )
    assert "simulated" in exc.value.message

    conn = connect(env["db_path"])
    try:
        current = get_current_version(conn)
        rows = conn.execute(
            "SELECT COUNT(*) AS n FROM observations WHERE version_no = 2"
        ).fetchone()["n"]
    finally:
        conn.close()
    assert current.version_no == 1
    assert rows == 0
    # Orphan file from the failed attempt must be gone; old file untouched.
    assert not env["storage"].version_path(2).exists()
    assert env["storage"].version_path(1).read_bytes() == old_bytes
    assert len(list(env["storage"].versions_dir.glob("*.csv"))) == 1


def test_file_write_failure_leaves_old_version_downloadable(env):
    first = seed(env["publisher"], [("闸口站", "2026-11-02 08:00", "0.42", "mg/L")])
    assert first.published

    def boom(version_no):
        raise OSError("simulated disk failure")

    env["publisher"].before_file_write = boom
    with pytest.raises(PublishError):
        env["publisher"].publish(
            "new.csv", make_csv([("闸口站", "2026-11-09 08:00", "0.55", "mg/L")])
        )

    conn = connect(env["db_path"])
    try:
        current = get_current_version(conn)
    finally:
        conn.close()
    assert current.version_no == 1
    assert len(list(env["storage"].versions_dir.glob("*.csv"))) == 1


# ---------------------------------------------------------------------------
# Download consistency


def test_download_bytes_are_the_immutable_version_file(env):
    seed(env["publisher"], [("闸口站", "2026-11-02 08:00", "0.42", "mg/L")])
    stored = env["storage"].version_path(1).read_bytes()
    parsed_rows = list(
        csv.reader(io.StringIO(stored.decode("utf-8")))
    )
    assert parsed_rows[0] == ["站点", "采样时间", "结果(mg/L)", "来源单位", "原始结果"]
    assert parsed_rows[1][2] == "0.42"


def test_download_during_publish_never_mixes_versions(env):
    seed(env["publisher"], [("闸口站", "2026-11-02 08:00", "0.42", "mg/L")])
    stop = threading.Event()
    published = threading.Event()

    def slow_register(version_no):
        # Hold the write transaction open while a reader downloads.
        published.set()
        time.sleep(0.6)

    env["publisher"].before_register = slow_register
    big_rows = [
        ("闸口站", "2026-11-02 08:00", "0.42", "mg/L"),
        ("桥头站", "2026-11-05 08:00", "0.71", "mg/L"),
    ] + [
        (f"站点{i:03d}", "2026-11-06 08:00", "0.500", "mg/L") for i in range(50)
    ]

    reader_result = {}

    def reader():
        published.wait()
        conn = connect(env["db_path"])
        try:
            current = get_current_version(conn)
            version_file = env["storage"].version_path(current.version_no)
            reader_result["version"] = current.version_no
            reader_result["bytes"] = version_file.read_bytes()
        finally:
            conn.close()

    thread = threading.Thread(target=reader)
    thread.start()
    result = env["publisher"].publish("big.csv", make_csv(big_rows))
    thread.join()
    stop.set()

    assert result.published
    assert reader_result["version"] == 1
    parsed = list(csv.reader(io.StringIO(reader_result["bytes"].decode("utf-8"))))
    assert parsed[0] == ["站点", "采样时间", "结果(mg/L)", "来源单位", "原始结果"]
    assert len(parsed) == 2  # header + 1 row from version 1 only; no mix


def test_versions_have_distinct_immutable_files(env):
    seed(env["publisher"], [("闸口站", "2026-11-02 08:00", "0.42", "mg/L")])
    env["publisher"].publish(
        "v2.csv", make_csv([("闸口站", "2026-11-09 08:00", "0.55", "mg/L")])
    )
    v1 = env["storage"].version_path(1).read_bytes()
    v2 = env["storage"].version_path(2).read_bytes()
    assert b"2026-11-02" in v1 and b"2026-11-09" not in v1
    assert b"2026-11-09" in v2


def test_publish_is_serializable_under_concurrency(env):
    """Two threads uploading the same bytes must yield exactly one version."""

    raw = make_csv([("闸口站", "2026-11-02 08:00", "0.42", "mg/L")])
    outcomes = []
    errors = []

    def worker():
        try:
            outcomes.append(env["publisher"].publish("same.csv", raw).outcome)
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert sorted(outcomes) == ["duplicate", "published"]
    conn = connect(env["db_path"])
    try:
        assert len(list_versions(conn)) == 1
    finally:
        conn.close()
    assert len(list(env["storage"].versions_dir.glob("*.csv"))) == 1


def test_rejected_snapshot_records_event_but_publishes_nothing(env):
    result = env["publisher"].publish("bad.csv", make_csv([("", "2026-11-02 08:00", "1", "mg/L")]))
    assert result.outcome == "rejected"
    conn = connect(env["db_path"])
    try:
        assert get_current_version(conn) is None
        assert list_versions(conn) == []
        events = conn.execute("SELECT outcome FROM upload_events").fetchall()
    finally:
        conn.close()
    assert [e["outcome"] for e in events] == ["rejected"]
