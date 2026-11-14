"""CSV ingestion, validation and conflict detection.

An upload is treated as a *complete snapshot*: the file must pass every
validation rule before anything becomes visible.  Results are converted to
mg/L.  Within one snapshot, two rows for the same station and sampling time
must agree after conversion; disagreements are reported as conflicts and the
whole upload is rejected (the previous published version stays current).
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from .units import (
    MG_PER_L,
    UG_PER_L,
    canonical_decimal,
    convert_to_mg_l,
    normalize_unit,
    parse_result,
)

# Column names accepted for each logical field.
HEADER_ALIASES = {
    "station": {
        "station",
        "station id",
        "site",
        "site id",
        "站点",
        "站点编号",
        "监测站",
        "监测站点",
        "断面",
    },
    "sampled_at": {
        "sampled at",
        "sample time",
        "sampling time",
        "sample datetime",
        "datetime",
        "time",
        "采样时间",
        "采样日期",
        "采集时间",
        "监测时间",
    },
    "result": {
        "result",
        "value",
        "concentration",
        "ammonia nitrogen",
        "ammonia",
        "nh3 n",
        "nh3n",
        "结果",
        "氨氮",
        "氨氮结果",
        "浓度",
        "监测结果",
    },
    "unit": {"unit", "units", "单位"},
}

# Order used when generating the published download header.
REQUIRED_FIELDS = ("station", "sampled_at", "result", "unit")

FIELD_LABELS = {
    "station": "站点",
    "sampled_at": "采样时间",
    "result": "结果",
    "unit": "单位",
}

OUTPUT_HEADER = ["站点", "采样时间", "结果(mg/L)", "来源单位", "原始结果"]

_ENCODINGS = ("utf-8-sig", "utf-8", "gb18030")

DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y%m%d %H%M%S",
    "%Y%m%d%H%M%S",
    "%Y%m%d",
    "%Y-%m-%d %H:%M:%S.%f",
)

MAX_REPORTED_ERRORS = 50
MAX_REPORTED_CONFLICTS = 20
MAX_STATION_LENGTH = 200


class SnapshotError(ValueError):
    """Raised when the uploaded snapshot fails validation."""

    def __init__(self, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {"errors": [], "conflicts": []}


@dataclass(frozen=True)
class Observation:
    station: str
    sampled_at: str  # canonical UTC timestamp, second precision
    result_mg_l: str  # canonical decimal string
    source_unit: str
    source_value: str
    source_line: int


@dataclass
class ParsedSnapshot:
    observations: list[Observation]
    warnings: list[dict] = field(default_factory=list)

    @property
    def row_count(self) -> int:
        return len(self.observations)

    def to_csv_bytes(self) -> bytes:
        return render_snapshot_csv(self.observations)

    def data_fingerprint(self) -> str:
        """Stable digest of the published data, independent of file spelling.

        Two uploads that describe the same station/time/result set are the same
        snapshot even if their headers, delimiters, units or source columns
        differ, so they must not create two versions.
        """

        digest = hashlib.sha256()
        for obs in self.observations:
            digest.update(
                (
                    obs.station
                    + "\x1f"
                    + obs.sampled_at
                    + "\x1f"
                    + obs.result_mg_l
                    + "\x1e"
                ).encode("utf-8")
            )
        return digest.hexdigest()


def detect_delimiter(text: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(text[:4096], delimiters=",;\t|")
        return dialect.delimiter
    except csv.Error:
        return ","


def decode_bytes(raw: bytes) -> str:
    for encoding in _ENCODINGS:
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise SnapshotError(
        "无法读取文件编码（支持 UTF-8 与 GB18030），请另存为 UTF-8 CSV 后重试"
    )


def _normalize_header(name: str) -> str:
    cleaned = name.replace("﻿", "").strip().casefold()
    # Drop a parenthesised unit hint such as "result (mg/L)"; the unit is a
    # separate column and the hint would otherwise hide the alias.
    for opener in ("(", "（"):
        if opener in cleaned:
            cleaned = cleaned.split(opener, 1)[0]
    cleaned = cleaned.replace("_", " ").replace("-", " ")
    return " ".join(cleaned.split())


def _map_headers(header_row: list[str]) -> dict[str, int]:
    normalized = [_normalize_header(h) for h in header_row]
    mapping: dict[str, int] = {}
    used_indexes: set[int] = set()
    for field_name, aliases in HEADER_ALIASES.items():
        for index, name in enumerate(normalized):
            if name in aliases and index not in used_indexes:
                mapping[field_name] = index
                used_indexes.add(index)
                break
    missing = [FIELD_LABELS[f] for f in REQUIRED_FIELDS if f not in mapping]
    if missing:
        found = [h for h in header_row if h.strip()]
        raise SnapshotError(
            "缺少必需列：" + "、".join(missing) + "。表头需包含站点、采样时间、结果、单位。"
            + (f" 当前表头：{', '.join(found)}" if found else " 文件没有表头。")
        )
    return mapping


def parse_datetime(raw: str) -> str:
    """Parse a sampling time and normalize it to ``YYYY-MM-DD HH:MM:SS`` UTC.

    Naive timestamps are interpreted as UTC so that identical strings always
    map to the same key regardless of the server's local timezone.
    """

    text = (raw or "").strip()
    if not text:
        raise ValueError("采样时间不能为空")

    iso_text = text.replace("Z", "+00:00").replace("z", "+00:00")
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(iso_text)
    except ValueError:
        for fmt in DATETIME_FORMATS:
            try:
                parsed = datetime.strptime(text.replace("/", "-"), fmt.replace("/", "-"))
                break
            except ValueError:
                continue
    if parsed is None:
        raise ValueError(f"无法解析采样时间：{text}")
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    if parsed.year < 1970 or parsed.year > 2100:
        raise ValueError(f"采样时间超出合理范围：{text}")
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def parse_snapshot(raw: bytes, *, max_rows: int | None = None) -> ParsedSnapshot:
    """Parse and fully validate a snapshot; raise ``SnapshotError`` on failure.

    All row-level errors are collected and reported together so the
    administrator can fix the lab export in one pass.
    """

    text = decode_bytes(raw)
    if not text.strip():
        raise SnapshotError("文件为空，缺少表头和结果数据。")

    delimiter = detect_delimiter(text)
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    try:
        header_row = next(reader)
    except StopIteration:
        raise SnapshotError("文件为空，缺少表头。")

    mapping = _map_headers(header_row)
    errors: list[dict] = []
    warnings: list[dict] = []
    groups: dict[tuple[str, str], list[Observation]] = {}
    max_index = max(mapping.values())
    total_errors = 0
    row_count = 0

    for line_no, row in enumerate(reader, start=2):
        if not any(cell.strip() for cell in row):
            continue  # tolerate blank separator lines
        row_count += 1
        if max_rows is not None and row_count > max_rows:
            raise SnapshotError(
                f"文件行数超过上限（{max_rows} 行），请拆分后重试。",
                {"errors": errors, "conflicts": []},
            )
        if len(row) <= max_index:
            total_errors += 1
            _append_limited(
                errors,
                {
                    "line": line_no,
                    "message": f"列数不足，应有 {len(header_row)} 列，实际 {len(row)} 列",
                },
            )
            continue

        raw_station = row[mapping["station"]]
        raw_time = row[mapping["sampled_at"]]
        raw_result = row[mapping["result"]]
        raw_unit = row[mapping["unit"]]

        row_errors: list[str] = []
        station = raw_station.strip()
        if not station:
            row_errors.append("站点不能为空")
        elif len(station) > MAX_STATION_LENGTH:
            row_errors.append(f"站点名称过长（超过 {MAX_STATION_LENGTH} 个字符）")

        sampled_at = ""
        try:
            sampled_at = parse_datetime(raw_time)
        except ValueError as exc:
            row_errors.append(str(exc))

        unit = ""
        try:
            unit = normalize_unit(raw_unit)
        except ValueError as exc:
            row_errors.append(str(exc))

        result_value: Decimal | None = None
        try:
            result_value = parse_result(raw_result)
        except ValueError as exc:
            row_errors.append(str(exc))

        if row_errors:
            total_errors += 1
            _append_limited(
                errors, {"line": line_no, "message": "；".join(row_errors)}
            )
            continue

        assert result_value is not None
        observation = Observation(
            station=station,
            sampled_at=sampled_at,
            result_mg_l=canonical_decimal(convert_to_mg_l(result_value, unit)),
            source_unit=unit,
            source_value=raw_result.strip(),
            source_line=line_no,
        )
        groups.setdefault((station, sampled_at), []).append(observation)

    if row_count == 0:
        total_errors += 1
        _append_limited(errors, {"line": 2, "message": "文件中没有数据行"})

    conflicts: list[dict] = []
    conflict_count = 0
    for (station, sampled_at), group in groups.items():
        distinct = {Decimal(obs.result_mg_l) for obs in group}
        if len(distinct) > 1:
            conflict_count += 1
            if len(conflicts) < MAX_REPORTED_CONFLICTS:
                conflicts.append(
                    {
                        "station": station,
                        "sampled_at": sampled_at,
                        "readings": [
                            {
                                "result_mg_l": obs.result_mg_l,
                                "line": obs.source_line,
                                "source_value": obs.source_value,
                                "source_unit": obs.source_unit,
                            }
                            for obs in group
                        ],
                    }
                )
        elif len(group) > 1:
            first = group[0]
            for duplicate in group[1:]:
                _append_limited(
                    warnings,
                    {
                        "line": duplicate.source_line,
                        "station": station,
                        "sampled_at": sampled_at,
                        "message": (
                            f"重复行已合并：{station} @ {sampled_at} "
                            f"与第 {first.source_line} 行结果一致"
                        ),
                    },
                )

    if total_errors or conflict_count:
        parts: list[str] = []
        if conflict_count:
            parts.append(f"{conflict_count} 处同站同采样时间结果冲突")
        if total_errors:
            parts.append(f"{total_errors} 行数据错误")
        raise SnapshotError(
            "校验未通过：" + "，".join(parts) + "。本次上传未发布，当前版本保持不变。",
            {
                "errors": errors,
                "conflicts": conflicts,
                "error_count": total_errors,
                "conflict_count": conflict_count,
            },
        )

    observations = sorted(
        (group[0] for group in groups.values()),
        key=lambda obs: (obs.station, obs.sampled_at),
    )
    return ParsedSnapshot(observations=observations, warnings=warnings)


def _append_limited(bucket: list[dict], item: dict) -> None:
    if len(bucket) < MAX_REPORTED_ERRORS:
        bucket.append(item)


def render_snapshot_csv(observations: list[Observation]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(OUTPUT_HEADER)
    for obs in observations:
        writer.writerow(
            [
                obs.station,
                obs.sampled_at,
                obs.result_mg_l,
                obs.source_unit,
                obs.source_value,
            ]
        )
    return buffer.getvalue().encode("utf-8")


def observations_from_rows(rows) -> list[Observation]:
    """Rebuild observations from database rows for re-rendering a download."""

    return [
        Observation(
            station=row["station"],
            sampled_at=row["sampled_at"],
            result_mg_l=row["result_mg_l"],
            source_unit=row["source_unit"],
            source_value=row["source_value"],
            source_line=row["source_line"],
        )
        for row in rows
    ]


__all__ = [
    "Observation",
    "ParsedSnapshot",
    "SnapshotError",
    "parse_snapshot",
    "parse_datetime",
    "render_snapshot_csv",
    "observations_from_rows",
    "OUTPUT_HEADER",
    "MG_PER_L",
    "UG_PER_L",
]
