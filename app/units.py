"""Unit handling and exact decimal helpers.

Every concentration is stored as a canonical decimal string in mg/L.  Decimal
arithmetic is used throughout so that a value such as ``500 ug/L`` becomes
exactly ``0.5`` rather than a binary floating point approximation.
"""

from __future__ import annotations

import re
import unicodedata
from decimal import Decimal, InvalidOperation

MG_PER_L = "mg/L"
UG_PER_L = "ug/L"

_CANONICAL_UNITS = {MG_PER_L, UG_PER_L}

# After normalization a unit looks like one of:
#   mg/l   mg/litre   mg/liter   mg/l-1   mgl   mgl-1
# and the same set with the ``ug`` prefix.
_UNIT_PATTERN = re.compile(
    r"^(?P<mass>mg|ug)"
    r"(?:"
    r"/(?:l|litre|liter)(?:-1|1)?"
    r"|l(?:-1|1)?"
    r")$"
)


class UnitError(ValueError):
    """Raised when a unit cannot be interpreted."""


def normalize_unit(raw: str) -> str:
    """Return ``mg/L`` or ``ug/L`` for a laboratory unit spelling.

    Unicode compatibility normalization turns ``µ``/``μ`` into ``u``, full
    width characters into ASCII and superscript ``¹`` into ``1``.  Spaces,
    middle dots, multiplication signs and Chinese words for milligram, microgram
    and per litre are then folded away before matching a small token grammar.
    """

    text = unicodedata.normalize("NFKC", (raw or "")).strip().casefold()
    if not text:
        raise UnitError("单位不能为空")

    text = text.replace("μ", "u").replace("µ", "u")
    text = text.replace("毫克", "mg").replace("微克", "ug")
    text = text.replace("每升", "/l").replace("升", "l")
    text = re.sub(r"[\s·•.*_()\[\]{}]+", "", text)
    text = text.replace("−", "-")

    match = _UNIT_PATTERN.match(text)
    if not match:
        raise UnitError(f"不支持的单位：{raw.strip()}（仅支持 mg/L 或 μg/L）")
    return MG_PER_L if match.group("mass") == "mg" else UG_PER_L


def convert_to_mg_l(value: Decimal, unit: str) -> Decimal:
    """Convert ``value`` from ``unit`` to mg/L exactly."""

    if unit == MG_PER_L:
        return value
    if unit == UG_PER_L:
        return value / Decimal(1000)
    raise UnitError(f"不支持的单位：{unit}")


def parse_result(raw: str) -> Decimal:
    """Parse a concentration value, rejecting blanks, NaN, infinities and negatives."""

    text = (raw or "").strip()
    if not text:
        raise ValueError("结果不能为空")
    # Accept a Unicode minus sign from spreadsheets.
    text = text.replace("−", "-")
    try:
        value = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"结果不是有效数字：{raw.strip()}") from exc
    if not value.is_finite():
        raise ValueError(f"结果不是有限数字：{raw.strip()}")
    if value < 0:
        raise ValueError(f"结果不能为负数：{raw.strip()}")
    return value


def canonical_decimal(value: Decimal) -> str:
    """Format a decimal without exponent and without meaningless trailing zeroes."""

    if value == 0:
        return "0"
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def is_canonical_unit(unit: str) -> bool:
    return unit in _CANONICAL_UNITS
