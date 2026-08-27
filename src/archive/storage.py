"""
FinancialJuice Storage
Saves news by ET date into data/{YYYY-MM-DD}/ directory.
"""

import os
import json
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

from src.core.config import DATA_DIR
from src.utils.LoggerManager import logger

ET = ZoneInfo("America/New_York")
RAW_MAX_RECORDS = 20_000
RAW_MAX_SERIALIZED_BYTES = 64 * 1024 * 1024
BREAKING_MAX_RECORDS = 10_000
BREAKING_MAX_SERIALIZED_BYTES = 48 * 1024 * 1024


def get_et_date():
    """Return current date string in ET timezone."""
    return datetime.now(ET).strftime("%Y-%m-%d")


def get_data_dir(date_str: Optional[str] = None) -> str:
    """Get (and create) the data directory for a given date."""
    date_str = date_str or get_et_date()
    d = os.path.join(DATA_DIR, date_str)
    os.makedirs(d, exist_ok=True)
    return d


def _atomic_write_json(path: str, data: Any) -> None:
    """Write JSON atomically via temp file + rename."""
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, path)


def load_json_state(path: str, max_bytes: Optional[int] = None) -> dict[str, Any]:
    """Load a JSON object, returning an empty object for missing/corrupt state."""
    if not os.path.exists(path):
        return {}
    try:
        if max_bytes is not None:
            size = os.path.getsize(path)
            if size > max_bytes:
                logger.warning(
                    f"Processor state oversized at {path}: size={size} limit={max_bytes}; "
                    "starting empty"
                )
                return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("state root is not an object")
        return data
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        logger.warning(f"Processor state unavailable at {path}: {exc}; starting empty")
        return {}


def save_json_state(path: str, data: dict[str, Any]) -> None:
    """Persist a JSON object atomically, creating its parent directory."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _atomic_write_json(path, data)


def load_daily_archive_records(
    data_dir: str, date_str: Optional[str] = None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Load old-compatible current-day archive arrays for one-time baselining."""
    day_dir = os.path.join(data_dir, date_str or get_et_date())
    loaded_archives: list[list[dict[str, Any]]] = []
    for filename in ("raw.json", "breaking.json"):
        path = os.path.join(day_dir, filename)
        records: list[dict[str, Any]] = []
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as archive_file:
                    loaded = json.load(archive_file)
                if isinstance(loaded, list):
                    records = [record for record in loaded if isinstance(record, dict)]
                else:
                    logger.warning(f"Baseline archive is not an array: {path}")
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                logger.warning(f"Baseline archive unavailable at {path}: {exc}")
        loaded_archives.append(records)
    return loaded_archives[0], loaded_archives[1]


def _serialized_size(records: list[dict[str, Any]]) -> int:
    return len(json.dumps(records, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _rotate_records(
    records: list[dict[str, Any]], max_records: int, max_bytes: int, archive_name: str
) -> list[dict[str, Any]]:
    original_count = len(records)
    if len(records) > max_records:
        records = records[-max_records:]
    if _serialized_size(records) > max_bytes:
        retained_reversed: list[dict[str, Any]] = []
        approximate_bytes = 2
        for record in reversed(records):
            record_bytes = len(
                json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            )
            separator_bytes = 1 if retained_reversed else 0
            if approximate_bytes + separator_bytes + record_bytes > max_bytes:
                break
            retained_reversed.append(record)
            approximate_bytes += separator_bytes + record_bytes
        records = list(reversed(retained_reversed))
    removed = original_count - len(records)
    if removed:
        logger.warning(
            f"Rotated {archive_name}: removed={removed} retained={len(records)} "
            f"max_records={max_records} max_bytes={max_bytes}"
        )
    return records


def save_news_items_batch(
    items: list[dict[str, Any]], date_str: Optional[str] = None
) -> list[dict[str, Any]]:
    """Save multiple news items in a single read/write cycle. Returns list of new items."""
    data_dir = get_data_dir(date_str)
    raw_path = os.path.join(data_dir, "raw.json")

    existing: list[dict[str, Any]] = []
    if os.path.exists(raw_path):
        with open(raw_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, list):
                existing = [record for record in loaded if isinstance(record, dict)]

    existing_keys = {
        (
            str(n.get("news_id", "")),
            n.get("fingerprint") or "",
            n.get("revision") or 0,
            n.get("title", ""),
            n.get("time", ""),
        )
        for n in existing
    }
    new_items: list[dict[str, Any]] = []
    for item in items:
        key = (
            str(item.get("news_id", "")),
            item.get("fingerprint") or "",
            item.get("revision") or 0,
            item.get("title", ""),
            item.get("time", ""),
        )
        if key not in existing_keys:
            new_items.append(item)
            existing_keys.add(key)

    if new_items:
        existing.extend(new_items)
        existing = _rotate_records(
            existing, RAW_MAX_RECORDS, RAW_MAX_SERIALIZED_BYTES, "raw.json"
        )
        _atomic_write_json(raw_path, existing)

    return new_items


def save_breaking_item(item: dict[str, Any], date_str: Optional[str] = None) -> bool:
    """
    Save a breaking (red) news item to breaking.json.
    Deduplicates by news_id or title. Does not mutate the input item.
    """
    data_dir = get_data_dir(date_str)
    breaking_path = os.path.join(data_dir, "breaking.json")

    existing: list[dict[str, Any]] = []
    if os.path.exists(breaking_path):
        with open(breaking_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            if isinstance(loaded, list):
                existing = [record for record in loaded if isinstance(record, dict)]

    nid = str(item.get("news_id", ""))
    fingerprint = item.get("fingerprint") or ""
    revision = item.get("revision") or 0
    duplicate = any(
        str(record.get("news_id", "")) == nid
        and (record.get("fingerprint") or "") == fingerprint
        and (record.get("revision") or 0) == revision
        for record in existing
    )
    if duplicate:
        return False

    # Create record with timestamp (don't mutate input)
    record = {**item, "detected_at": datetime.now(ET).isoformat()}
    existing.append(record)
    existing = _rotate_records(
        existing,
        BREAKING_MAX_RECORDS,
        BREAKING_MAX_SERIALIZED_BYTES,
        "breaking.json",
    )

    _atomic_write_json(breaking_path, existing)
    return True
