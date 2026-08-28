import asyncio
import copy
import hashlib
import json
import os
from typing import Any, Dict, List, Optional, Protocol

from src.archive.storage import (
    load_daily_archive_records,
    load_json_state,
    save_breaking_item,
    save_json_state,
    save_news_items_batch,
)
from src.core.config import DATA_DIR, TRANSLATE_ENABLED
from src.core.security_limits import MAX_ITEMS_PER_PROCESS_BATCH
from src.telegram.bot import tg_edit_message, tg_send_group_resumable as tg_send_group
from src.telegram.rendering import (
    MAX_RENDERED_HTML_LENGTH,
    MAX_TELEGRAM_CHUNKS,
    TELEGRAM_TEXT_LIMIT,
    TelegramRenderLimitError,
    normalize_upstream_html,
    render_message_chunks,
)
from src.translate.queue_worker import TranslationJob
from src.utils.LoggerManager import logger

STATE_VERSION = 4
MAX_STATE_ENTRIES = 5000
TRIMMED_STATE_ENTRIES = 2000
MAX_NEWS_ID_LENGTH = 128
MAX_TITLE_LENGTH = 32_768
MAX_DESCRIPTION_LENGTH = 250_000
MAX_SOURCE_TIME_LENGTH = 1_024
MAX_EURL_LENGTH = 8_192
MAX_TRANSLATION_INPUT_LENGTH = 120_000
MAX_PROCESSOR_STATE_BYTES = 256 * 1024 * 1024
MAX_PENDING_ARCHIVE_RECORDS = 128
MAX_PENDING_ARCHIVE_BYTES = 32 * 1024 * 1024
MAX_ARCHIVE_METADATA_LENGTH = 4_096
MAX_LEVEL_LENGTH = 1_024
MAX_SOURCE_METHOD_LENGTH = 256
MAX_RUNTIME_STATE_BYTES = 192 * 1024 * 1024
MAX_GLOBAL_PENDING_ARCHIVE_RECORDS = 4_096
MAX_GLOBAL_PENDING_ARCHIVE_BYTES = 128 * 1024 * 1024
ALLOWED_NOTIFICATION_KINDS = {
    "none", "breaking", "upgrade", "update", "legacy_breaking",
}
ALLOWED_PREFIXES = {
    "", "🚨 BREAKING\n", "🔺 UPGRADED\n", "UPDATE\n",
}
ALLOWED_PENDING_GROUP_KINDS = {
    "english_breaking",
    "english_upgrade",
    "english_update",
    "translation",
}

FIELD_LIMITS = {
    "news_id": MAX_NEWS_ID_LENGTH,
    "title": MAX_TITLE_LENGTH,
    "description": MAX_DESCRIPTION_LENGTH,
    "source_time": MAX_SOURCE_TIME_LENGTH,
    "eurl": MAX_EURL_LENGTH,
}


class TranslationSubmitter(Protocol):
    def submit(self, job: TranslationJob) -> bool:
        ...


def _safe_string(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _safe_nonnegative_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def _news_id(value: Any) -> str:
    if value is None or isinstance(value, bool):
        return ""
    if isinstance(value, (str, int)):
        return str(value).strip()
    return ""


def _normalized_content(
    item: Dict[str, Any], existing: Optional[Dict[str, Any]] = None
) -> Dict[str, str]:
    previous = existing or {}
    title = (
        _safe_string(item.get("Title"))
        if "Title" in item
        else _safe_string(previous.get("title"))
    )
    description = (
        _safe_string(item.get("Description"))
        if "Description" in item
        else _safe_string(previous.get("description"))
    )
    if "PostedLong" in item or "PostedShort" in item:
        posted_long = _safe_string(item.get("PostedLong")) if "PostedLong" in item else ""
        posted_short = _safe_string(item.get("PostedShort")) if "PostedShort" in item else ""
        source_time = posted_long or posted_short
    else:
        source_time = _safe_string(previous.get("source_time"))
    eurl = (
        _safe_string(item.get("EURL"))
        if "EURL" in item
        else _safe_string(previous.get("eurl"))
    )
    return {
        "title": title,
        "description": description,
        "source_time": source_time,
        "eurl": eurl,
    }


def _safe_log_text(text: str, max_length: int = 90) -> str:
    safe = "".join(char if char.isprintable() and char not in "\r\n" else " " for char in text)
    return safe[:max_length]


def _bounded_metadata(value: Any, max_length: int) -> Optional[str]:
    if not isinstance(value, str):
        return ""
    safe = "".join(
        char if char.isprintable() and char not in "\r\n" else " " for char in value
    )
    if len(safe) > max_length:
        return None
    return safe


def _oversized_field(news_id: str, content: Dict[str, str]) -> Optional[tuple[str, int, int]]:
    values = {"news_id": news_id, **content}
    for field, limit in FIELD_LIMITS.items():
        length = len(values[field])
        if length > limit:
            return field, length, limit
    return None


def _fingerprint(content: Dict[str, str]) -> str:
    editorial = {
        "title": content["title"],
        "description": content["description"],
    }
    payload = json.dumps(editorial, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _serialized_bytes(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _validate_archive_record(record: Any, expected_news_id: str) -> Optional[Dict[str, Any]]:
    if not isinstance(record, dict):
        return None
    allowed = {
        "news_id", "title", "description", "time", "link", "is_important",
        "level", "breaking", "fingerprint", "revision", "source_method",
        "content_revision", "upgraded",
    }
    if any(key not in allowed for key in record):
        return None
    nid = _news_id(record.get("news_id"))
    if nid != expected_news_id:
        return None
    content = {
        "title": _safe_string(record.get("title")),
        "description": _safe_string(record.get("description")),
        "source_time": _safe_string(record.get("time")),
        "eurl": _safe_string(record.get("link")),
    }
    if _oversized_field(nid, content) is not None:
        return None
    fingerprint = record.get("fingerprint")
    revision = record.get("revision")
    if not isinstance(fingerprint, str) or len(fingerprint) != 64:
        return None
    if any(char not in "0123456789abcdef" for char in fingerprint.lower()):
        return None
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
        return None
    level = record.get("level", "")
    source_method = record.get("source_method", "")
    if not isinstance(level, str) or len(level) > MAX_LEVEL_LENGTH:
        return None
    if not isinstance(source_method, str) or len(source_method) > MAX_SOURCE_METHOD_LENGTH:
        return None
    for bool_field in ("is_important", "breaking", "content_revision", "upgraded"):
        if bool_field in record and not isinstance(record[bool_field], bool):
            return None
    normalized: Dict[str, Any] = {
        "news_id": nid,
        "title": content["title"],
        "description": content["description"],
        "time": content["source_time"],
        "link": content["eurl"],
        "is_important": bool(record.get("is_important")),
        "level": level,
        "breaking": bool(record.get("breaking")),
        "fingerprint": fingerprint,
        "revision": revision,
        "source_method": _safe_string(record.get("source_method")),
    }
    for flag in ("content_revision", "upgraded"):
        if flag in record:
            normalized[flag] = bool(record[flag])
    return normalized


def _validate_pending_archive(
    value: Any, news_id: str, sink: str
) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(value, list):
        return []
    if len(value) > MAX_PENDING_ARCHIVE_RECORDS:
        logger.warning(
            f"Discard state entry news_id={news_id}: {sink} pending count={len(value)} "
            f"limit={MAX_PENDING_ARCHIVE_RECORDS}"
        )
        return None
    if _serialized_bytes(value) > MAX_PENDING_ARCHIVE_BYTES:
        logger.warning(
            f"Discard state entry news_id={news_id}: {sink} pending bytes exceed "
            f"{MAX_PENDING_ARCHIVE_BYTES}"
        )
        return None
    records: List[Dict[str, Any]] = []
    for record in value:
        validated = _validate_archive_record(record, news_id)
        if validated is None:
            logger.warning(f"Discard invalid {sink} pending record news_id={news_id}")
            continue
        records.append(validated)
    return records


def _validate_pending_group(value: Any) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {
        "revision", "kind", "digest", "chunks", "message_ids", "next_chunk_index"
    }:
        return None
    revision = value.get("revision")
    kind = value.get("kind")
    digest = value.get("digest")
    chunks = value.get("chunks")
    message_ids = value.get("message_ids")
    next_index = value.get("next_chunk_index")
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
        return None
    if kind not in ALLOWED_PENDING_GROUP_KINDS:
        return None
    if not isinstance(digest, str) or len(digest) != 64:
        return None
    if not isinstance(chunks, list) or len(chunks) > MAX_TELEGRAM_CHUNKS:
        return None
    if any(not isinstance(chunk, str) or len(chunk) > TELEGRAM_TEXT_LIMIT for chunk in chunks):
        return None
    if sum(len(chunk) for chunk in chunks) > MAX_RENDERED_HTML_LENGTH:
        return None
    if not isinstance(message_ids, list) or len(message_ids) > len(chunks):
        return None
    if any(not isinstance(message_id, int) or isinstance(message_id, bool) or message_id <= 0
           for message_id in message_ids):
        return None
    if not isinstance(next_index, int) or isinstance(next_index, bool):
        return None
    if next_index != len(message_ids) or next_index > len(chunks):
        return None
    expected_digest = hashlib.sha256("\0".join(chunks).encode("utf-8")).hexdigest()
    if digest != expected_digest:
        return None
    return {
        "revision": revision,
        "kind": kind,
        "digest": digest,
        "chunks": chunks,
        "message_ids": message_ids,
        "next_chunk_index": next_index,
    }


def _is_breaking(level: str, breaking: bool) -> bool:
    return breaking or "active" in level


def _english_text(prefix: str, content: Dict[str, str]) -> str:
    title = normalize_upstream_html(content["title"])
    description = normalize_upstream_html(content["description"])
    parts = [f"{prefix}{title}"]
    if description:
        parts.append(description)
    parts.append(f"Source time: {content['source_time']}")
    return "\n\n".join(parts)


def _translation_text(content: Dict[str, str]) -> str:
    title = normalize_upstream_html(content["title"])
    description = normalize_upstream_html(content["description"])
    if description:
        return f"{title}\n\n{description}"
    return title


class NewsProcessor:
    def __init__(
        self,
        translation_worker: Optional[TranslationSubmitter] = None,
        state_path: Optional[str] = None,
    ):
        self._translator = translation_worker
        self._state_repaired = False
        self._state_path = state_path or os.path.join(DATA_DIR, "news_processor_state.json")
        self._data_dir = DATA_DIR if state_path is None else os.path.dirname(self._state_path)
        state_existed = os.path.exists(self._state_path)
        self._state: Dict[str, Dict[str, Any]] = self._load_state()
        if not state_existed:
            self._state = self._migrate_archive_baseline()
        self._last_seen_seq = max(
            (int(state.get("last_seen_seq", 0)) for state in self._state.values()),
            default=0,
        )
        if not state_existed or self._state_repaired:
            self._persist()
        self._publication_lock = asyncio.Lock()

    def _migrate_archive_baseline(self) -> Dict[str, Dict[str, Any]]:
        raw_records, breaking_records = load_daily_archive_records(self._data_dir)
        breaking_ids = {
            _news_id(record.get("news_id"))
            for record in breaking_records
            if _news_id(record.get("news_id"))
        }
        latest: Dict[str, Dict[str, Any]] = {}
        for record in [*raw_records, *breaking_records]:
            nid = _news_id(record.get("news_id"))
            if not nid:
                continue
            prior = latest.get(nid)
            content = {
                "title": _safe_string(record.get("title"))
                if "title" in record else _safe_string(prior.get("title")) if prior else "",
                "description": _safe_string(record.get("description"))
                if "description" in record else _safe_string(prior.get("description")) if prior else "",
                "source_time": _safe_string(record.get("time"))
                if "time" in record else _safe_string(prior.get("source_time")) if prior else "",
                "eurl": _safe_string(record.get("link"))
                if "link" in record else _safe_string(prior.get("eurl")) if prior else "",
            }
            oversized = _oversized_field(nid, content)
            if oversized is not None:
                field, length, limit = oversized
                logger.warning(
                    f"Skip baseline news_id={_safe_log_text(nid, 128)} "
                    f"field={field} length={length} limit={limit}"
                )
                continue
            revision_raw = record.get("revision", prior.get("revision", 1) if prior else 1)
            try:
                revision = max(1, int(revision_raw))
            except (TypeError, ValueError):
                revision = prior.get("revision", 1) if prior else 1
            candidate_state = {
                "level": _safe_string(record.get("level")) or ("active" if nid in breaking_ids else ""),
                "breaking": bool(record.get("breaking")) or nid in breaking_ids,
                "fingerprint": _fingerprint(content),
                "revision": revision,
                **content,
                "telegram_message_id": None,
                "prefix": "UPDATE\n" if nid in breaking_ids else "",
                "notification_kind": "legacy_breaking" if nid in breaking_ids else "none",
                "source_method": _safe_string(record.get("source_method")) or "legacy_archive",
                "notification_revision": revision if nid in breaking_ids else 0,
                "telegram_revision": revision if nid in breaking_ids else 0,
                "telegram_is_group": False,
                "telegram_message_ids": [],
                "alert_recorded": nid in breaking_ids,
                "raw_archive_revision": revision,
                "breaking_archive_revision": revision if nid in breaking_ids else 0,
                "raw_archive_pending": [],
                "breaking_archive_pending": [],
                "pending_group": None,
                "last_seen_seq": 0,
            }
            if nid not in latest and len(latest) >= MAX_STATE_ENTRIES:
                logger.warning("Stop archive baseline at processor state entry limit")
                break
            prospective = {**latest, nid: candidate_state}
            if _serialized_bytes({"version": STATE_VERSION, "entries": prospective}) > MAX_RUNTIME_STATE_BYTES:
                logger.warning("Stop archive baseline at processor runtime state budget")
                break
            latest[nid] = candidate_state
        logger.info(
            f"Initialized processor baseline from current-day archives: "
            f"seen={len(latest)} legacy_alerts={len(breaking_ids & latest.keys())}"
        )
        return latest

    def _load_state(self) -> Dict[str, Dict[str, Any]]:
        data = load_json_state(self._state_path, max_bytes=MAX_PROCESSOR_STATE_BYTES)
        entries = data.get("entries", {}) if isinstance(data, dict) else {}
        if not isinstance(entries, dict):
            logger.warning("Processor state entries are invalid; starting empty")
            return {}
        if len(entries) > MAX_STATE_ENTRIES:
            logger.warning(
                f"Processor state entry count={len(entries)} exceeds {MAX_STATE_ENTRIES}; "
                "loading bounded prefix"
            )
        valid: Dict[str, Dict[str, Any]] = {}
        global_pending_count = 0
        global_pending_bytes = 0
        for index, (raw_id, raw_state) in enumerate(entries.items()):
            if index >= MAX_STATE_ENTRIES:
                break
            nid = _news_id(raw_id)
            if not nid or not isinstance(raw_state, dict):
                continue
            required = ("level", "breaking", "fingerprint", "revision", "title",
                        "description", "source_time", "eurl", "prefix", "notification_kind")
            if not all(key in raw_state for key in required):
                continue
            raw_revision = raw_state.get("revision", 1)
            try:
                revision = max(1, int(raw_revision))
                notification_revision = max(
                    0,
                    int(raw_state.get(
                        "notification_revision",
                        revision if raw_state.get("telegram_message_id") else 0,
                    )),
                )
                telegram_revision = max(
                    0,
                    int(raw_state.get(
                        "telegram_revision",
                        revision if raw_state.get("telegram_message_id") else 0,
                    )),
                )
            except (TypeError, ValueError):
                continue
            raw_message_ids = raw_state.get("telegram_message_ids", [])
            message_ids = (
                [message_id for message_id in raw_message_ids if isinstance(message_id, int)]
                if isinstance(raw_message_ids, list)
                else []
            )
            raw_primary_message_id = raw_state.get("telegram_message_id")
            primary_message_id = (
                raw_primary_message_id
                if isinstance(raw_primary_message_id, int)
                and not isinstance(raw_primary_message_id, bool)
                and raw_primary_message_id > 0
                else None
            )
            if raw_primary_message_id is not None and primary_message_id is None:
                logger.warning(f"Discard invalid primary Telegram message ID news_id={nid}")
                continue
            if (
                not isinstance(raw_message_ids, list)
                or len(raw_message_ids) > MAX_TELEGRAM_CHUNKS
                or any(
                    not isinstance(message_id, int)
                    or isinstance(message_id, bool)
                    or message_id <= 0
                    for message_id in raw_message_ids
                )
            ):
                logger.warning(f"Discard invalid Telegram message IDs news_id={nid}")
                continue
            message_ids = list(raw_message_ids)
            if primary_message_id is not None and message_ids and message_ids[0] != primary_message_id:
                logger.warning(f"Discard inconsistent primary Telegram message ID news_id={nid}")
                continue
            if primary_message_id is not None and not message_ids:
                message_ids = [primary_message_id]
            content = {
                "title": _safe_string(raw_state.get("title")),
                "description": _safe_string(raw_state.get("description")),
                "source_time": _safe_string(raw_state.get("source_time")),
                "eurl": _safe_string(raw_state.get("eurl")),
            }
            if _oversized_field(nid, content) is not None:
                logger.warning(f"Discard oversized processor state entry news_id={nid}")
                continue
            fingerprint = raw_state.get("fingerprint")
            if (
                not isinstance(fingerprint, str)
                or len(fingerprint) != 64
                or any(char not in "0123456789abcdef" for char in fingerprint.lower())
            ):
                logger.warning(f"Discard invalid processor fingerprint news_id={nid}")
                continue
            raw_pending = _validate_pending_archive(
                raw_state.get("raw_archive_pending", []), nid, "raw"
            )
            breaking_pending = _validate_pending_archive(
                raw_state.get("breaking_archive_pending", []), nid, "breaking"
            )
            if raw_pending is None or breaking_pending is None:
                continue
            pending_group_raw = raw_state.get("pending_group")
            pending_group = _validate_pending_group(pending_group_raw)
            repaired_pending_group = pending_group_raw is not None and pending_group is None
            if repaired_pending_group:
                logger.warning(f"Discard invalid pending Telegram group news_id={nid}")
                self._state_repaired = True
            level = _bounded_metadata(raw_state.get("level"), MAX_LEVEL_LENGTH)
            source_method = _bounded_metadata(
                raw_state.get("source_method"), MAX_SOURCE_METHOD_LENGTH
            )
            prefix = raw_state.get("prefix")
            notification_kind = raw_state.get("notification_kind")
            if (
                level is None
                or source_method is None
                or prefix not in ALLOWED_PREFIXES
                or notification_kind not in ALLOWED_NOTIFICATION_KINDS
            ):
                logger.warning(f"Discard invalid processor metadata news_id={nid}")
                continue
            candidate_state = {
                "level": level,
                "breaking": bool(raw_state.get("breaking")),
                "fingerprint": fingerprint,
                "revision": revision,
                **content,
                "telegram_message_id": primary_message_id,
                "prefix": prefix,
                "notification_kind": notification_kind,
                "source_method": source_method,
                "notification_revision": notification_revision,
                "telegram_revision": telegram_revision,
                "telegram_is_group": bool(raw_state.get("telegram_is_group", False)),
                "telegram_message_ids": message_ids,
                "alert_recorded": bool(
                    raw_state.get("alert_recorded", False)
                    or primary_message_id is not None
                    or notification_revision > 0
                    or _safe_string(raw_state.get("notification_kind")) == "legacy_breaking"
                ),
                "raw_archive_revision": _safe_nonnegative_int(
                    raw_state.get("raw_archive_revision", revision), revision
                ),
                "breaking_archive_revision": _safe_nonnegative_int(
                    raw_state.get(
                        "breaking_archive_revision",
                        revision if raw_state.get("alert_recorded") else 0,
                    )
                ),
                "raw_archive_pending": raw_pending,
                "breaking_archive_pending": breaking_pending,
                "pending_group": pending_group,
                "last_seen_seq": _safe_nonnegative_int(raw_state.get("last_seen_seq", 0)),
            }
            candidate_pending = [*raw_pending, *breaking_pending]
            candidate_pending_count = len(candidate_pending)
            candidate_pending_bytes = _serialized_bytes(candidate_pending)
            if (
                global_pending_count + candidate_pending_count
                > MAX_GLOBAL_PENDING_ARCHIVE_RECORDS
                or global_pending_bytes + candidate_pending_bytes
                > MAX_GLOBAL_PENDING_ARCHIVE_BYTES
            ):
                logger.warning(f"Discard state entry by global pending budget news_id={nid}")
                self._state_repaired = True
                continue
            prospective_entries = {**valid, nid: candidate_state}
            if _serialized_bytes({"version": STATE_VERSION, "entries": prospective_entries}) > MAX_RUNTIME_STATE_BYTES:
                logger.warning(f"Discard state entry by runtime state budget news_id={nid}")
                self._state_repaired = True
                continue
            valid[nid] = candidate_state
            global_pending_count += candidate_pending_count
            global_pending_bytes += candidate_pending_bytes
        return valid

    def _persist(self) -> None:
        payload = {
            "version": STATE_VERSION,
            "last_seen_seq": self._last_seen_seq,
            "entries": self._state,
        }
        size = _serialized_bytes(payload)
        if size > MAX_RUNTIME_STATE_BYTES:
            logger.error(
                f"Refuse oversized processor state persist: size={size} "
                f"limit={MAX_RUNTIME_STATE_BYTES}"
            )
            raise ValueError("processor state runtime budget exceeded")
        save_json_state(self._state_path, payload)

    def _state_fits(self, entries: Dict[str, Dict[str, Any]]) -> bool:
        payload = {
            "version": STATE_VERSION,
            "last_seen_seq": self._last_seen_seq + 1,
            "entries": entries,
        }
        return _serialized_bytes(payload) <= MAX_RUNTIME_STATE_BYTES

    def _has_unfinished_work(self, state: Dict[str, Any]) -> bool:
        return bool(
            state.get("raw_archive_pending")
            or state.get("breaking_archive_pending")
            or state.get("pending_group")
            or (
                state.get("alert_recorded")
                and int(state.get("notification_revision", 0)) < int(state.get("revision", 0))
            )
        )

    def _ensure_new_entry_capacity(self) -> bool:
        if len(self._state) < MAX_STATE_ENTRIES:
            return True
        removable = sorted(
            (
                (nid, state)
                for nid, state in self._state.items()
                if not self._has_unfinished_work(state)
            ),
            key=lambda entry: int(entry[1].get("last_seen_seq", 0)),
        )
        if not removable:
            return False
        target = min(TRIMMED_STATE_ENTRIES, MAX_STATE_ENTRIES - 1)
        remove_count = max(1, len(self._state) - target)
        for nid, _state in removable[:remove_count]:
            del self._state[nid]
        self._persist()
        return len(self._state) < MAX_STATE_ENTRIES

    def _global_pending_usage(self) -> tuple[int, int]:
        records: List[Dict[str, Any]] = []
        for state in self._state.values():
            for sink in ("raw", "breaking"):
                pending = state.get(f"{sink}_archive_pending", [])
                if isinstance(pending, list):
                    records.extend(record for record in pending if isinstance(record, dict))
        return len(records), _serialized_bytes(records)

    def _trim(self) -> None:
        if len(self._state) <= MAX_STATE_ENTRIES:
            return
        retained = sorted(
            self._state.items(),
            key=lambda entry: int(entry[1].get("last_seen_seq", 0)),
        )[-TRIMMED_STATE_ENTRIES:]
        self._state = dict(retained)
        self._persist()

    def _touch(self, state: Dict[str, Any]) -> None:
        self._last_seen_seq += 1
        state["last_seen_seq"] = self._last_seen_seq

    def _queue_archive(self, state: Dict[str, Any], sink: str, record: Dict[str, Any]) -> None:
        pending_key = f"{sink}_archive_pending"
        pending = state[pending_key]
        key = (record.get("revision"), record.get("fingerprint"))
        if not any((entry.get("revision"), entry.get("fingerprint")) == key for entry in pending):
            pending.append(record)

    def _archive_backpressure(
        self, state: Dict[str, Any], candidates: Dict[str, Dict[str, Any]]
    ) -> bool:
        global_count, global_bytes = self._global_pending_usage()
        for sink, candidate in candidates.items():
            pending = state.get(f"{sink}_archive_pending", [])
            if not isinstance(pending, list):
                return True
            candidate_pending = [*pending, candidate]
            if (
                len(candidate_pending) > MAX_PENDING_ARCHIVE_RECORDS
                or _serialized_bytes(candidate_pending) > MAX_PENDING_ARCHIVE_BYTES
            ):
                return True
            global_count += 1
            global_bytes += _serialized_bytes(candidate) + 1
        if (
            global_count > MAX_GLOBAL_PENDING_ARCHIVE_RECORDS
            or global_bytes > MAX_GLOBAL_PENDING_ARCHIVE_BYTES
        ):
            return True
        return False

    def _flush_archives(self, nid: str, state: Dict[str, Any]) -> None:
        for sink in ("raw", "breaking"):
            pending_key = f"{sink}_archive_pending"
            progress_key = f"{sink}_archive_revision"
            pending = state[pending_key]
            while pending:
                record = pending[0]
                revision = int(record.get("revision", 0))
                try:
                    if sink == "raw":
                        save_news_items_batch([record])
                    else:
                        save_breaking_item(record)
                except (OSError, ValueError, json.JSONDecodeError) as exc:
                    logger.warning(
                        f"Archive sink failed news_id={nid} sink={sink} "
                        f"revision={revision}: {type(exc).__name__}: {exc}"
                    )
                    break
                pending.pop(0)
                old_progress = state.get(progress_key, 0)
                state[progress_key] = max(int(old_progress), revision)
                try:
                    self._persist()
                except ValueError:
                    state[progress_key] = old_progress
                    pending.insert(0, record)
                    break

    async def _publish_group(
        self,
        nid: str,
        state: Dict[str, Any],
        revision: int,
        kind: str,
        chunks: List[str],
    ) -> Optional[List[int]]:
        digest = hashlib.sha256("\0".join(chunks).encode("utf-8")).hexdigest()
        pending = state.get("pending_group")
        if not isinstance(pending, dict) or any(
            pending.get(key) != value
            for key, value in (("revision", revision), ("kind", kind), ("digest", digest))
        ):
            candidate_pending = {
                "revision": revision,
                "kind": kind,
                "digest": digest,
                "chunks": chunks,
                "message_ids": [],
                "next_chunk_index": 0,
            }
            simulated = copy.deepcopy(self._state)
            simulated_state = simulated.get(nid)
            if simulated_state is None:
                return None
            budget_pending: Dict[str, Any] = copy.deepcopy(candidate_pending)
            budget_pending["message_ids"] = [9_999_999_999_999_999_999] * len(chunks)
            budget_pending["next_chunk_index"] = len(chunks)
            simulated_state["pending_group"] = budget_pending
            if not self._state_fits(simulated):
                logger.warning(
                    f"Reject Telegram group by runtime state budget news_id={nid} "
                    f"revision={revision} kind={kind}"
                )
                return None
            pending = candidate_pending
            state["pending_group"] = pending
            self._persist()

        async def record_progress(message_ids: List[int], next_chunk_index: int) -> bool:
            current = state.get("pending_group")
            if not isinstance(current, dict) or current.get("digest") != digest:
                return False
            durable_snapshot = copy.deepcopy(current)
            candidate = copy.deepcopy(current)
            candidate["message_ids"] = message_ids
            candidate["next_chunk_index"] = next_chunk_index
            state["pending_group"] = candidate
            try:
                self._persist()
            except (OSError, ValueError) as exc:
                state["pending_group"] = durable_snapshot
                logger.warning(
                    f"Telegram progress persist failed news_id={nid} revision={revision} "
                    f"kind={kind} chunk={next_chunk_index}: "
                    f"{type(exc).__name__}: {exc}"
                )
                return False
            return True

        pending_chunks_raw = pending.get("chunks", [])
        pending_chunks = (
            [chunk for chunk in pending_chunks_raw if isinstance(chunk, str)]
            if isinstance(pending_chunks_raw, list)
            else []
        )
        pending_ids_raw = pending.get("message_ids", [])
        pending_ids = (
            [value for value in pending_ids_raw if isinstance(value, int)]
            if isinstance(pending_ids_raw, list)
            else []
        )
        next_index_raw = pending.get("next_chunk_index", 0)
        next_index = next_index_raw if isinstance(next_index_raw, int) else 0
        try:
            result = await tg_send_group(
                pending_chunks,
                important=True,
                start_index=next_index,
                existing_message_ids=pending_ids,
                on_progress=record_progress,
            )
        except (TelegramRenderLimitError, ValueError) as exc:
            logger.warning(
                f"Telegram group rejected news_id={nid} revision={revision} kind={kind}: {exc}"
            )
            state["pending_group"] = None
            self._persist()
            return None
        if isinstance(result, list):
            if not result:
                return None
            completed_ids = result
        elif not result.complete:
            return None
        else:
            completed_ids = result.message_ids

        durable_complete_state = copy.deepcopy(state)
        candidate = copy.deepcopy(state)
        candidate["pending_group"] = None
        self._record_representation(candidate, revision, completed_ids)
        if kind in ("english_breaking", "english_upgrade", "english_update"):
            candidate["notification_revision"] = revision
        if kind == "english_update" or kind == "translation":
            candidate["prefix"] = "UPDATE\n"
            candidate["notification_kind"] = "update"
        state.clear()
        state.update(candidate)
        try:
            self._persist()
        except (OSError, ValueError) as exc:
            state.clear()
            state.update(durable_complete_state)
            logger.warning(
                f"Telegram group finalization persist failed news_id={nid} "
                f"revision={revision} kind={kind}: {type(exc).__name__}: {exc}"
            )
            return None
        return completed_ids

    async def _resume_pending_translation(
        self, nid: str, state: Dict[str, Any]
    ) -> bool:
        pending = state.get("pending_group")
        if not isinstance(pending, dict) or pending.get("kind") != "translation":
            return False
        revision = _safe_nonnegative_int(pending.get("revision"))
        if revision != state["revision"] or state["notification_revision"] != revision:
            state["pending_group"] = None
            self._persist()
            return False
        chunks_raw = pending.get("chunks", [])
        chunks = (
            [chunk for chunk in chunks_raw if isinstance(chunk, str)]
            if isinstance(chunks_raw, list)
            else []
        )
        if not chunks:
            state["pending_group"] = None
            self._persist()
            return False
        message_ids = await self._publish_group(
            nid, state, revision, "translation", chunks
        )
        if not message_ids:
            return True
        return True

    def _archive_record(self, nid: str, state: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "news_id": nid,
            "title": state["title"],
            "description": state["description"],
            "time": state["source_time"],
            "link": state["eurl"],
            "is_important": _is_breaking(state["level"], state["breaking"]),
            "level": state["level"],
            "breaking": state["breaking"],
            "fingerprint": state["fingerprint"],
            "revision": state["revision"],
            "source_method": state["source_method"],
        }

    def _enqueue_translation(self, nid: str, state: Dict[str, Any]) -> None:
        message_id = state.get("telegram_message_id")
        try:
            original = _translation_text(state)
        except TelegramRenderLimitError as exc:
            logger.warning(
                f"Skip translation normalization news_id={nid} revision={state['revision']}: {exc}"
            )
            return
        if not TRANSLATE_ENABLED or self._translator is None or not message_id or not original:
            return
        if len(original) > MAX_TRANSLATION_INPUT_LENGTH:
            logger.warning(
                f"Skip translation news_id={nid} revision={state['revision']} "
                f"input_length={len(original)} limit={MAX_TRANSLATION_INPUT_LENGTH}"
            )
            return
        self._translator.submit(
            TranslationJob(
                news_id=nid,
                revision=state["revision"],
                message_id=message_id,
                original_text=original,
                source_time=state["source_time"],
                prefix=state["prefix"],
                apply_translation=self.apply_translated_revision,
            )
        )

    async def _send_complete(
        self, nid: str, state: Dict[str, Any], revision: int, kind: str, text: str, label: str
    ) -> Optional[List[int]]:
        chunks = render_message_chunks(text, group_label=label)
        return await self._publish_group(nid, state, revision, kind, chunks)

    def _record_representation(
        self, state: Dict[str, Any], revision: int, message_ids: List[int]
    ) -> None:
        state["telegram_message_ids"] = message_ids
        state["telegram_message_id"] = message_ids[0]
        state["telegram_is_group"] = len(message_ids) > 1
        state["telegram_revision"] = revision
        state["alert_recorded"] = True

    async def apply_translated_revision(
        self, news_id: str, revision: int, message_id: int, translated_text: str
    ) -> bool:
        """Validate and publish a translated revision while publication is serialized."""
        async with self._publication_lock:
            state = self._state.get(_news_id(news_id))
            if (
                state is None
                or state["revision"] != revision
                or state["notification_revision"] != revision
                or state.get("telegram_message_id") != message_id
            ):
                return False
            pending = state.get("pending_group")
            if isinstance(pending, dict) and (
                pending.get("revision") != revision or pending.get("kind") != "translation"
            ):
                state["pending_group"] = None
                self._persist()

            try:
                chunks = render_message_chunks(translated_text, group_label="UPDATE")
            except TelegramRenderLimitError as exc:
                logger.warning(
                    f"Reject translated render news_id={news_id} revision={revision}: {exc}"
                )
                return False
            if (
                not state.get("telegram_is_group")
                and len(chunks) == 1
                and await tg_edit_message(message_id, chunks[0])
            ):
                state["telegram_revision"] = revision
                self._persist()
                return True

            try:
                replacement = render_message_chunks(
                    f"UPDATE\n{translated_text}", group_label="UPDATE"
                )
            except TelegramRenderLimitError as exc:
                logger.warning(
                    f"Reject translated replacement news_id={news_id} revision={revision}: {exc}"
                )
                return False
            message_ids = await self._publish_group(
                news_id, state, revision, "translation", replacement
            )
            if not message_ids:
                return False
            return True

    async def _notify_new_breaking(self, nid: str, state: Dict[str, Any], upgraded: bool) -> None:
        prefix = "🔺 UPGRADED\n" if upgraded else "🚨 BREAKING\n"
        state["prefix"] = prefix
        state["notification_kind"] = "upgrade" if upgraded else "breaking"
        try:
            text = _english_text(prefix, state)
            message_ids = await self._send_complete(
                nid,
                state,
                state["revision"],
                "english_upgrade" if upgraded else "english_breaking",
                text,
                "UPGRADED" if upgraded else "BREAKING",
            )
        except TelegramRenderLimitError as exc:
            logger.warning(
                f"Telegram render rejected news_id={nid} revision={state['revision']}: {exc}"
            )
            self._persist()
            return
        if not message_ids:
            return
        self._enqueue_translation(nid, state)

    async def _update_existing_message(self, nid: str, state: Dict[str, Any]) -> None:
        message_id = state.get("telegram_message_id")
        if not message_id:
            if state.get("alert_recorded"):
                try:
                    replacement_text = _english_text("UPDATE\n", state)
                    message_ids = await self._send_complete(
                        nid, state, state["revision"], "english_update", replacement_text, "UPDATE"
                    )
                except TelegramRenderLimitError as exc:
                    logger.warning(
                        f"Telegram legacy replacement rejected news_id={nid} "
                        f"revision={state['revision']}: {exc}"
                    )
                    return
                if message_ids:
                    self._enqueue_translation(nid, state)
            else:
                await self._notify_new_breaking(
                    nid, state, upgraded=state["notification_kind"] == "upgrade"
                )
            return
        try:
            text = _english_text(state["prefix"], state)
            chunks = render_message_chunks(text, group_label="UPDATE")
        except TelegramRenderLimitError as exc:
            logger.warning(
                f"Telegram render rejected news_id={nid} revision={state['revision']}: {exc}"
            )
            return
        if (
            not state.get("telegram_is_group")
            and len(chunks) == 1
            and await tg_edit_message(message_id, chunks[0])
        ):
            state["notification_revision"] = state["revision"]
            state["telegram_revision"] = state["revision"]
            self._persist()
            self._enqueue_translation(nid, state)
            return

        try:
            replacement_text = _english_text("UPDATE\n", state)
            message_ids = await self._send_complete(
                nid, state, state["revision"], "english_update", replacement_text, "UPDATE"
            )
        except TelegramRenderLimitError as exc:
            logger.warning(
                f"Telegram replacement rejected news_id={nid} revision={state['revision']}: {exc}"
            )
            return
        if message_ids:
            self._enqueue_translation(nid, state)

    async def process(self, items: List[Dict[str, Any]], source: str = "?") -> None:
        if len(items) > MAX_ITEMS_PER_PROCESS_BATCH:
            logger.warning(
                f"Reject oversized news batch count={len(items)} "
                f"limit={MAX_ITEMS_PER_PROCESS_BATCH} source={_safe_log_text(str(source), 64)}"
            )
            return
        bounded_source = _bounded_metadata(source, MAX_SOURCE_METHOD_LENGTH)
        if bounded_source is None:
            logger.warning(
                f"Reject news batch with oversized source length={len(str(source))} "
                f"limit={MAX_SOURCE_METHOD_LENGTH}"
            )
            return
        for item in items:
            async with self._publication_lock:
                await self._process_item(item, bounded_source)
        self._trim()

    async def _process_item(self, item: Dict[str, Any], source: str) -> None:
        nid = _news_id(item.get("NewsID"))
        if not nid:
            return
        old = self._state.get(nid)
        content = _normalized_content(item, old)
        oversized = _oversized_field(nid, content)
        if oversized is not None:
            field, length, limit = oversized
            logger.warning(
                f"Reject news item news_id={_safe_log_text(nid, 128)} "
                f"field={field} length={length} limit={limit}"
            )
            return
        fingerprint = _fingerprint(content)
        level = _bounded_metadata(item.get("Level"), MAX_LEVEL_LENGTH)
        if level is None:
            logger.warning(
                f"Reject news item news_id={_safe_log_text(nid, 128)} field=level "
                f"length={len(str(item.get('Level', '')))} limit={MAX_LEVEL_LENGTH}"
            )
            return
        breaking = bool(item.get("Breaking"))
        old_level = old["level"] if old is not None else ""
        old_breaking = old["breaking"] if old is not None else False
        old_source_time = old["source_time"] if old is not None else ""
        old_eurl = old["eurl"] if old is not None else ""
        old_source_method = old["source_method"] if old is not None else ""
        was_breaking = bool(old and _is_breaking(old["level"], old["breaking"]))
        breaking_now = _is_breaking(level, breaking)
        content_changed = old is None or old["fingerprint"] != fingerprint
        upgraded = old is not None and not was_breaking and breaking_now
        ws_source = item.get("__ws_method__") or item.get("__ws_channel__")
        ws_method = _bounded_metadata(ws_source, MAX_SOURCE_METHOD_LENGTH)
        if ws_method is None:
            logger.warning(
                f"Reject news item news_id={_safe_log_text(nid, 128)} field=source_method "
                f"length={len(str(ws_source or ''))} "
                f"limit={MAX_SOURCE_METHOD_LENGTH}"
            )
            return
        source_method = ws_method or source
        metadata_changed = bool(
            old is not None
            and (
                old_source_time != content["source_time"]
                or old_eurl != content["eurl"]
                or old_source_method != source_method
                or old_level != level
                or old_breaking != breaking
            )
        )

        if old is not None and content_changed:
            self._flush_archives(nid, old)

        next_revision = 1 if old is None else old["revision"] + (1 if content_changed else 0)
        prospective_state = {
            "level": level,
            "breaking": breaking,
            "fingerprint": fingerprint,
            "revision": next_revision,
            **content,
            "source_method": source_method,
        }
        archive_candidates: Dict[str, Dict[str, Any]] = {}
        if content_changed:
            archive_candidates["raw"] = self._archive_record(nid, prospective_state)
        if (old is None and breaking_now) or upgraded or (
            old is not None and content_changed and breaking_now
        ):
            breaking_record = {
                **self._archive_record(nid, prospective_state),
                "is_important": True,
            }
            if old is not None and content_changed:
                breaking_record["content_revision"] = True
            if upgraded:
                breaking_record["upgraded"] = True
            archive_candidates["breaking"] = breaking_record
        backlog_state = old or {
            "raw_archive_pending": [],
            "breaking_archive_pending": [],
        }
        if content_changed and self._archive_backpressure(backlog_state, archive_candidates):
            logger.warning(
                f"Reject editorial revision by archive backpressure news_id={nid} "
                f"current_revision={old['revision'] if old is not None else 0}"
            )
            return
        simulated: Dict[str, Dict[str, Any]] = copy.deepcopy(self._state)
        if content_changed or metadata_changed or old is None:
            simulated_state = simulated.get(nid)
            if simulated_state is None:
                simulated_state = {
                    "level": level, "breaking": breaking, "fingerprint": fingerprint,
                    "revision": 1, **content, "telegram_message_id": None, "prefix": "",
                    "notification_kind": "none", "source_method": source_method,
                    "notification_revision": 0, "telegram_revision": 0,
                    "telegram_is_group": False, "telegram_message_ids": [],
                    "alert_recorded": False, "raw_archive_revision": 0,
                    "breaking_archive_revision": 0, "raw_archive_pending": [],
                    "breaking_archive_pending": [], "pending_group": None,
                    "last_seen_seq": self._last_seen_seq + 1,
                }
                simulated[nid] = simulated_state
            else:
                simulated_state.update(prospective_state)
            for sink, candidate in archive_candidates.items():
                pending_key = f"{sink}_archive_pending"
                simulated_pending = simulated_state.get(pending_key)
                if not isinstance(simulated_pending, list):
                    simulated_pending = []
                    simulated_state[pending_key] = simulated_pending
                simulated_pending.append(candidate)
            if not self._state_fits(simulated):
                logger.warning(
                    f"Reject editorial revision by runtime state budget news_id={nid}"
                )
                return
        if old is None and not self._ensure_new_entry_capacity():
            logger.warning(f"Reject new NewsID by state capacity news_id={_safe_log_text(nid, 128)}")
            return

        previous_seq = self._last_seen_seq
        previous_state = copy.deepcopy(old) if old is not None else None
        if content_changed:
            state = copy.deepcopy(simulated[nid])
            pending_group = state.get("pending_group")
            if (
                isinstance(pending_group, dict)
                and pending_group.get("revision") != state["revision"]
            ):
                state["pending_group"] = None
            if upgraded:
                state["prefix"] = "🔺 UPGRADED\n"
                state["notification_kind"] = "upgrade"
            self._last_seen_seq += 1
            state["last_seen_seq"] = self._last_seen_seq
            self._state[nid] = state
            try:
                # Editorial state and every required archive retry record become
                # durable in this single atomic processor-state replacement.
                self._persist()
            except (OSError, ValueError) as exc:
                if previous_state is None:
                    self._state.pop(nid, None)
                else:
                    self._state[nid] = previous_state
                self._last_seen_seq = previous_seq
                logger.warning(
                    f"Reject editorial revision after state persist failure news_id={nid} "
                    f"revision={next_revision}: {type(exc).__name__}: {exc}"
                )
                return
        else:
            if old is None:
                return
            state = old
            snapshot = copy.deepcopy(state)
            state["level"] = level
            state["breaking"] = breaking
            state["source_method"] = source_method
            state["source_time"] = content["source_time"]
            state["eurl"] = content["eurl"]
            if upgraded:
                state["prefix"] = "🔺 UPGRADED\n"
                state["notification_kind"] = "upgrade"
                breaking_candidate = archive_candidates.get("breaking")
                if breaking_candidate is not None:
                    self._queue_archive(state, "breaking", breaking_candidate)
            self._touch(state)
            if metadata_changed or upgraded:
                try:
                    self._persist()
                except (OSError, ValueError) as exc:
                    self._state[nid] = snapshot
                    self._last_seen_seq = previous_seq
                    logger.warning(
                        f"Reject state update after persist failure news_id={nid}: "
                        f"{type(exc).__name__}: {exc}"
                    )
                    return

        alert_eligible = breaking_now or bool(state.get("alert_recorded"))
        pending_notification = alert_eligible and state["notification_revision"] < state["revision"]
        archive_pending = bool(
            state.get("raw_archive_pending") or state.get("breaking_archive_pending")
        )
        current_pending_group = state.get("pending_group")
        pending_translation = bool(
            isinstance(current_pending_group, dict)
            and current_pending_group.get("kind") == "translation"
        )
        if not content_changed and not upgraded and not pending_notification and not archive_pending:
            if pending_translation:
                await self._resume_pending_translation(nid, state)
            return

        if old is None and breaking_now:
            logger.warning(
                f"🚨 [{source}] BREAKING news_id={nid}: {_safe_log_text(state['title'])}"
            )
            self._flush_archives(nid, state)
            await self._notify_new_breaking(nid, state, upgraded=False)
        elif upgraded:
            logger.warning(
                f"🔺 [{source}] UPGRADED TO BREAKING news_id={nid}: "
                f"{_safe_log_text(state['title'])}"
            )
            self._flush_archives(nid, state)
            await self._notify_new_breaking(nid, state, upgraded=True)
        elif pending_notification:
            self._flush_archives(nid, state)
            await self._update_existing_message(nid, state)
        elif old is None:
            self._flush_archives(nid, state)
            logger.info(
                f"📰 [{source}] news_id={nid}: {_safe_log_text(state['title'])}"
            )
        else:
            self._flush_archives(nid, state)
