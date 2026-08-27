import json
from typing import Any, Dict, List

from src.core.security_limits import (
    MAX_ITEMS_PER_PROCESS_BATCH,
    MAX_WS_INVOCATIONS,
    MAX_WS_ITEM_SERIALIZED_BYTES,
    MAX_WS_NESTED_PAYLOAD_BYTES,
    MAX_WS_TEXT_FRAME_BYTES,
)
from src.utils.LoggerManager import logger


class WSFrameServerError(ValueError):
    pass


def parse_signalr_frame(text: str) -> List[Dict[str, Any]]:
    if not text or text == "{}":
        return []
    frame_bytes = len(text.encode("utf-8"))
    if frame_bytes > MAX_WS_TEXT_FRAME_BYTES:
        logger.warning(
            f"Reject oversized WS text frame bytes={frame_bytes} limit={MAX_WS_TEXT_FRAME_BYTES}"
        )
        return []
    try:
        envelope = json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Reject non-JSON WS frame")
        return []
    if not isinstance(envelope, dict):
        return []
    if envelope.get("E"):
        raise WSFrameServerError(str(envelope.get("E", ""))[:200])
    if envelope.get("S") == 1:
        return []
    invocations = envelope.get("M") or []
    if not isinstance(invocations, list) or len(invocations) > MAX_WS_INVOCATIONS:
        logger.warning(
            f"Reject WS invocation count={len(invocations) if isinstance(invocations, list) else -1} "
            f"limit={MAX_WS_INVOCATIONS}"
        )
        return []
    out: List[Dict[str, Any]] = []
    for invocation in invocations:
        if not isinstance(invocation, dict):
            continue
        method = invocation.get("M") or ""
        args = invocation.get("A") or []
        if method not in ("sendUpdates", "sendHeadlineUpdated") or not isinstance(args, list) or not args:
            continue
        payload = args[0]
        if isinstance(payload, str):
            payload_bytes = len(payload.encode("utf-8"))
            if payload_bytes > MAX_WS_NESTED_PAYLOAD_BYTES:
                logger.warning(
                    f"Reject oversized WS nested payload method={method} bytes={payload_bytes} "
                    f"limit={MAX_WS_NESTED_PAYLOAD_BYTES}"
                )
                return []
        try:
            items = json.loads(payload) if isinstance(payload, str) else payload
        except json.JSONDecodeError:
            logger.warning(f"Reject invalid nested WS JSON method={method}")
            return []
        records = items if isinstance(items, list) else [items] if isinstance(items, dict) else []
        if len(records) > MAX_ITEMS_PER_PROCESS_BATCH or len(out) + len(records) > MAX_ITEMS_PER_PROCESS_BATCH:
            logger.warning("Reject WS aggregate item count over batch limit")
            return []
        for record in records:
            if not isinstance(record, dict):
                continue
            record_bytes = len(
                json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            )
            if record_bytes > MAX_WS_ITEM_SERIALIZED_BYTES:
                logger.warning("Reject oversized WS item")
                return []
            normalized = dict(record)
            normalized["__ws_method__"] = method
            out.append(normalized)
    return out
