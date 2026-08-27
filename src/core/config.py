import json
import math
import os
from typing import cast

from dotenv import load_dotenv

_ = load_dotenv()

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")


def _optional_positive_int(name: str) -> int | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


TG_THREAD_ID = _optional_positive_int("TG_THREAD_ID")

FJ_EMAIL = os.environ.get("FJ_EMAIL", "")
FJ_PASSWORD = os.environ.get("FJ_PASSWORD", "")

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data"
)
COOKIES_PATH = os.environ.get("FJ_COOKIES_PATH", os.path.join(DATA_DIR, "cookies.json"))

WS_RECONNECT_BASE_DELAY = float(os.environ.get("FJ_WS_RECONNECT_BASE_DELAY", "3"))
WS_RECONNECT_MAX_DELAY = float(os.environ.get("FJ_WS_RECONNECT_MAX_DELAY", "60"))

POLL_FALLBACK_INTERVAL = float(os.environ.get("FJ_POLL_FALLBACK_INTERVAL", "15"))

FEEDTOKEN_REFRESH_HOURS = float(os.environ.get("FJ_FEEDTOKEN_REFRESH_HOURS", "6"))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float, *, minimum: float | None = None) -> float:
    raw = os.environ.get(name)
    try:
        value = float(raw) if raw is not None else default
    except (TypeError, ValueError):
        return default
    if not math.isfinite(value) or (minimum is not None and value < minimum):
        return default
    return value


def _env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = os.environ.get(name)
    try:
        value = int(raw) if raw is not None else default
    except (TypeError, ValueError):
        return default
    if minimum is not None and value < minimum:
        return default
    return value


WS_RECEIVE_TIMEOUT = _env_float("FJ_WS_RECEIVE_TIMEOUT", 180.0, minimum=1.0)


def _env_json_object(name: str) -> dict[str, object]:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(value, dict):
        return {}
    return cast(dict[str, object], value)


def _env_string_headers(name: str) -> dict[str, str]:
    value = _env_json_object(name)
    if any(not isinstance(item, str) for item in value.values()):
        return {}
    return {key: cast(str, item) for key, item in value.items()}


TRANSLATE_ENABLED = _env_bool("FJ_TRANSLATE_ENABLED", False)
TRANSLATE_TIMEOUT = _env_float("FJ_TRANSLATE_TIMEOUT", 60.0, minimum=0.1)
TRANSLATE_MAX_TOKENS = _env_int("FJ_TRANSLATE_MAX_TOKENS", 256, minimum=1)
TRANSLATE_TEMPERATURE = _env_float("FJ_TRANSLATE_TEMPERATURE", 0.3, minimum=0.0)

# KIMI_* variables are deprecated compatibility aliases. FJ_TRANSLATE_* wins.
KIMI_API_KEY = os.environ.get("KIMI_API_KEY", "")
KIMI_BASE_URL = os.environ.get("KIMI_BASE_URL", "")
KIMI_MODEL = os.environ.get("KIMI_MODEL", "")
TRANSLATE_API_KEY = os.environ.get("FJ_TRANSLATE_API_KEY", KIMI_API_KEY).strip()
TRANSLATE_BASE_URL = os.environ.get(
    "FJ_TRANSLATE_BASE_URL", KIMI_BASE_URL or "https://api.moonshot.cn/v1"
).strip() or "https://api.moonshot.cn/v1"
TRANSLATE_MODEL = os.environ.get(
    "FJ_TRANSLATE_MODEL", KIMI_MODEL or "moonshot-v1-8k"
).strip() or "moonshot-v1-8k"
TRANSLATE_HEADERS = _env_string_headers("FJ_TRANSLATE_HEADERS_JSON")
TRANSLATE_EXTRA_BODY = _env_json_object("FJ_TRANSLATE_EXTRA_BODY_JSON")

TRANSLATE_PROMPT_FILE = os.environ.get(
    "FJ_TRANSLATE_PROMPT_FILE",
    os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "translate",
        "prompts",
        "news_zh.md",
    ),
)


def _load_prompt_file() -> str:
    try:
        with open(TRANSLATE_PROMPT_FILE, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return (
            "把下面的金融/财经新闻标题翻译成简体中文，保留所有数字、百分比、"
            "机构名、人名和专有名词。只输出译文一行，不要任何解释、引号或前后缀。"
        )


TRANSLATE_SYSTEM_PROMPT = _load_prompt_file()
