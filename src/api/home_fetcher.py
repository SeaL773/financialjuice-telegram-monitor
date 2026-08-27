import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from src.utils.LoggerManager import logger

HOME_URL = "https://www.financialjuice.com/home"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

_FEEDTOKEN_RE = re.compile(r"var feedtoken\s*=\s*'([^']+)'")
_CENTRIFUGO_TOKEN_RE = re.compile(r"var centrifugoToken\s*=\s*'([^']+)'")
_CENTRIFUGO_URL_RE = re.compile(r"var centrifugoUrl\s*=\s*'([^']+)'")
_INFO_RE = re.compile(r"var info\s*=\s*'([^']+)'")
_LOGGED_RE = re.compile(r"var LoggedUser\s*=\s*(true|false)")
_USERID_RE = re.compile(r"var LoggedUserID\s*=\s*(\d+)")


@dataclass
class HomeState:
    logged_in: bool
    user_id: int
    feedtoken: str
    centrifugo_token: str
    centrifugo_url: str
    info: str

    def feedtoken_expires_at(self) -> Optional[datetime]:
        try:
            import base64
            import json

            payload_b64 = self.feedtoken.split(".")[1]
            payload_b64 += "=" * (-len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            exp_str = payload.get("expires")
            if not exp_str:
                return None
            return datetime.fromisoformat(exp_str.replace("Z", "+00:00"))
        except Exception:
            return None

    def feedtoken_expired(self, slack_seconds: int = 600) -> bool:
        exp = self.feedtoken_expires_at()
        if exp is None:
            return False
        return (exp - datetime.now(timezone.utc)).total_seconds() < slack_seconds

    def centrifugo_token_expires_at(self) -> Optional[datetime]:
        try:
            import base64
            import json

            payload_b64 = self.centrifugo_token.split(".")[1]
            payload_b64 += "=" * (-len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            exp = payload.get("exp")
            if exp is None:
                return None
            return datetime.fromtimestamp(int(exp), tz=timezone.utc)
        except Exception:
            return None

    def centrifugo_token_expired(self, slack_seconds: int = 600) -> bool:
        exp = self.centrifugo_token_expires_at()
        if exp is None:
            return False
        return (exp - datetime.now(timezone.utc)).total_seconds() < slack_seconds


async def fetch_home_state(
    session: aiohttp.ClientSession,
    cookies: dict[str, str],
) -> HomeState:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with session.get(
        HOME_URL,
        cookies=cookies,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=20),
    ) as r:
        if r.status != 200:
            raise RuntimeError(f"GET /home returned {r.status}")
        html = await r.text()

    logged_match = _LOGGED_RE.search(html)
    feedtoken_match = _FEEDTOKEN_RE.search(html)
    centrifugo_token_match = _CENTRIFUGO_TOKEN_RE.search(html)
    centrifugo_url_match = _CENTRIFUGO_URL_RE.search(html)
    info_match = _INFO_RE.search(html)
    userid_match = _USERID_RE.search(html)

    logged_in = bool(logged_match and logged_match.group(1) == "true")
    feedtoken = feedtoken_match.group(1) if feedtoken_match else ""
    centrifugo_token = centrifugo_token_match.group(1) if centrifugo_token_match else ""
    centrifugo_url = centrifugo_url_match.group(1) if centrifugo_url_match else ""
    info = info_match.group(1) if info_match else ""
    user_id = int(userid_match.group(1)) if userid_match else 0

    if logged_in and not feedtoken:
        logger.warning("Page reports LoggedUser=true but feedtoken is empty")

    return HomeState(
        logged_in=logged_in,
        user_id=user_id,
        feedtoken=feedtoken,
        centrifugo_token=centrifugo_token,
        centrifugo_url=centrifugo_url,
        info=info,
    )


def cookies_list_to_dict(cookies: list[dict[str, object]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for c in cookies:
        name = c.get("name")
        value = c.get("value")
        domain = c.get("domain")
        if not isinstance(name, str) or not isinstance(value, str) or not isinstance(domain, str):
            continue
        if "financialjuice.com" in domain:
            out[name] = value
    return out
