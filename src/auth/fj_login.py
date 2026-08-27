import asyncio
import json
import os
import re
from datetime import datetime, timezone
from html import unescape
from typing import Dict, List, Optional

import httpx

from src.utils.LoggerManager import logger

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)

LOGIN_URL = "https://www.financialjuice.com/home"

_HIDDEN_INPUT_RE = re.compile(
    r'<input\b[^>]*\btype=["\']hidden["\'][^>]*>',
    re.IGNORECASE,
)
_NAME_ATTR_RE = re.compile(r'\bname=["\']([^"\']+)["\']')
_VALUE_ATTR_RE = re.compile(r'\bvalue=["\']([^"\']*)["\']')


def _extract_hidden_inputs(html: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for m in _HIDDEN_INPUT_RE.finditer(html):
        tag = m.group(0)
        name_m = _NAME_ATTR_RE.search(tag)
        if not name_m:
            continue
        value_m = _VALUE_ATTR_RE.search(tag)
        out[name_m.group(1)] = unescape(value_m.group(1)) if value_m else ""
    return out


def cookies_to_header(cookies: List[Dict]) -> str:
    return "; ".join(
        f"{c['name']}={c['value']}"
        for c in cookies
        if "financialjuice.com" in c.get("domain", "")
    )


def cookies_to_dict(cookies: List[Dict]) -> Dict[str, str]:
    return {
        c["name"]: c["value"]
        for c in cookies
        if "financialjuice.com" in c.get("domain", "")
    }


def has_aspxauth(cookies: List[Dict]) -> bool:
    return any(c["name"] == ".ASPXAUTH" for c in cookies)


def save_cookies(cookies: List[Dict], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "cookies": cookies,
    }
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)
    logger.info(f"💾 Saved {len(cookies)} cookies to {path}")


def load_cookies(path: str) -> Optional[List[Dict]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        cookies = payload.get("cookies", [])
        if not has_aspxauth(cookies):
            logger.warning("Stored cookies missing .ASPXAUTH, ignoring")
            return None
        return cookies
    except Exception as e:
        logger.warning(f"Failed to load cookies: {e}")
        return None


def _jar_to_list(jar) -> List[Dict]:
    out: List[Dict] = []
    for cookie in jar:
        out.append({
            "name": cookie.name,
            "value": cookie.value or "",
            "domain": cookie.domain or "",
            "path": cookie.path or "/",
        })
    return out


async def http_login(email: str, password: str) -> List[Dict]:
    if not email or not password:
        raise ValueError("FJ_EMAIL and FJ_PASSWORD must be set")

    logger.info("🔐 Starting HTTP login flow")

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        timeout=30,
        follow_redirects=False,
    ) as client:
        r = await client.get(LOGIN_URL)
        if r.status_code != 200:
            raise RuntimeError(f"GET /home returned {r.status_code}")

        hidden = _extract_hidden_inputs(r.text)
        if "__VIEWSTATE" not in hidden:
            raise RuntimeError("Login page missing __VIEWSTATE - form structure changed")

        form_data = {
            "ctl00$ScriptManager1": (
                "ctl00$SignInSignUp$loginForm1$UpdatePanel1|"
                "ctl00$SignInSignUp$loginForm1$btnLogin"
            ),
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": hidden["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
            "ctl00$header1$siteMode": "2",
            "ctl00$ContentPlaceHolder1$contentToggle": "3",
            "ctl00$forgotpasswordform$inputEmail": "",
            "ctl00$SignInSignUp$loginForm1$inputEmail": email,
            "ctl00$SignInSignUp$loginForm1$inputPassword": password,
            "ctl00$SignInSignUp$Signup$inputUsername": "",
            "ctl00$SignInSignUp$Signup$inputEmail": "",
            "ctl00$SignInSignUp$Signup$inputPassword": "",
            "g-recaptcha-response": "",
            "ctl00$SignInSignUp$FacebookLogin1$fbtoken": "",
            "ctl00$SignInSignUp$GooglePlusLogin$googlePlusName": "",
            "ctl00$SignInSignUp$GooglePlusLogin$googlePlusLastName": "",
            "ctl00$SignInSignUp$GooglePlusLogin$googlePlusID": "",
            "ctl00$SignInSignUp$GooglePlusLogin$googlePlusEmail": "",
            "__ASYNCPOST": "true",
            "ctl00$SignInSignUp$loginForm1$btnLogin": "Login",
        }

        post_headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": LOGIN_URL,
            "Origin": "https://www.financialjuice.com",
            "X-Requested-With": "XMLHttpRequest",
            "X-MicrosoftAjax": "Delta=true",
            "Cache-Control": "no-cache",
        }

        r2 = await client.post(LOGIN_URL, data=form_data, headers=post_headers)
        if r2.status_code != 200:
            raise RuntimeError(f"Login POST returned {r2.status_code}")

        if ".ASPXAUTH" not in client.cookies:
            body = (r2.text or "")[:300]
            raise RuntimeError(
                f"Login POST returned 200 but .ASPXAUTH cookie missing. "
                f"Likely wrong credentials. Body: {body!r}"
            )

        body = r2.text or ""
        if "pageRedirect" not in body and "error" in body.lower():
            logger.warning(f"Server response unusual: {body[:200]}")

        cookies = _jar_to_list(client.cookies.jar)
        logger.info(f"✅ Logged in via HTTP, {len(cookies)} cookies in jar")
        return cookies


async def login_with_retry(
    email: str,
    password: str,
    max_attempts: int = 3,
) -> List[Dict]:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await http_login(email, password)
        except Exception as e:
            last_err = e
            logger.error(f"Login attempt {attempt}/{max_attempts} failed: {type(e).__name__}: {e}")
            if attempt < max_attempts:
                await asyncio.sleep(5 * attempt)
    raise RuntimeError(f"Login failed after {max_attempts} attempts: {last_err}")
