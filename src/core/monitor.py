import asyncio
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pytz

import aiohttp
import httpx

from src.api.home_fetcher import HomeState, cookies_list_to_dict, fetch_home_state
from src.api.startup_client import USER_AGENT, fetch_startup
from src.api.centrifugo_client import CentrifugoClient
from src.api.ws_client import FJSignalRClient, WSAuthError, WSConnectionError
from src.archive.storage import get_et_date
from src.auth.fj_login import load_cookies, login_with_retry, save_cookies
from src.core.config import (
    COOKIES_PATH,
    FEEDTOKEN_REFRESH_HOURS,
    FJ_EMAIL,
    FJ_PASSWORD,
    POLL_FALLBACK_INTERVAL,
    TRANSLATE_ENABLED,
    WS_RECONNECT_BASE_DELAY,
    WS_RECONNECT_MAX_DELAY,
)
from src.core.news_processor import NewsProcessor
from src.translate.queue_worker import TranslationQueueWorker
from src.utils.LoggerManager import logger


_HEARTBEAT = Path('/tmp/healthcheck')
_WATCHDOG_TIMEOUT = 300
_WATCHDOG_TIMEOUT_WEEKEND = 14400
_ET = pytz.timezone("America/New_York")


def _watchdog():
    while True:
        time.sleep(60)
        try:
            is_weekend = datetime.now(_ET).weekday() >= 5
            timeout = _WATCHDOG_TIMEOUT_WEEKEND if is_weekend else _WATCHDOG_TIMEOUT
            if not _HEARTBEAT.exists():
                logger.critical(f"Watchdog: heartbeat file missing, forcing exit")
                os._exit(1)
            if time.time() - _HEARTBEAT.stat().st_mtime > timeout:
                logger.critical(f"Watchdog: no heartbeat for >{timeout}s, forcing exit")
                os._exit(1)
        except Exception:
            pass


class FJMonitor:
    def __init__(self):
        self.translator = TranslationQueueWorker() if TRANSLATE_ENABLED else None
        self.processor = NewsProcessor(translation_worker=self.translator)
        self.cookies_dict: dict[str, str] = {}
        self.home_state: Optional[HomeState] = None
        self._ws_failures = 0
        self._polling_stop = asyncio.Event()
        self._polling_task: Optional[asyncio.Task[None]] = None

    async def _ensure_logged_in(self) -> None:
        cookies = load_cookies(COOKIES_PATH)
        if cookies is not None:
            logger.info("📂 Loaded cookies from disk, validating against /home")
            self.cookies_dict = cookies_list_to_dict(cookies)
            try:
                async with aiohttp.ClientSession() as session:
                    state = await fetch_home_state(session, self.cookies_dict)
                if state.logged_in:
                    self.home_state = state
                    logger.info(f"✅ Cached cookies still valid (user_id={state.user_id})")
                    return
                logger.warning("Cached cookies expired or invalid (LoggedUser=false)")
            except Exception as e:
                logger.warning(f"Failed to validate cookies: {e}")

        logger.info("🔐 Logging in via HTTP")
        cookies = await login_with_retry(FJ_EMAIL, FJ_PASSWORD)
        save_cookies(cookies, COOKIES_PATH)
        self.cookies_dict = cookies_list_to_dict(cookies)
        async with aiohttp.ClientSession() as session:
            self.home_state = await fetch_home_state(session, self.cookies_dict)
        if not self.home_state.logged_in:
            raise RuntimeError("Login succeeded but /home still reports LoggedUser=false")

    async def _refresh_feedtoken(self) -> None:
        async with aiohttp.ClientSession() as session:
            self.home_state = await fetch_home_state(session, self.cookies_dict)
        if not self.home_state.logged_in:
            raise WSAuthError("Session expired (LoggedUser=false)")
        if self.home_state.centrifugo_token and self.home_state.centrifugo_url:
            logger.info("🔄 Refreshed Centrifugo token from /home")
        else:
            logger.info("🔄 Refreshed feedtoken from /home")

    async def _refresh_ws_auth_if_needed(self) -> None:
        if self.home_state is None:
            raise RuntimeError("Auth refresh attempted before login")

        if self.home_state.centrifugo_token and self.home_state.centrifugo_url:
            if self.home_state.centrifugo_token_expired(slack_seconds=600):
                await self._refresh_feedtoken()
            return

        if not self.home_state.feedtoken:
            raise WSConnectionError("feedtoken is empty, falling back to polling")

        if self.home_state.feedtoken_expired(slack_seconds=600):
            await self._refresh_feedtoken()

    async def _ensure_polling_running(self) -> None:
        if self._polling_task is None or self._polling_task.done():
            self._polling_stop.clear()
            self._polling_task = asyncio.create_task(self._polling_loop())
            logger.warning("⏬ Polling fallback started")

    async def _stop_polling(self) -> None:
        if self._polling_task is not None and not self._polling_task.done():
            self._polling_stop.set()
            try:
                await asyncio.wait_for(self._polling_task, timeout=5)
            except (asyncio.TimeoutError, Exception):
                pass
            logger.info("⏬ Polling fallback stopped")
        self._polling_task = None

    async def _ws_session(self) -> None:
        if self.home_state is None:
            raise RuntimeError("WS session attempted before login")

        await self._refresh_ws_auth_if_needed()

        use_centrifugo = bool(
            self.home_state.centrifugo_token and self.home_state.centrifugo_url
        )

        async with aiohttp.ClientSession() as session:
            if use_centrifugo:
                client = CentrifugoClient(
                    ws_url=self.home_state.centrifugo_url,
                    token=self.home_state.centrifugo_token,
                    session=session,
                )
            else:
                client = FJSignalRClient(
                    cookies=self.cookies_dict,
                    feedtoken=self.home_state.feedtoken,
                    session=session,
                )
            try:
                await client.connect()
                await self._stop_polling()
                self._ws_failures = 0

                last_refresh = time.time()
                refresh_interval = FEEDTOKEN_REFRESH_HOURS * 3600

                async def _ws_keepalive():
                    while True:
                        await asyncio.sleep(60)
                        _HEARTBEAT.write_text('')
                        try:
                            await self._refresh_ws_auth_if_needed()
                        except Exception as e:
                            logger.warning(f"Periodic WS token refresh failed: {e}")

                keepalive_task = asyncio.create_task(_ws_keepalive())
                try:
                    async for items in client.listen():
                        await self.processor.process(items, source="WS")
                        _HEARTBEAT.write_text('')
                        if not use_centrifugo and time.time() - last_refresh > refresh_interval:
                            try:
                                await self._refresh_feedtoken()
                                last_refresh = time.time()
                            except Exception as e:
                                logger.warning(f"Periodic feedtoken refresh failed: {e}")
                finally:
                    keepalive_task.cancel()
                    try:
                        await keepalive_task
                    except asyncio.CancelledError:
                        pass
            finally:
                await client.close()

    async def _polling_loop(self) -> None:
        if self.home_state is None or not self.home_state.info:
            logger.error("Polling fallback cannot start: no info token")
            return

        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            timeout=20,
            follow_redirects=True,
        ) as client:
            while not self._polling_stop.is_set():
                try:
                    items = await fetch_startup(
                        client,
                        cookies=self.cookies_dict,
                        info=self.home_state.info,
                    )
                    if items:
                        await self.processor.process(items, source="POLL")
                        _HEARTBEAT.write_text('')
                except Exception as e:
                    logger.error(f"Polling error: {type(e).__name__}: {e}")

                try:
                    await asyncio.wait_for(
                        self._polling_stop.wait(),
                        timeout=POLL_FALLBACK_INTERVAL,
                    )
                except asyncio.TimeoutError:
                    pass

    def _backoff_delay(self) -> float:
        return min(
            WS_RECONNECT_BASE_DELAY * (2 ** min(self._ws_failures, 5)),
            WS_RECONNECT_MAX_DELAY,
        )

    async def _handle_auth_error(self, err: Exception) -> None:
        logger.error(f"Auth error: {err} - re-logging in")
        try:
            cookies = await login_with_retry(FJ_EMAIL, FJ_PASSWORD)
            save_cookies(cookies, COOKIES_PATH)
            self.cookies_dict = cookies_list_to_dict(cookies)
            async with aiohttp.ClientSession() as session:
                self.home_state = await fetch_home_state(session, self.cookies_dict)
            self._ws_failures = 0
        except Exception as login_err:
            logger.error(f"Re-login failed: {login_err}")
            self._ws_failures += 1

    async def run(self) -> None:
        logger.info("🚀 Starting FinancialJuice Monitor (WS-primary, polling-fallback)")
        logger.info(f"📅 Date: {get_et_date()}")

        await self._ensure_logged_in()

        if self.translator is not None:
            await self.translator.start()

        _HEARTBEAT.write_text('')
        threading.Thread(target=_watchdog, daemon=True).start()
        try:
            while True:
                try:
                    await self._ws_session()
                    self._ws_failures += 1
                    logger.warning(f"WS listen() returned without error #{self._ws_failures} - reconnecting")
                except WSAuthError as e:
                    await self._handle_auth_error(e)
                except WSConnectionError as e:
                    self._ws_failures += 1
                    logger.warning(f"WS connection error #{self._ws_failures}: {e}")
                except Exception as e:
                    self._ws_failures += 1
                    logger.error(f"WS unexpected error #{self._ws_failures}: {type(e).__name__}: {e}")

                if self._ws_failures >= 2:
                    await self._ensure_polling_running()

                delay = self._backoff_delay()
                logger.info(f"⏳ Reconnecting WS in {delay:.0f}s")
                await asyncio.sleep(delay)
        finally:
            await self._stop_polling()
            if self.translator is not None:
                await self.translator.stop()


async def main():
    monitor = FJMonitor()
    await monitor.run()
