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
from src.api.startup_client import (
    USER_AGENT,
    StartupAuthError,
    StartupProtocolError,
    StartupTransportError,
    fetch_startup,
)
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
    WS_RECEIVE_TIMEOUT,
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
        self._polling_lock = asyncio.Lock()
        self._auth_lock = asyncio.Lock()
        self._auth_generation = 0
        self._auth_failures = 0
        self._auth_retry_after = 0.0

    @staticmethod
    def _record_transport_activity() -> None:
        _HEARTBEAT.write_text('')

    def _record_ws_activity(self) -> None:
        self._ws_failures = 0
        self._record_transport_activity()

    @staticmethod
    def _monotonic() -> float:
        return time.monotonic()

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

    async def _refresh_feedtoken(self) -> bool:
        async with self._auth_lock:
            observed_generation = self._auth_generation
            cookies_snapshot = dict(self.cookies_dict)

        async with aiohttp.ClientSession() as session:
            fetched_state = await fetch_home_state(session, cookies_snapshot)

        async with self._auth_lock:
            credentials_changed = (
                observed_generation != self._auth_generation
                or cookies_snapshot != self.cookies_dict
            )
            if credentials_changed:
                logger.info("Discarded stale WS token refresh after credentials changed")
                return True
            if not fetched_state.logged_in:
                raise WSAuthError("Session expired (LoggedUser=false)")
            self.home_state = fetched_state

        if fetched_state.centrifugo_token and fetched_state.centrifugo_url:
            logger.info("🔄 Refreshed Centrifugo token from /home")
        else:
            logger.info("🔄 Refreshed feedtoken from /home")
        return True

    async def _refresh_ws_auth_if_needed(self) -> bool:
        if self.home_state is None:
            raise RuntimeError("Auth refresh attempted before login")

        if self.home_state.centrifugo_token and self.home_state.centrifugo_url:
            if self.home_state.centrifugo_token_expired(slack_seconds=600):
                return await self._refresh_feedtoken()
            return False

        if not self.home_state.feedtoken:
            raise WSConnectionError("feedtoken is empty, falling back to polling")

        if self.home_state.feedtoken_expired(slack_seconds=600):
            return await self._refresh_feedtoken()
        return False

    async def _ensure_polling_running(self) -> None:
        async with self._polling_lock:
            if self._polling_task is None or self._polling_task.done():
                self._polling_stop.clear()
                self._polling_task = asyncio.create_task(self._polling_loop())
                logger.warning("⏬ Polling fallback started")

    async def _stop_polling(self) -> None:
        async with self._polling_lock:
            task = self._polling_task
            if task is None:
                return
            self._polling_stop.set()
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if not task.done():
                    raise
            except Exception as exc:
                logger.warning(f"Polling task stopped with error: {type(exc).__name__}: {exc}")
            if task.done() and self._polling_task is task:
                self._polling_task = None
                logger.info("⏬ Polling fallback stopped")

    async def _ws_session(self) -> None:
        if self.home_state is None:
            raise RuntimeError("WS session attempted before login")

        await self._refresh_ws_auth_if_needed()

        use_centrifugo = bool(
            self.home_state.centrifugo_token and self.home_state.centrifugo_url
        )

        ws_activity = asyncio.Event()

        def _on_ws_activity() -> None:
            self._record_ws_activity()
            ws_activity.set()

        async with aiohttp.ClientSession() as session:
            if use_centrifugo:
                client = CentrifugoClient(
                    ws_url=self.home_state.centrifugo_url,
                    token=self.home_state.centrifugo_token,
                    session=session,
                    receive_timeout=WS_RECEIVE_TIMEOUT,
                    on_activity=_on_ws_activity,
                )
            else:
                client = FJSignalRClient(
                    cookies=self.cookies_dict,
                    feedtoken=self.home_state.feedtoken,
                    session=session,
                    receive_timeout=WS_RECEIVE_TIMEOUT,
                    on_activity=_on_ws_activity,
                )
            try:
                await client.connect()
                last_auth_refresh = self._monotonic()
                refresh_interval = FEEDTOKEN_REFRESH_HOURS * 3600

                async def _ws_activity_watcher() -> None:
                    await ws_activity.wait()
                    await self._stop_polling()

                async def _ws_auth_watcher():
                    while True:
                        await asyncio.sleep(60)
                        try:
                            if self._monotonic() - last_auth_refresh >= refresh_interval:
                                refreshed = await self._refresh_feedtoken()
                            else:
                                refreshed = await self._refresh_ws_auth_if_needed()
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            logger.warning(
                                f"Periodic WS auth refresh failed: {type(exc).__name__}: {exc}"
                            )
                            continue
                        if refreshed:
                            logger.info("🔄 WS credentials refreshed; reconnecting active session")
                            await client.close()
                            return

                auth_watcher_task = asyncio.create_task(_ws_auth_watcher())
                activity_watcher_task = asyncio.create_task(_ws_activity_watcher())
                try:
                    async for items in client.listen():
                        await self.processor.process(items, source="WS")
                finally:
                    auth_watcher_task.cancel()
                    activity_watcher_task.cancel()
                    await asyncio.gather(
                        auth_watcher_task,
                        activity_watcher_task,
                        return_exceptions=True,
                    )
            finally:
                await client.close()

    async def _polling_loop(self) -> None:
        async with httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            timeout=20,
            follow_redirects=False,
        ) as client:
            while not self._polling_stop.is_set():
                observed_generation = self._auth_generation
                try:
                    if self.home_state is None:
                        raise StartupAuthError("Polling has no HomeState")
                    items = await fetch_startup(
                        client,
                        cookies=self.cookies_dict,
                        info=self.home_state.info,
                    )
                    self._record_transport_activity()
                    if items:
                        await self.processor.process(items, source="POLL")
                except StartupAuthError as exc:
                    logger.warning(f"Polling authentication error: {exc}")
                    await self._handle_auth_error(exc, observed_generation)
                except StartupTransportError as exc:
                    logger.error(f"Polling transport error: {exc}")
                except StartupProtocolError as exc:
                    logger.error(f"Polling protocol error: {exc}")
                except Exception as exc:
                    logger.error(f"Polling unexpected error: {type(exc).__name__}: {exc}")

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

    async def _handle_auth_error(self, err: Exception, observed_generation: int) -> bool:
        logger.error(f"Auth error: {err} - re-logging in")
        async with self._auth_lock:
            if observed_generation != self._auth_generation:
                return True
            now = self._monotonic()
            if now < self._auth_retry_after:
                logger.warning(
                    f"Auth refresh cooldown active for {self._auth_retry_after - now:.1f}s"
                )
                return False
            try:
                cookies = await login_with_retry(FJ_EMAIL, FJ_PASSWORD)
                refreshed_cookies = cookies_list_to_dict(cookies)
                async with aiohttp.ClientSession() as session:
                    refreshed_state = await fetch_home_state(session, refreshed_cookies)
                if not refreshed_state.logged_in:
                    raise RuntimeError("Re-login completed but /home reports LoggedUser=false")
                save_cookies(cookies, COOKIES_PATH)
                self.cookies_dict = refreshed_cookies
                self.home_state = refreshed_state
                self._auth_generation += 1
                self._auth_failures = 0
                self._auth_retry_after = 0.0
                return True
            except asyncio.CancelledError:
                raise
            except Exception as login_err:
                logger.error(f"Re-login failed: {login_err}")
                self._auth_failures += 1
                cooldown = min(
                    WS_RECONNECT_BASE_DELAY * (2 ** (self._auth_failures - 1)),
                    WS_RECONNECT_MAX_DELAY,
                )
                self._auth_retry_after = self._monotonic() + cooldown
                return False

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
                observed_generation = self._auth_generation
                try:
                    await self._ws_session()
                    self._ws_failures += 1
                    logger.warning(f"WS listen() returned without error #{self._ws_failures} - reconnecting")
                except WSAuthError as e:
                    await self._handle_auth_error(e, observed_generation)
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
