import asyncio
import unittest
from unittest.mock import ANY, AsyncMock, Mock, patch

from src.api.home_fetcher import HomeState
from src.api.startup_client import StartupAuthError
from src.api.ws_common import WSConnectionError
from src.core.monitor import FJMonitor


def home_state() -> HomeState:
    return HomeState(
        logged_in=True,
        user_id=1,
        feedtoken="feed",
        centrifugo_token="",
        centrifugo_url="",
        info="info",
    )


class MonitorPollingTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_polling_disables_redirect_following(self) -> None:
        monitor = FJMonitor()
        monitor.home_state = home_state()
        monitor._polling_stop.set()

        client_context = Mock()
        client_context.__aenter__ = AsyncMock(return_value=Mock())
        client_context.__aexit__ = AsyncMock(return_value=None)
        with patch(
            "src.core.monitor.httpx.AsyncClient",
            return_value=client_context,
        ) as client_factory:
            await monitor._polling_loop()

        client_factory.assert_called_once_with(
            headers={"User-Agent": ANY},
            timeout=20,
            follow_redirects=False,
        )

    async def test_successful_empty_poll_records_transport_activity(self) -> None:
        monitor = FJMonitor()
        monitor.home_state = home_state()
        monitor._record_transport_activity = Mock()
        monitor.processor.process = AsyncMock()

        async def stop_after_first(*args: object, **kwargs: object) -> list[dict[str, object]]:
            monitor._polling_stop.set()
            return []

        with patch("src.core.monitor.fetch_startup", side_effect=stop_after_first):
            await monitor._polling_loop()

        monitor._record_transport_activity.assert_called_once_with()
        monitor.processor.process.assert_not_awaited()

    async def test_polling_auth_error_relogs_in_and_continues(self) -> None:
        monitor = FJMonitor()
        monitor.home_state = home_state()
        monitor._handle_auth_error = AsyncMock()

        async def auth_failure(*args: object, **kwargs: object) -> list[dict[str, object]]:
            monitor._polling_stop.set()
            raise StartupAuthError("expired")

        with patch("src.core.monitor.fetch_startup", side_effect=auth_failure):
            await monitor._polling_loop()

        monitor._handle_auth_error.assert_awaited_once_with(
            ANY, 0
        )

    async def test_auth_handler_replaces_cookies_and_home_state(self) -> None:
        monitor = FJMonitor()
        monitor.cookies_dict = {"old": "cookie"}
        monitor.home_state = home_state()
        refreshed_state = HomeState(
            logged_in=True,
            user_id=2,
            feedtoken="new-feed",
            centrifugo_token="",
            centrifugo_url="",
            info="new-info",
        )
        cookies = [
            {
                "name": "session",
                "value": "new-cookie",
                "domain": ".financialjuice.com",
            }
        ]
        session = Mock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)

        with patch(
            "src.core.monitor.login_with_retry", AsyncMock(return_value=cookies)
        ), patch("src.core.monitor.save_cookies") as save, patch(
            "src.core.monitor.aiohttp.ClientSession", return_value=session
        ), patch(
            "src.core.monitor.fetch_home_state",
            AsyncMock(return_value=refreshed_state),
        ):
            result = await monitor._handle_auth_error(StartupAuthError("expired"), 0)

        self.assertTrue(result)
        self.assertEqual({"session": "new-cookie"}, monitor.cookies_dict)
        self.assertIs(refreshed_state, monitor.home_state)
        self.assertEqual(1, monitor._auth_generation)
        save.assert_called_once_with(cookies, ANY)

    async def test_polling_start_is_idempotent_and_stop_finishes_tracked_task(self) -> None:
        monitor = FJMonitor()
        started = asyncio.Event()

        async def loop() -> None:
            started.set()
            await monitor._polling_stop.wait()

        monitor._polling_loop = loop
        await asyncio.gather(
            monitor._ensure_polling_running(), monitor._ensure_polling_running()
        )
        await started.wait()
        task = monitor._polling_task
        self.assertIsNotNone(task)
        assert task is not None

        await monitor._stop_polling()

        self.assertTrue(task.done())
        self.assertFalse(task.cancelled())
        self.assertIsNone(monitor._polling_task)

    async def test_stop_polling_allows_inflight_processing_to_finish(self) -> None:
        monitor = FJMonitor()
        monitor.home_state = home_state()
        processing = asyncio.Event()
        release = asyncio.Event()

        async def process(items: object, source: str) -> None:
            processing.set()
            await release.wait()

        monitor.processor.process = AsyncMock(side_effect=process)
        with patch(
            "src.core.monitor.fetch_startup",
            AsyncMock(return_value=[{"NewsID": 1}]),
        ):
            await monitor._ensure_polling_running()
            await processing.wait()
            task = monitor._polling_task
            stop_task = asyncio.create_task(monitor._stop_polling())
            await asyncio.sleep(0)

            self.assertFalse(stop_task.done())
            self.assertIs(task, monitor._polling_task)
            release.set()
            await stop_task

        monitor.processor.process.assert_awaited_once()
        self.assertIsNone(monitor._polling_task)


class MonitorAuthTestCase(unittest.IsolatedAsyncioTestCase):
    def _session(self) -> Mock:
        session = Mock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        return session

    async def test_concurrent_same_generation_auth_errors_login_once(self) -> None:
        monitor = FJMonitor()
        monitor.home_state = home_state()
        refreshed_state = home_state()
        cookies = [{"name": "session", "value": "new", "domain": ".financialjuice.com"}]
        login = AsyncMock(return_value=cookies)

        with patch("src.core.monitor.login_with_retry", login), patch(
            "src.core.monitor.save_cookies"
        ), patch(
            "src.core.monitor.aiohttp.ClientSession", return_value=self._session()
        ), patch(
            "src.core.monitor.fetch_home_state", AsyncMock(return_value=refreshed_state)
        ):
            results = await asyncio.gather(
                monitor._handle_auth_error(StartupAuthError("poll"), 0),
                monitor._handle_auth_error(WSConnectionError("ws"), 0),
            )

        self.assertEqual([True, True], results)
        login.assert_awaited_once()
        self.assertEqual(1, monitor._auth_generation)

    async def test_repeated_auth_error_during_cooldown_skips_login(self) -> None:
        monitor = FJMonitor()
        monitor.home_state = home_state()
        monitor._monotonic = Mock(side_effect=[10.0, 10.0, 11.0])
        login = AsyncMock(side_effect=RuntimeError("offline"))

        with patch("src.core.monitor.login_with_retry", login):
            first = await monitor._handle_auth_error(StartupAuthError("first"), 0)
            second = await monitor._handle_auth_error(StartupAuthError("second"), 0)

        self.assertFalse(first)
        self.assertFalse(second)
        login.assert_awaited_once()

    async def test_stale_token_refresh_cannot_overwrite_completed_relogin(self) -> None:
        monitor = FJMonitor()
        monitor.cookies_dict = {"session": "old-cookie"}
        old_state = home_state()
        monitor.home_state = old_state
        old_fetch_started = asyncio.Event()
        release_old_fetch = asyncio.Event()
        stale_state = HomeState(
            logged_in=True,
            user_id=1,
            feedtoken="stale-feed",
            centrifugo_token="",
            centrifugo_url="",
            info="stale-info",
        )
        new_state = HomeState(
            logged_in=True,
            user_id=2,
            feedtoken="new-feed",
            centrifugo_token="",
            centrifugo_url="",
            info="new-info",
        )
        new_cookie_list = [
            {
                "name": "session",
                "value": "new-cookie",
                "domain": ".financialjuice.com",
            }
        ]
        fetch_calls = 0

        async def fetch_state(session: object, cookies: dict[str, str]) -> HomeState:
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls == 1:
                self.assertEqual({"session": "old-cookie"}, cookies)
                old_fetch_started.set()
                await release_old_fetch.wait()
                return stale_state
            self.assertEqual({"session": "new-cookie"}, cookies)
            return new_state

        with patch(
            "src.core.monitor.login_with_retry",
            AsyncMock(return_value=new_cookie_list),
        ), patch("src.core.monitor.save_cookies"), patch(
            "src.core.monitor.aiohttp.ClientSession", return_value=self._session()
        ), patch("src.core.monitor.fetch_home_state", side_effect=fetch_state):
            refresh_task = asyncio.create_task(monitor._refresh_feedtoken())
            await old_fetch_started.wait()
            relogin_result = await monitor._handle_auth_error(
                StartupAuthError("expired"), 0
            )
            release_old_fetch.set()
            refresh_result = await refresh_task

        self.assertTrue(relogin_result)
        self.assertTrue(refresh_result)
        self.assertEqual({"session": "new-cookie"}, monitor.cookies_dict)
        self.assertIs(new_state, monitor.home_state)
        self.assertEqual(1, monitor._auth_generation)


class MonitorWebSocketTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_ws_watcher_cleanup_preserves_external_cancellation(self) -> None:
        monitor = FJMonitor()
        monitor.home_state = home_state()
        client = Mock(connect=AsyncMock(), close=AsyncMock())

        async def listen():
            if False:
                yield []

        client.listen = listen
        with patch("src.core.monitor.FJSignalRClient", return_value=client), patch(
            "src.core.monitor.asyncio.gather",
            new=AsyncMock(side_effect=asyncio.CancelledError()),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await monitor._ws_session()

    async def test_refreshing_active_credentials_closes_ws_for_reconnect(self) -> None:
        monitor = FJMonitor()
        monitor.home_state = home_state()
        closed = asyncio.Event()
        client = Mock()
        client.connect = AsyncMock()

        async def close() -> None:
            closed.set()

        client.close = AsyncMock(side_effect=close)

        async def listen():
            await closed.wait()
            if False:
                yield []

        client.listen = listen

        monitor._refresh_ws_auth_if_needed = AsyncMock(side_effect=[False, True])
        with patch("src.core.monitor.FJSignalRClient", return_value=client), patch(
            "src.core.monitor.asyncio.sleep", new=AsyncMock()
        ):
            await monitor._ws_session()

        self.assertGreaterEqual(client.close.await_count, 1)

    async def test_periodic_interval_forces_refresh_and_reconnect(self) -> None:
        monitor = FJMonitor()
        monitor.home_state = home_state()
        closed = asyncio.Event()
        client = Mock(connect=AsyncMock())

        async def close() -> None:
            closed.set()

        async def listen():
            await closed.wait()
            if False:
                yield []

        client.close = AsyncMock(side_effect=close)
        client.listen = listen
        monitor._refresh_ws_auth_if_needed = AsyncMock(return_value=False)
        monitor._refresh_feedtoken = AsyncMock()
        monitor._monotonic = Mock(side_effect=[0.0, 3600.0])

        with patch("src.core.monitor.FJSignalRClient", return_value=client), patch(
            "src.core.monitor.asyncio.sleep", new=AsyncMock()
        ), patch("src.core.monitor.FEEDTOKEN_REFRESH_HOURS", 1):
            await monitor._ws_session()

        monitor._refresh_feedtoken.assert_awaited_once_with()
        client.close.assert_awaited()

    async def test_auth_watcher_continues_after_refresh_failure(self) -> None:
        monitor = FJMonitor()
        monitor.home_state = home_state()
        closed = asyncio.Event()
        client = Mock(connect=AsyncMock())

        async def close() -> None:
            closed.set()

        async def listen():
            await closed.wait()
            if False:
                yield []

        client.close = AsyncMock(side_effect=close)
        client.listen = listen
        monitor._refresh_ws_auth_if_needed = AsyncMock(
            side_effect=[False, RuntimeError("temporary"), True]
        )
        monitor._monotonic = Mock(side_effect=[0.0, 1.0, 2.0])

        with patch("src.core.monitor.FJSignalRClient", return_value=client), patch(
            "src.core.monitor.asyncio.sleep", new=AsyncMock()
        ):
            await monitor._ws_session()

        self.assertEqual(3, monitor._refresh_ws_auth_if_needed.await_count)
        client.close.assert_awaited()

    async def test_refresh_helper_reports_when_home_state_changed(self) -> None:
        monitor = FJMonitor()
        state = home_state()
        state.feedtoken_expired = Mock(return_value=True)
        monitor.home_state = state
        monitor._refresh_feedtoken = AsyncMock()

        self.assertTrue(await monitor._refresh_ws_auth_if_needed())
        monitor._refresh_feedtoken.assert_awaited_once_with()

    async def test_post_connect_protocol_frame_resets_ws_failures(self) -> None:
        monitor = FJMonitor()
        monitor.home_state = home_state()
        monitor._ws_failures = 3
        activity = None
        client = Mock(connect=AsyncMock(), close=AsyncMock())

        def factory(**kwargs: object) -> Mock:
            nonlocal activity
            activity = kwargs["on_activity"]
            return client

        async def listen():
            assert callable(activity)
            activity()
            raise WSConnectionError("later stale")
            if False:
                yield []

        client.listen = listen
        with patch("src.core.monitor.FJSignalRClient", side_effect=factory):
            with self.assertRaises(WSConnectionError):
                await monitor._ws_session()

        self.assertEqual(0, monitor._ws_failures)


class MonitorSupervisorTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_two_connect_success_listen_stale_failures_start_polling(self) -> None:
        monitor = FJMonitor()
        monitor._ensure_logged_in = AsyncMock()
        monitor._ws_session = AsyncMock(
            side_effect=[
                WSConnectionError("stale one"),
                WSConnectionError("stale two"),
            ]
        )
        monitor._ensure_polling_running = AsyncMock(
            side_effect=asyncio.CancelledError()
        )
        monitor._stop_polling = AsyncMock()

        with patch("src.core.monitor._HEARTBEAT") as heartbeat, patch(
            "src.core.monitor.threading.Thread"
        ), patch("src.core.monitor.asyncio.sleep", new=AsyncMock()):
            with self.assertRaises(asyncio.CancelledError):
                await monitor.run()

        heartbeat.write_text.assert_called_once_with("")
        self.assertEqual(2, monitor._ws_failures)
        monitor._ensure_polling_running.assert_awaited_once_with()


if __name__ == "__main__":
    unittest.main()
