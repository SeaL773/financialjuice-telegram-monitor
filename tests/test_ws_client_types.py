import importlib
import asyncio
import unittest

ws_client = importlib.import_module("src.api.ws_client")


class NegotiationValidationTestCase(unittest.TestCase):
    def test_required_string_accepts_valid_response(self):
        self.assertEqual(
            "value",
            ws_client.FJSignalRClient._required_string(
                {"Token": "value"}, "Token", "test"
            ),
        )

    def test_required_string_rejects_malformed_response(self):
        malformed = [None, [], {}, {"Token": None}, {"Token": ""}, {"Token": 1}]
        for value in malformed:
            with self.subTest(value=value):
                with self.assertRaises(ws_client.WSConnectionError):
                    ws_client.FJSignalRClient._required_string(
                        value, "Token", "test"
                    )

    def test_optional_guards_raise_connection_error(self):
        client = ws_client.FJSignalRClient({}, "feed", session=None)
        for guard in (
            client._require_session,
            client._require_redirect_url,
            client._require_access_token,
            client._require_connection_token,
        ):
            with self.assertRaises(ws_client.WSConnectionError):
                guard()


class SignalRReceiveTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_stale_receive_raises_connection_error(self):
        class StaleWebSocket:
            closed = False

            async def receive(self):
                await asyncio.Event().wait()

        client = ws_client.FJSignalRClient(
            {}, "feed", session=None, receive_timeout=0.01
        )
        client._ws = StaleWebSocket()

        with self.assertRaisesRegex(ws_client.WSConnectionError, "No SignalR frame"):
            await anext(client.listen())


if __name__ == "__main__":
    unittest.main()
