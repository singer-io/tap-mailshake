import unittest
from unittest.mock import patch
from requests.exceptions import Timeout, ConnectionError
from tap_mailshake.client import MailshakeClient


class TestMailshakeClientRetries(unittest.TestCase):

    @patch("time.sleep", return_value=None)
    @patch("requests.Session.request", side_effect=Timeout)
    def test_request_retries_on_timeout(self, mocked_request, mocked_sleep):
        """Test that MailshakeClient.request retries on Timeout error."""
        client = MailshakeClient(api_key="dummy")

        # Expect a Timeout exception after retries
        with self.assertRaises(Timeout):
            client.request("GET", path="some_endpoint")

        # Verify request retried 5 times due to backoff config
        self.assertEqual(mocked_request.call_count, 5)

    @patch("time.sleep", return_value=None)
    @patch("requests.Session.request", side_effect=[ConnectionError()] * 5)
    def test_connection_error(self, mock_request, _):
        """Simulate 5 retries on ConnectionError."""
        client = MailshakeClient(api_key="dummy", user_agent="test")

        with self.assertRaises(ConnectionError):
            client.request("GET", path="dummy_endpoint")

        self.assertEqual(mock_request.call_count, 5)
