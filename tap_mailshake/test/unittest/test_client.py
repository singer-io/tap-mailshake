import unittest
from unittest.mock import patch, Mock
from parameterized import parameterized
from requests.exceptions import Timeout
from unittest.mock import patch
from requests.exceptions import ConnectionError
from tap_mailshake.client import MailshakeClient


class TestMailshakeClientTimeout(unittest.TestCase):

    @parameterized.expand([
        ("valid_timeout", {"request_timeout": 50}, 50),
        ("default_timeout", {}, 300),
    ])
    @patch('tap_mailshake.client.requests.Session.request')
    def test_timeout_values(self, name, request_kwargs, expected_timeout, mock_request):
        """Test that request uses the correct timeout value."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'nextToken': ''}
        mock_request.return_value = mock_response

        client = MailshakeClient(api_key="test-key")
        client.request(method='GET', path='some-endpoint', **request_kwargs)

        _, kwargs = mock_request.call_args
        self.assertEqual(kwargs.get('timeout'), expected_timeout)

    def test_none_timeout_raises_value_error(self):
        """Test that None timeout raises ValueError."""
        client = MailshakeClient(api_key="test-key")
        with self.assertRaises(ValueError) as context:
            client.request(method='GET', path='some-endpoint', request_timeout=None)
        self.assertIn("Timeout must be explicitly set and cannot be None", str(context.exception))

    @patch('tap_mailshake.client.requests.Session.request')
    def test_timeout_exception_is_raised(self, mock_request):
        """Test that a timeout exception is raised when request times out."""
        mock_request.side_effect = Timeout("Simulated timeout")

        client = MailshakeClient(api_key="test-key")
        with self.assertRaises(Timeout):
            client.request(method='GET', path='some-endpoint', request_timeout=10)

    @patch("time.sleep", return_value=None)
    def test_connection_error(self, _):
        client = MailshakeClient(api_key="dummy", user_agent="test")

        # Simulate 5 failures to exhaust retry attempts
        with patch.object(
                client._MailshakeClient__session,
                "request",
                side_effect=[ConnectionError] * 5
        ) as mock_request:
            with self.assertRaises(ConnectionError):
                client.request("GET", path="dummy_endpoint")

            self.assertEqual(mock_request.call_count, 5)










