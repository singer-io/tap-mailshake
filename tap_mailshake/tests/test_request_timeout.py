import unittest
from unittest.mock import patch, Mock
from tap_mailshake.client import MailshakeClient


class TestMailshakeClientTimeout(unittest.TestCase):
    @patch('tap_mailshake.client.metrics.http_request_timer')
    @patch('tap_mailshake.client.requests.Session.request')
    def test_request_passes_with_valid_timeout(self, mock_request, mock_timer):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'nextToken': ''}
        mock_request.return_value = mock_response
        mock_timer.return_value.__enter__.return_value = Mock(tags={})
        client = MailshakeClient(api_key="test-key")

        client.request(method='GET', path='some-endpoint', timeout= 50)
        _, kwargs = mock_request.call_args
        self.assertEqual(kwargs.get('timeout'), 50)

    @patch('tap_mailshake.client.metrics.http_request_timer')
    def test_request_fails_with_none_timeout(self, mock_timer):
        mock_timer.return_value.__enter__.return_value = Mock(tags={})
        client = MailshakeClient(api_key="test-key")
        with self.assertRaises(ValueError) as context:
            client.request(method='GET', path='some-endpoint', timeout=None)
        self.assertIn("Timeout must be explicitly set and cannot be None", str(context.exception))


if __name__ == "__main__":
    unittest.main()