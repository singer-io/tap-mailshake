import unittest
from unittest.mock import patch, Mock
from tap_mailshake.client import MailshakeClient
from parameterized import parameterized
from requests.exceptions import Timeout


class TestMailshakeClientTimeout(unittest.TestCase):
    @parameterized.expand([
        ("valid_timeout", {"request_timeout": 50}, 50, False, False),
        ("default_timeout", {}, 300, False, False),
        ("none_timeout_should_raise", {"request_timeout": None}, None, True, False),
        ("timeout_exception_raised", {"request_timeout": 10}, 10, False, True),
    ])
    @patch('tap_mailshake.client.metrics.http_request_timer')
    @patch('tap_mailshake.client.requests.Session.request')
    def test_request_timeout_behavior(
        self, name, request_kwargs, expected_timeout,
        expect_value_error, simulate_timeout,
        mock_request, mock_timer
    ):
        mock_timer.return_value.__enter__.return_value = Mock(tags={})

        if simulate_timeout:
            mock_request.side_effect = Timeout("Simulated timeout")
        elif not expect_value_error:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {'nextToken': ''}
            mock_request.return_value = mock_response

        client = MailshakeClient(api_key="test-key")

        if expect_value_error:
            with self.assertRaises(ValueError) as context:
                client.request(method='GET', path='some-endpoint', **request_kwargs)
            self.assertIn("Timeout must be explicitly set and cannot be None", str(context.exception))
        elif simulate_timeout:
            with self.assertRaises(Timeout):
                client.request(method='GET', path='some-endpoint', **request_kwargs)
        else:
            client.request(method='GET', path='some-endpoint', **request_kwargs)
            _, kwargs = mock_request.call_args
            self.assertEqual(kwargs.get('timeout'), expected_timeout)
