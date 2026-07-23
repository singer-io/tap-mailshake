import unittest
from unittest.mock import Mock, patch
from parameterized import parameterized
import requests
from tap_mailshake.client import (
    get_exception_for_error_code,
    ERROR_CODE_EXCEPTION_MAPPING,
    MailshakeError,
    MailshakeInvalidApiKeyError,
    MailshakeAPILimitReachedError,
    MailshakeNotFoundError,
    MailshakeNotAuthorizedError,
    MailshakeInternalError,
    MailshakeMissingParameterError,
    MailshakeClient,
    REQUEST_TIMEOUT,
)


class TestGetExceptionForErrorCode(unittest.TestCase):

    @parameterized.expand([
        ("invalid_api_key",    "invalid_api_key",     MailshakeInvalidApiKeyError),
        ("limit_reached",      "limit_reached",       MailshakeAPILimitReachedError),
        ("not_found",          "not_found",           MailshakeNotFoundError),
        ("not_authorized",     "not_authorized",      MailshakeNotAuthorizedError),
        ("internal_error",     "internal_error",      MailshakeInternalError),
        ("unknown_code",       "totally_unknown_code", MailshakeError),
    ])
    def test_error_code_maps_to_correct_exception(self, _name, error_code, expected_exc):
        exc = get_exception_for_error_code(error_code)
        self.assertIs(exc, expected_exc)

    def test_all_mapped_codes_return_non_base_exception(self):
        for code, exc_class in ERROR_CODE_EXCEPTION_MAPPING.items():
            self.assertIsNot(
                exc_class, MailshakeError,
                msg=f"Code '{code}' should map to a specific subclass, not base MailshakeError"
            )


class TestMailshakeClientInit(unittest.TestCase):

    def test_default_timeout_is_request_timeout_constant(self):
        client = MailshakeClient(api_key='test-key')
        self.assertEqual(client.request_timeout, REQUEST_TIMEOUT)

    def test_custom_timeout_is_respected(self):
        client = MailshakeClient(api_key='test-key', request_timeout=60)
        self.assertEqual(client.request_timeout, 60)

    def test_none_timeout_falls_back_to_default(self):
        client = MailshakeClient(api_key='test-key', request_timeout=None)
        self.assertEqual(client.request_timeout, REQUEST_TIMEOUT)

    def test_base_url_contains_api_version(self):
        from tap_mailshake.client import API_VERSION
        client = MailshakeClient(api_key='test-key')
        self.assertIn(API_VERSION, client.base_url)

    def test_base_url_contains_mailshake_domain(self):
        client = MailshakeClient(api_key='test-key')
        self.assertIn('mailshake.com', client.base_url)


class TestMailshakeClientGetPost(unittest.TestCase):

    def setUp(self):
        from unittest.mock import patch, Mock
        self.client = MailshakeClient(api_key='test-key')
        patcher = patch.object(self.client, 'request')
        self.mock_request = patcher.start()
        self.mock_request.return_value = ({'results': []}, None)
        self.addCleanup(patcher.stop)

    def test_get_calls_request_with_get_method(self):
        self.client.get('some/path')
        self.mock_request.assert_called_once()
        args, kwargs = self.mock_request.call_args
        self.assertEqual(args[0], 'GET')

    def test_post_calls_request_with_post_method(self):
        self.client.post('some/path')
        self.mock_request.assert_called_once()
        args, kwargs = self.mock_request.call_args
        self.assertEqual(args[0], 'POST')

    def test_get_passes_path_kwarg(self):
        self.client.get('campaigns/list')
        _, kwargs = self.mock_request.call_args
        self.assertEqual(kwargs['path'], 'campaigns/list')


class TestMailshakeClientAccessValidation(unittest.TestCase):

    def test_missing_api_key_raises_missing_parameter(self):
        client = MailshakeClient(api_key=None)
        with self.assertRaises(MailshakeMissingParameterError):
            client.check_access()

    @patch("tap_mailshake.client.requests.Session.get")
    def test_invalid_api_key_raises_immediately(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "unauthorized"
        mock_response.reason = "Unauthorized"
        mock_response.content = b'{"error":"invalid_api_key","message":"bad key","code":"invalid_api_key"}'
        mock_response.raise_for_status.side_effect = requests.HTTPError("401 Client Error")
        mock_response.json.return_value = {
            "error": "invalid_api_key",
            "message": "bad key",
            "code": "invalid_api_key",
        }
        mock_get.return_value = mock_response

        client = MailshakeClient(api_key="bad-key")
        with self.assertRaises(MailshakeInvalidApiKeyError):
            client.check_access()

    @patch("tap_mailshake.client.LOGGER")
    @patch("tap_mailshake.client.requests.Session.get")
    def test_invalid_api_key_is_masked_in_logs(self, mock_get, mock_logger):
        exposed_key = "abcdefgh=="
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = (
            '{"code":"invalid_api_key","error":"Invalid api key: '
            + exposed_key + '","time":"2026-07-09T08:39:40.461Z"}'
        )
        mock_response.reason = "Unauthorized"
        mock_response.content = mock_response.text.encode("utf-8")
        mock_response.raise_for_status.side_effect = requests.HTTPError("401 Client Error")
        mock_response.json.return_value = {
            "code": "invalid_api_key",
            "error": "invalid_api_key",
            "message": "bad key"
        }
        mock_get.return_value = mock_response

        client = MailshakeClient(api_key="bad-key")
        with self.assertRaises(MailshakeInvalidApiKeyError):
            client.check_access()

        logged_message = mock_logger.error.call_args[0][0]
        self.assertNotIn(exposed_key, logged_message)
        self.assertIn("Invalid api key: ***", logged_message)

    @patch("tap_mailshake.client.requests.Session.get")
    def test_invalid_api_key_is_masked_in_raised_exception_message(self, mock_get):
        exposed_key = "abcdefgh=="
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = (
            '{"code":"invalid_api_key","error":"invalid_api_key","message":"Invalid api key: '
            + exposed_key + '"}'
        )
        mock_response.reason = "Unauthorized"
        mock_response.content = mock_response.text.encode("utf-8")
        mock_response.raise_for_status.side_effect = requests.HTTPError("401 Client Error")
        mock_response.json.return_value = {
            "code": "invalid_api_key",
            "error": "invalid_api_key",
            "message": "Invalid api key: " + exposed_key
        }
        mock_get.return_value = mock_response

        client = MailshakeClient(api_key="bad-key")
        with self.assertRaises(MailshakeInvalidApiKeyError) as raised_error:
            client.check_access()

        self.assertNotIn(exposed_key, str(raised_error.exception))
        self.assertIn("Invalid api key: ***", str(raised_error.exception))

    @patch("tap_mailshake.client.requests.Session.get")
    def test_invalid_api_key_in_error_field_is_masked_in_raised_exception_message(self, mock_get):
        exposed_key = "abcdefgh=="
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = (
            '{"code":"invalid_api_key","error":"Invalid api key: '
            + exposed_key + '","time":"2026-07-09T08:39:40.461Z"}'
        )
        mock_response.reason = "Unauthorized"
        mock_response.content = mock_response.text.encode("utf-8")
        mock_response.raise_for_status.side_effect = requests.HTTPError("401 Client Error")
        mock_response.json.return_value = {
            "code": "invalid_api_key",
            "error": "Invalid api key: " + exposed_key,
        }
        mock_get.return_value = mock_response

        client = MailshakeClient(api_key="bad-key")
        with self.assertRaises(MailshakeInvalidApiKeyError) as raised_error:
            client.check_access()

        self.assertNotIn(exposed_key, str(raised_error.exception))
        self.assertIn("Invalid api key: ***", str(raised_error.exception))
