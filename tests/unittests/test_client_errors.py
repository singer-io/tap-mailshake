import unittest
from parameterized import parameterized
from tap_mailshake.client import (
    get_exception_for_error_code,
    ERROR_CODE_EXCEPTION_MAPPING,
    MailshakeError,
    MailshakeInvalidApiKeyError,
    MailshakeAPILimitReachedError,
    MailshakeNotFoundError,
    MailshakeNotAuthorizedError,
    MailshakeInternalError,
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
