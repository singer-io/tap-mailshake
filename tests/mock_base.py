"""MockMailshakeBaseTest — drop-in replacement for MailshakeBaseTest when running
integration tests without a live Mailshake account.

Usage
─────
Set INTEGRATION_TEST_MODE=mock (or leave unset when TAP_MAILSHAKE_API_KEY is
absent) and the existing test files automatically pick up this class through
tests/base.py.

HTTP mocking
────────────
MailshakeClient.request() is patched with a side-effect function that serves
dynamically generated JSON built from the tap's own JSON Schema files.
Fixtures are synthesised once at the start of the test session
(``tests/mock_data_generator.FIXTURES``) and held in memory for the
duration of the run.
"""
from __future__ import annotations

from _mock_tap_tester import BaseCase
from mock_data_generator import FIXTURES


class MockMailshakeBaseTest(BaseCase):
    """
    Integration-test base that exercises the tap against mocked HTTP responses.

    Inherits the catalog helpers (run_and_verify_check_mode,
    perform_and_verify_table_and_field_selection) from the stub BaseCase so
    that the existing tap_tester-style test patterns work unchanged.
    """

    start_date = "2024-01-01T00:00:00Z"

    @staticmethod
    def tap_name() -> str:
        return "tap-mailshake"

    @staticmethod
    def get_type() -> str:
        return "platform.mailshake"

    def get_properties(self, original: bool = True) -> dict:
        return {
            "start_date": self.start_date,
            "user_agent": "tap-mailshake-mock-test",
        }

    def get_credentials(self) -> dict:
        return {"api_key": "mock-api-key"}

    @classmethod
    def expected_metadata(cls) -> dict:
        return {
            "campaigns": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
            },
            "recipients": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created"},
            },
            "leads": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created"},
            },
            "senders": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created"},
            },
            "team_members": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
            },
            "sent_messages": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"actionDate"},
            },
            "opens": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"actionDate"},
            },
            "clicks": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"actionDate"},
            },
            "replies": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"actionDate"},
            },
        }

    @classmethod
    def expected_streams(cls) -> set:
        return set(cls.expected_metadata().keys())

    @classmethod
    def expected_primary_keys(cls) -> dict:
        return {s: m[cls.PRIMARY_KEYS] for s, m in cls.expected_metadata().items()}

    @classmethod
    def expected_replication_keys(cls) -> dict:
        return {s: m[cls.REPLICATION_KEYS] for s, m in cls.expected_metadata().items()}

    def _build_mock_request(self):
        """
        Return a side_effect callable for patching MailshakeClient.request.

        The callable receives the same arguments as MailshakeClient.request and
        returns (body_dict, next_token) by looking up the path/url fragment in
        the in-memory FIXTURES map generated from the tap's JSON Schema files.
        Unknown paths return an empty-results payload so the tap continues
        without errors.
        """
        def _side_effect(method, path=None, url=None, **kwargs):
            lookup = path or url or ""
            for key, body in FIXTURES.items():
                if key in lookup:
                    next_token = body.get("nextToken")
                    return body, next_token
            # Fallback: empty page — tap will move on to the next stream
            return {"results": [], "nextToken": None}, None

        return _side_effect
