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

import _mock_tap_tester  # noqa: F401 — must be imported first to inject stubs
from tap_tester.base_suite_tests.base_case import BaseCase
from mock_data_generator import FIXTURES


class MockMailshakeBaseTest(BaseCase):
    """
    Integration-test base that exercises the tap against mocked HTTP responses.

    Inherits all catalog/sync helpers from tap_tester's BaseCase so that the
    full set of base suite tests (AllFieldsTest, DiscoveryTest, etc.) can be
    used alongside mock mode without modification.
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
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created"},
                cls.RESPECTS_START_DATE: True,
            },
            "recipients": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created"},
                cls.RESPECTS_START_DATE: True,
            },
            "leads": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created"},
                cls.RESPECTS_START_DATE: True,
            },
            "senders": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"created"},
                cls.RESPECTS_START_DATE: True,
            },
            "team_members": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.RESPECTS_START_DATE: False,
            },
            "sent_messages": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"actionDate"},
                cls.RESPECTS_START_DATE: True,
            },
            "opens": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"actionDate"},
                cls.RESPECTS_START_DATE: True,
            },
            "clicks": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"actionDate"},
                cls.RESPECTS_START_DATE: True,
            },
            "replies": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"actionDate"},
                cls.RESPECTS_START_DATE: True,
            },
        }

    def get_bookmark_value(self, state, stream):
        """tap-mailshake stores bookmarks as flat datetime strings, not nested dicts."""
        stream_id = self.get_stream_id(stream)
        return state.get("bookmarks", {}).get(stream_id) or state.get("bookmarks", {}).get(stream)

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
