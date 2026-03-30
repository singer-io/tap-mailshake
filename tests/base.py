import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import connections, menagerie, runner
from tap_tester.base_suite_tests.base_case import BaseCase


class MailshakeBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """
    start_date = "2024-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        return "tap-mailshake"

    @staticmethod
    def get_type():
        return "platform.mailshake"

    def get_properties(self, original=True):
        return {
            "start_date": self.start_date,
            "user_agent": "tap-mailshake <api_user_agent@example.com>",
        }

    def get_credentials(self):
        return {
            "api_key": os.environ["TAP_MAILSHAKE_API_KEY"],
        }

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
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
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"team_id"},
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
    def expected_streams(cls):
        return set(cls.expected_metadata().keys())

    @classmethod
    def expected_primary_keys(cls):
        return {stream: meta[cls.PRIMARY_KEYS]
                for stream, meta in cls.expected_metadata().items()}

    @classmethod
    def expected_replication_keys(cls):
        return {stream: meta[cls.REPLICATION_KEYS]
                for stream, meta in cls.expected_metadata().items()}

    @classmethod
    def expected_replication_method(cls):
        return {stream: meta[cls.REPLICATION_METHOD]
                for stream, meta in cls.expected_metadata().items()}
