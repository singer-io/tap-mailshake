import unittest
from base import MailshakeBaseTest
from tap_tester.base_suite_tests.pagination_test import PaginationTest as TT_PaginationTest


class PaginationTest(TT_PaginationTest, MailshakeBaseTest):
    """Verify the tap pages through large result sets correctly."""

    @staticmethod
    def name():
        return "tap_mailshake_pagination_test"

    def streams_to_test(self):
        # Exclude recipients (duplicate IDs across parent campaigns in mock).
        return self.expected_stream_names().difference({"recipients"})

    @unittest.skip(
        "Requires live API data with > 100 records per stream; not applicable in mock mode."
    )
    def test_record_count_greater_than_page_limit(self):
        """Skip — mock data has only 3 records per stream."""
