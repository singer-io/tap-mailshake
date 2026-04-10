from base import MailshakeBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class AutomaticFieldsTest(MinimumSelectionTest, MailshakeBaseTest):
    """Verify that primary and replication keys are always replicated."""

    @staticmethod
    def name():
        return "tap_mailshake_automatic_fields_test"

    def streams_to_test(self):
        streams_to_exclude = set({"recipients"})
        return self.expected_stream_names().difference(streams_to_exclude)
