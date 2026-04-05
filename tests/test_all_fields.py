from base import MailshakeBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest as TT_AllFieldsTest


class AllFieldsTest(TT_AllFieldsTest, MailshakeBaseTest):
    """Verify all fields are replicated for every stream."""

    @staticmethod
    def name():
        return "tap_mailshake_all_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names()
