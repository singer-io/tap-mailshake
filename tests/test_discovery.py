from base import MailshakeBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest as TT_DiscoveryTest


class DiscoveryTest(TT_DiscoveryTest, MailshakeBaseTest):
    """Tap-mailshake discovery test using the standard tap-tester suite."""

    @staticmethod
    def name():
        return "tap_mailshake_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()
