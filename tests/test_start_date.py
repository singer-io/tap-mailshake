from base import MailshakeBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest as TT_StartDateTest


class StartDateTest(TT_StartDateTest, MailshakeBaseTest):
    """Verify that the tap's start_date config controls how far back data is synced."""

    # start_date_1: before all 3 mock records -> 3 records per incremental stream
    start_date_1 = "2021-01-01T00:00:00Z"
    # start_date_2: between the 2nd (2024-01-15) and 3rd (2024-02-01) mock record
    start_date_2 = "2024-01-20T00:00:00Z"

    @staticmethod
    def name():
        return "tap_mailshake_start_date_test"

    def streams_to_test(self):
        # Exclude recipients: it is a child stream of campaigns. When only
        # incremental streams are selected, campaigns (full-table) is excluded
        # from the catalog selection, so recipients are never synced.
        return {s for s, m in self.expected_replication_method().items()
                if m == self.INCREMENTAL and s != "recipients"}
