from base import MailshakeBaseTest
from tap_tester import connections, menagerie, runner


class StartDateTest(MailshakeBaseTest):
    """Test that the start_date config controls how far back data is synced."""

    @staticmethod
    def name():
        return "tap_mailshake_start_date_test"

    def test_start_date_is_respected(self):
        """
        Run sync with a recent start_date. Expect fewer records than with an
        older start_date for INCREMENTAL streams that obey start_date.
        """
        incremental_streams = {
            stream for stream, method in self.expected_replication_method().items()
            if method == self.INCREMENTAL
        }

        if not incremental_streams:
            self.skipTest("No INCREMENTAL streams to test start_date for")

        # Sync 1 — older start date (more data expected)
        self.start_date = "2021-01-01T00:00:00Z"
        conn_id_1 = connections.ensure_connection(self, original_properties=False)
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)
        self.perform_and_verify_table_and_field_selection(
            conn_id_1, found_catalogs_1, select_all_fields=True
        )
        job_1 = runner.run_sync_mode(self, conn_id_1)
        exit_1 = menagerie.get_exit_status(conn_id_1, job_1)
        menagerie.verify_sync_exit_status(self, exit_1, job_1)
        count_1 = runner.examine_target_output_file(
            self, conn_id_1, incremental_streams, self.expected_primary_keys()
        )

        # Sync 2 — newer start date (fewer data expected)
        self.start_date = "2024-06-01T00:00:00Z"
        conn_id_2 = connections.ensure_connection(self, original_properties=False)
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)
        self.perform_and_verify_table_and_field_selection(
            conn_id_2, found_catalogs_2, select_all_fields=True
        )
        job_2 = runner.run_sync_mode(self, conn_id_2)
        exit_2 = menagerie.get_exit_status(conn_id_2, job_2)
        menagerie.verify_sync_exit_status(self, exit_2, job_2)
        count_2 = runner.examine_target_output_file(
            self, conn_id_2, incremental_streams, self.expected_primary_keys()
        )

        for stream in incremental_streams:
            with self.subTest(stream=stream):
                self.assertGreaterEqual(
                    count_1.get(stream, 0),
                    count_2.get(stream, 0),
                    msg=(
                        f"Stream '{stream}': older start_date should return ≥ records "
                        "than newer start_date"
                    )
                )
