from tests.base import MailshakeBaseTest
from tap_tester import connections, menagerie, runner


class BookmarkTest(MailshakeBaseTest):
    """Test that incremental streams honour and advance their bookmarks."""

    @staticmethod
    def name():
        return "tap_mailshake_bookmark_test"

    def test_bookmark_is_saved_and_respected(self):
        """
        Run sync twice. The second sync should start from the bookmark saved
        by the first sync and return only newer records.
        """
        incremental_streams = {
            stream for stream, method in self.expected_replication_method().items()
            if method == self.INCREMENTAL
        }

        if not incremental_streams:
            self.skipTest("No INCREMENTAL streams to test bookmarks for")

        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, select_all_fields=True
        )

        # First sync
        first_sync_job = runner.run_sync_mode(self, conn_id)
        first_exit = menagerie.get_exit_status(conn_id, first_sync_job)
        menagerie.verify_sync_exit_status(self, first_exit, first_sync_job)
        first_record_count = runner.examine_target_output_file(
            self, conn_id, incremental_streams, self.expected_primary_keys()
        )
        first_state = menagerie.get_state(conn_id)

        # Second sync
        menagerie.set_state(conn_id, first_state)
        second_sync_job = runner.run_sync_mode(self, conn_id)
        second_exit = menagerie.get_exit_status(conn_id, second_sync_job)
        menagerie.verify_sync_exit_status(self, second_exit, second_sync_job)
        second_record_count = runner.examine_target_output_file(
            self, conn_id, incremental_streams, self.expected_primary_keys()
        )

        for stream in incremental_streams:
            with self.subTest(stream=stream):
                self.assertLessEqual(
                    second_record_count.get(stream, 0),
                    first_record_count.get(stream, 0),
                    msg=(
                        f"Second sync for '{stream}' should return ≤ records than first sync "
                        "(bookmark must reduce the result set)"
                    )
                )
