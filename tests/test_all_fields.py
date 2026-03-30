from tests.base import MailshakeBaseTest
from tap_tester import connections, menagerie, runner


class AllFieldsTest(MailshakeBaseTest):
    """Test that syncing all fields returns data for all expected streams."""

    @staticmethod
    def name():
        return "tap_mailshake_all_fields_test"

    def test_all_fields(self):
        """
        Verify that selecting all fields in all streams produces records
        for every stream without errors.
        """
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, select_all_fields=True
        )

        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys()
        )

        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                self.assertGreaterEqual(
                    record_count_by_stream.get(stream, 0), 0,
                    msg=f"Expected records for stream '{stream}'"
                )
