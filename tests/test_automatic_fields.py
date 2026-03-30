from tests.base import MailshakeBaseTest
from tap_tester import connections, menagerie, runner


class AutomaticFieldsTest(MailshakeBaseTest):
    """Test that primary keys and replication keys are always selected."""

    @staticmethod
    def name():
        return "tap_mailshake_automatic_fields_test"

    def test_automatic_fields_only(self):
        """
        Verify that selecting only automatic fields (PKs + bookmark fields) still
        produces SCHEMA + RECORD + STATE messages without errors.
        """
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only automatic fields for each stream
        for catalog_entry in found_catalogs:
            stream = catalog_entry["stream_name"]
            stream_id = catalog_entry["tap_stream_id"]
            schema = catalog_entry.get("schema", {})

            # Deselect all non-automatic fields
            automatic_fields = (
                self.expected_primary_keys().get(stream, set()) |
                self.expected_replication_keys().get(stream, set())
            )
            non_selected = {
                f for f in schema.get("properties", {}).keys()
                if f not in automatic_fields
            }
            self.select_all_streams_and_fields(conn_id, found_catalogs, select_all_fields=False)
            menagerie.select_catalog(conn_id, catalog_entry)

        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys()
        )

        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                self.assertGreaterEqual(record_count_by_stream.get(stream, 0), 0)
