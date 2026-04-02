from base import MailshakeBaseTest
from tap_tester import connections, menagerie, runner


class DiscoveryTest(MailshakeBaseTest):
    """Test tap discovery mode returns expected streams and metadata."""

    @staticmethod
    def name():
        return "tap_mailshake_discovery_test"

    def test_stream_discovery(self):
        """Verify all expected streams are discovered."""
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        found_streams = {c["stream_name"] for c in found_catalogs}
        self.assertSetEqual(self.expected_streams(), found_streams)

    def test_catalog_entries_have_key_properties(self):
        """Verify every catalog entry has key_properties."""
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        for entry in found_catalogs:
            stream = entry["stream_name"]
            with self.subTest(stream=stream):
                self.assertIn("key_properties", entry.get("metadata", {}).get("metadata", {}))

    def test_incremental_streams_have_replication_keys(self):
        """Verify INCREMENTAL streams declare valid-replication-keys."""
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        for entry in found_catalogs:
            stream = entry["stream_name"]
            expected_method = self.expected_replication_method().get(stream)
            if expected_method == self.INCREMENTAL:
                with self.subTest(stream=stream):
                    self.assertIn(stream, self.expected_replication_keys())
                    self.assertTrue(self.expected_replication_keys()[stream])
