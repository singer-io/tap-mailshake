import unittest
from unittest.mock import patch, MagicMock
from singer.catalog import Catalog, CatalogEntry
from tap_mailshake.discover import discover, check_stream_access
from tap_mailshake.client import MailshakeInvalidApiKeyError, MailshakeNotAuthorizedError


# ---------------------------------------------------------------------------
# check_stream_access
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):

    def test_returns_true_when_accessible(self):
        client = MagicMock()
        client.base_url = 'https://api.mailshake.com/2017-04-01'
        stream_config = {'path': 'campaigns/list'}
        result = check_stream_access(client, 'campaigns', stream_config)
        self.assertTrue(result)
        client.get.assert_called_once_with(
            url='https://api.mailshake.com/2017-04-01/campaigns/list',
            path='campaigns/list',
            params='perPage=1',
            endpoint='campaigns',
        )

    def test_returns_false_on_invalid_api_key(self):
        client = MagicMock()
        client.get.side_effect = MailshakeInvalidApiKeyError("invalid_api_key")
        result = check_stream_access(client, 'campaigns', {'path': 'campaigns/list'})
        self.assertFalse(result)

    def test_returns_false_on_not_authorized(self):
        client = MagicMock()
        client.get.side_effect = MailshakeNotAuthorizedError("not_authorized")
        result = check_stream_access(client, 'leads', {'path': 'leads/list'})
        self.assertFalse(result)

    def test_reraises_other_errors(self):
        client = MagicMock()
        client.get.side_effect = ConnectionError("network error")
        with self.assertRaises(ConnectionError):
            check_stream_access(client, 'campaigns', {'path': 'campaigns/list'})


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):

    @patch("tap_mailshake.discover.check_stream_access")
    def test_discover_returns_catalog(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        self.assertIsInstance(catalog, Catalog)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_catalog_streams_is_a_list(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        self.assertIsInstance(catalog.streams, list)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_catalog_contains_known_streams_when_all_accessible(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        stream_names = [s.stream for s in catalog.streams]
        for expected in ('campaigns', 'leads', 'senders', 'recipients'):
            self.assertIn(expected, stream_names)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_all_entries_have_stream_name(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        for entry in catalog.streams:
            self.assertIsNotNone(entry.stream)
            self.assertIsInstance(entry.stream, str)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_all_entries_have_tap_stream_id(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        for entry in catalog.streams:
            self.assertEqual(entry.tap_stream_id, entry.stream)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_all_entries_have_key_properties(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        for entry in catalog.streams:
            self.assertIsNotNone(entry.key_properties)
            self.assertIsInstance(entry.key_properties, list)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_campaigns_key_property_is_id(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        campaigns_entry = next(s for s in catalog.streams if s.stream == 'campaigns')
        self.assertEqual(campaigns_entry.key_properties, ['id'])

    @patch("tap_mailshake.discover.check_stream_access")
    def test_entries_have_schema(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        for entry in catalog.streams:
            self.assertIsNotNone(entry.schema)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_entries_have_metadata(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        for entry in catalog.streams:
            self.assertIsNotNone(entry.metadata)
            self.assertIsInstance(entry.metadata, list)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_stream_count_matches_flat_streams_when_all_accessible(self, mock_check):
        from tap_mailshake.streams import flatten_streams
        mock_check.return_value = True
        catalog = discover(MagicMock())
        self.assertEqual(len(catalog.streams), len(flatten_streams()))

    @patch("tap_mailshake.discover.check_stream_access")
    def test_inaccessible_stream_excluded(self, mock_check):
        """A stream that fails the access check is excluded from the catalog."""
        mock_check.side_effect = lambda client, name, cfg: name != 'leads'
        catalog = discover(MagicMock())
        stream_names = [s.stream for s in catalog.streams]
        self.assertNotIn('leads', stream_names)
        self.assertIn('campaigns', stream_names)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_child_stream_excluded_when_parent_inaccessible(self, mock_check):
        """Child stream 'recipients' is excluded when parent 'campaigns' is inaccessible."""
        mock_check.side_effect = lambda client, name, cfg: name != 'campaigns'
        catalog = discover(MagicMock())
        stream_names = [s.stream for s in catalog.streams]
        self.assertNotIn('campaigns', stream_names)
        self.assertNotIn('recipients', stream_names)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_child_stream_included_when_parent_accessible(self, mock_check):
        """Child stream 'recipients' is included when parent 'campaigns' is accessible."""
        mock_check.return_value = True
        catalog = discover(MagicMock())
        stream_names = [s.stream for s in catalog.streams]
        self.assertIn('campaigns', stream_names)
        self.assertIn('recipients', stream_names)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_child_stream_not_probed_directly(self, mock_check):
        """check_stream_access is never called for child streams."""
        mock_check.return_value = True
        discover(MagicMock())
        probed = [call.args[1] for call in mock_check.call_args_list]
        self.assertNotIn('recipients', probed)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_warning_logged_for_excluded_stream(self, mock_check):
        """A warning is logged when a stream is excluded due to insufficient permissions."""
        mock_check.side_effect = lambda client, name, cfg: name != 'leads'
        with patch("tap_mailshake.discover.LOGGER") as mock_logger:
            discover(MagicMock())
        warned = [call.args[1] for call in mock_logger.warning.call_args_list]
        self.assertIn('leads', warned)

    @patch("tap_mailshake.discover.check_stream_access")
    def test_all_inaccessible_raises_exception(self, mock_check):
        """When all streams are inaccessible, discover() raises an exception."""
        mock_check.return_value = False
        with self.assertRaises(Exception) as ctx:
            discover(MagicMock())
        self.assertIn("do not have read access", str(ctx.exception))
