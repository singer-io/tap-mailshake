import unittest
from unittest.mock import patch, MagicMock
from singer.catalog import Catalog, CatalogEntry
from tap_mailshake.discover import discover


class TestDiscover(unittest.TestCase):

    def test_discover_returns_catalog(self):
        catalog = discover()
        self.assertIsInstance(catalog, Catalog)

    def test_catalog_streams_is_a_list(self):
        catalog = discover()
        self.assertIsInstance(catalog.streams, list)

    def test_catalog_contains_known_streams(self):
        catalog = discover()
        stream_names = [s.stream for s in catalog.streams]
        for expected in ('campaigns', 'leads', 'senders', 'recipients'):
            self.assertIn(expected, stream_names)

    def test_all_entries_have_stream_name(self):
        catalog = discover()
        for entry in catalog.streams:
            self.assertIsNotNone(entry.stream)
            self.assertIsInstance(entry.stream, str)

    def test_all_entries_have_tap_stream_id(self):
        catalog = discover()
        for entry in catalog.streams:
            self.assertEqual(entry.tap_stream_id, entry.stream)

    def test_all_entries_have_key_properties(self):
        catalog = discover()
        for entry in catalog.streams:
            self.assertIsNotNone(entry.key_properties)
            self.assertIsInstance(entry.key_properties, list)

    def test_campaigns_key_property_is_id(self):
        catalog = discover()
        campaigns_entry = next(s for s in catalog.streams if s.stream == 'campaigns')
        self.assertEqual(campaigns_entry.key_properties, ['id'])

    def test_entries_have_schema(self):
        catalog = discover()
        for entry in catalog.streams:
            self.assertIsNotNone(entry.schema)

    def test_entries_have_metadata(self):
        catalog = discover()
        for entry in catalog.streams:
            self.assertIsNotNone(entry.metadata)
            self.assertIsInstance(entry.metadata, list)

    def test_stream_count_matches_flat_streams(self):
        from tap_mailshake.streams import flatten_streams
        catalog = discover()
        self.assertEqual(len(catalog.streams), len(flatten_streams()))
