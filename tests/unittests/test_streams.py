import unittest
from tap_mailshake.streams import STREAMS, flatten_streams


class TestStreamsStructure(unittest.TestCase):
    """Tests for the STREAMS dictionary structure."""

    def test_streams_is_a_dict(self):
        self.assertIsInstance(STREAMS, dict)

    def test_expected_top_level_streams_present(self):
        expected = {'campaigns', 'leads', 'senders', 'team_members',
                    'sent_messages', 'opens', 'clicks', 'replies'}
        self.assertEqual(set(STREAMS.keys()), expected)

    def test_campaigns_has_children(self):
        self.assertIn('children', STREAMS['campaigns'])
        self.assertIn('recipients', STREAMS['campaigns']['children'])

    def test_each_stream_has_required_keys(self):
        required = {'path', 'key_properties', 'replication_method', 'data_key'}
        for name, config in STREAMS.items():
            for key in required:
                self.assertIn(key, config, msg=f"Stream '{name}' missing key '{key}'")

    def test_incremental_streams_have_replication_keys(self):
        for name, config in STREAMS.items():
            if config.get('replication_method') == 'INCREMENTAL':
                self.assertTrue(
                    config.get('replication_keys'),
                    msg=f"INCREMENTAL stream '{name}' missing replication_keys"
                )

    def test_all_key_properties_are_lists(self):
        for name, config in STREAMS.items():
            self.assertIsInstance(
                config['key_properties'], list,
                msg=f"'{name}' key_properties should be a list"
            )

    def test_campaigns_full_table(self):
        self.assertEqual(STREAMS['campaigns']['replication_method'], 'FULL_TABLE')

    def test_leads_incremental(self):
        self.assertEqual(STREAMS['leads']['replication_method'], 'INCREMENTAL')

    def test_recipients_child_has_campaign_id_param(self):
        recipient = STREAMS['campaigns']['children']['recipients']
        self.assertIn('campaignID', recipient['params'])


class TestFlattenStreams(unittest.TestCase):
    """Tests for flatten_streams()."""

    def setUp(self):
        self.flat = flatten_streams()

    def test_flatten_streams_returns_dict(self):
        self.assertIsInstance(self.flat, dict)

    def test_top_level_streams_are_present(self):
        for name in STREAMS:
            self.assertIn(name, self.flat)

    def test_recipients_child_is_flattened(self):
        self.assertIn('recipients', self.flat)

    def test_flat_stream_has_key_properties(self):
        for name, config in self.flat.items():
            self.assertIn('key_properties', config,
                          msg=f"Flat stream '{name}' missing key_properties")

    def test_flat_stream_has_replication_method(self):
        for name, config in self.flat.items():
            self.assertIn('replication_method', config)

    def test_flat_stream_has_replication_keys(self):
        for name, config in self.flat.items():
            self.assertIn('replication_keys', config)

    def test_total_flat_stream_count(self):
        # 8 top-level + 1 child (recipients)
        self.assertEqual(len(self.flat), 9)

    def test_campaigns_key_properties_preserved(self):
        self.assertEqual(self.flat['campaigns']['key_properties'], ['id'])

    def test_recipients_replication_method_preserved(self):
        self.assertEqual(self.flat['recipients']['replication_method'], 'INCREMENTAL')
