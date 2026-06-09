import unittest
from tap_mailshake.transform import (
    enrich_recipients, enrich_campaigns, reformat_keys, transform_data
)


class TestEnrichRecipients(unittest.TestCase):

    def test_adds_campaign_id_to_record(self):
        record = {'id': 1, 'email': 'a@b.com'}
        enrich_recipients(record, parent_id=42)
        self.assertEqual(record['campaignId'], 42)

    def test_overwrites_existing_campaign_id(self):
        record = {'id': 1, 'campaignId': 99}
        enrich_recipients(record, parent_id=7)
        self.assertEqual(record['campaignId'], 7)

    def test_does_not_modify_other_fields(self):
        record = {'id': 1, 'email': 'x@y.com'}
        enrich_recipients(record, parent_id=5)
        self.assertEqual(record['email'], 'x@y.com')


class TestEnrichCampaigns(unittest.TestCase):

    def test_adds_campaign_id_to_each_message(self):
        record = {'id': 10, 'messages': [{'subject': 'Hi'}, {'subject': 'Follow up'}]}
        enrich_campaigns(record)
        for msg in record['messages']:
            self.assertEqual(msg['campaignId'], 10)

    def test_empty_messages_list_no_error(self):
        record = {'id': 5, 'messages': []}
        enrich_campaigns(record)  # should not raise

    def test_missing_messages_key_no_error(self):
        record = {'id': 5}
        enrich_campaigns(record)  # messages defaults to []

    def test_missing_id_sets_none(self):
        record = {'messages': [{'subject': 'Hi'}]}
        enrich_campaigns(record)
        self.assertIsNone(record['messages'][0]['campaignId'])


class TestReformatKeys(unittest.TestCase):

    def test_spaces_replaced_with_underscores(self):
        record = {'fields': {'First Name': 'Alice'}}
        reformat_keys(record)
        self.assertIn('First_Name', record['fields'])

    def test_parentheses_removed(self):
        record = {'fields': {'Size (MB)': '10'}}
        reformat_keys(record)
        self.assertIn('Size_MB', record['fields'])

    def test_empty_key_renamed_to_blank(self):
        record = {'fields': {'': 'value'}}
        reformat_keys(record)
        self.assertIn('blank', record['fields'])
        self.assertNotIn('', record['fields'])

    def test_no_fields_key_no_error(self):
        record = {}
        reformat_keys(record)  # should not raise

    def test_normal_key_unchanged(self):
        record = {'fields': {'email': 'a@b.com'}}
        reformat_keys(record)
        self.assertIn('email', record['fields'])


class TestTransformData(unittest.TestCase):

    def test_recipients_get_campaign_id(self):
        data = [{'id': 1}]
        result = transform_data(data, 'recipients', parent_id=99)
        self.assertEqual(result[0]['campaignId'], 99)

    def test_campaigns_messages_enriched(self):
        data = [{'id': 5, 'messages': [{'subject': 'Test'}]}]
        result = transform_data(data, 'campaigns')
        self.assertEqual(result[0]['messages'][0]['campaignId'], 5)

    def test_other_stream_no_enrichment(self):
        data = [{'id': 1, 'email': 'a@b.com'}]
        result = transform_data(data, 'leads')
        self.assertNotIn('campaignId', result[0])

    def test_returns_same_list_reference(self):
        data = [{'id': 1}]
        result = transform_data(data, 'leads')
        self.assertIs(result, data)

    def test_reformat_keys_applied_for_all_streams(self):
        data = [{'id': 1, 'fields': {'First Name': 'Bob'}}]
        result = transform_data(data, 'leads')
        self.assertIn('First_Name', result[0]['fields'])
