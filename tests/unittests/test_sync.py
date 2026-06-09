import unittest
from unittest.mock import patch, MagicMock, call
import singer
from tap_mailshake.sync import (
    get_bookmark, write_bookmark, transform_datetime
)


class TestGetBookmark(unittest.TestCase):

    def test_returns_default_when_state_is_none(self):
        result = get_bookmark(None, 'leads', '2024-01-01T00:00:00Z')
        self.assertEqual(result, '2024-01-01T00:00:00Z')

    def test_returns_default_when_no_bookmarks_key(self):
        result = get_bookmark({}, 'leads', '2024-01-01T00:00:00Z')
        self.assertEqual(result, '2024-01-01T00:00:00Z')

    def test_returns_default_when_stream_not_in_bookmarks(self):
        state = {'bookmarks': {'campaigns': '2024-06-01T00:00:00Z'}}
        result = get_bookmark(state, 'leads', '2024-01-01T00:00:00Z')
        self.assertEqual(result, '2024-01-01T00:00:00Z')

    def test_returns_bookmark_value_when_present(self):
        state = {'bookmarks': {'leads': '2024-06-01T00:00:00Z'}}
        result = get_bookmark(state, 'leads', '2024-01-01T00:00:00Z')
        self.assertEqual(result, '2024-06-01T00:00:00Z')

    def test_returns_none_bookmark_over_default(self):
        state = {'bookmarks': {'leads': None}}
        result = get_bookmark(state, 'leads', '2024-01-01T00:00:00Z')
        self.assertIsNone(result)


class TestWriteBookmark(unittest.TestCase):

    @patch('singer.write_state')
    def test_creates_bookmarks_key_when_absent(self, mock_write_state):
        state = {}
        write_bookmark(state, 'leads', '2024-06-01T00:00:00Z')
        self.assertIn('bookmarks', state)

    @patch('singer.write_state')
    def test_writes_value_to_correct_stream(self, mock_write_state):
        state = {}
        write_bookmark(state, 'leads', '2024-06-01T00:00:00Z')
        self.assertEqual(state['bookmarks']['leads'], '2024-06-01T00:00:00Z')

    @patch('singer.write_state')
    def test_preserves_existing_bookmarks(self, mock_write_state):
        state = {'bookmarks': {'campaigns': '2024-05-01T00:00:00Z'}}
        write_bookmark(state, 'leads', '2024-06-01T00:00:00Z')
        self.assertEqual(state['bookmarks']['campaigns'], '2024-05-01T00:00:00Z')

    @patch('singer.write_state')
    def test_calls_write_state_with_updated_state(self, mock_write_state):
        state = {}
        write_bookmark(state, 'leads', '2024-06-01T00:00:00Z')
        mock_write_state.assert_called_once_with(state)

    @patch('singer.write_state')
    def test_overwrites_existing_bookmark(self, mock_write_state):
        state = {'bookmarks': {'leads': '2024-01-01T00:00:00Z'}}
        write_bookmark(state, 'leads', '2024-12-01T00:00:00Z')
        self.assertEqual(state['bookmarks']['leads'], '2024-12-01T00:00:00Z')


class TestTransformDatetime(unittest.TestCase):

    def test_transforms_valid_datetime_string(self):
        result = transform_datetime('2024-01-15T10:30:00Z')
        self.assertIsNotNone(result)

    def test_returns_string_output(self):
        result = transform_datetime('2024-01-15T10:30:00Z')
        self.assertIsInstance(result, str)

    def test_transforms_date_only_string(self):
        result = transform_datetime('2024-01-15')
        self.assertIsNotNone(result)


class TestWriteSchema(unittest.TestCase):

    @patch('singer.write_schema')
    def test_write_schema_calls_singer_write_schema(self, mock_write_schema):
        from tap_mailshake.sync import write_schema
        from tap_mailshake.discover import discover
        catalog = discover()
        write_schema(catalog, 'leads')
        self.assertTrue(mock_write_schema.called)
        args = mock_write_schema.call_args[0]
        self.assertEqual(args[0], 'leads')

    @patch('singer.write_schema')
    def test_write_schema_raises_on_os_error(self, mock_write_schema):
        from tap_mailshake.sync import write_schema
        from tap_mailshake.discover import discover
        mock_write_schema.side_effect = OSError("disk full")
        catalog = discover()
        with self.assertRaises(OSError):
            write_schema(catalog, 'leads')


class TestWriteRecord(unittest.TestCase):

    @patch('singer.messages.write_record')
    def test_write_record_calls_singer(self, mock_write_record):
        from tap_mailshake.sync import write_record
        import singer
        time_extracted = singer.utils.now()
        write_record('leads', {'id': 1}, time_extracted)
        self.assertTrue(mock_write_record.called)

    @patch('singer.messages.write_record')
    def test_write_record_raises_on_os_error(self, mock_write_record):
        from tap_mailshake.sync import write_record
        import singer
        mock_write_record.side_effect = OSError("disk full")
        with self.assertRaises(OSError):
            write_record('leads', {'id': 1}, singer.utils.now())
