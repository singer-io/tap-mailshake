import unittest
from tap_mailshake.schema import get_schemas, get_abs_path
from tap_mailshake.streams import flatten_streams


class TestGetAbsPath(unittest.TestCase):

    def test_returns_a_string(self):
        self.assertIsInstance(get_abs_path('schemas'), str)

    def test_path_ends_with_suffix(self):
        result = get_abs_path('schemas')
        self.assertTrue(result.endswith('schemas'))


class TestGetSchemas(unittest.TestCase):

    def setUp(self):
        self.schemas, self.field_metadata = get_schemas()
        self.flat_streams = flatten_streams()

    def test_returns_dict_for_schemas(self):
        self.assertIsInstance(self.schemas, dict)

    def test_returns_dict_for_field_metadata(self):
        self.assertIsInstance(self.field_metadata, dict)

    def test_schema_keys_match_flat_streams(self):
        self.assertEqual(set(self.schemas.keys()), set(self.flat_streams.keys()))

    def test_metadata_keys_match_flat_streams(self):
        self.assertEqual(set(self.field_metadata.keys()), set(self.flat_streams.keys()))

    def test_each_schema_has_properties(self):
        for stream_name, schema in self.schemas.items():
            self.assertIn('properties', schema,
                          msg=f"Schema for '{stream_name}' missing 'properties'")

    def test_each_schema_has_type(self):
        for stream_name, schema in self.schemas.items():
            self.assertIn('type', schema,
                          msg=f"Schema for '{stream_name}' missing 'type'")

    def test_campaigns_schema_loaded(self):
        self.assertIn('campaigns', self.schemas)

    def test_metadata_is_list(self):
        for stream_name, mdata in self.field_metadata.items():
            self.assertIsInstance(mdata, list,
                                  msg=f"Metadata for '{stream_name}' should be a list")

    def test_schema_properties_is_dict(self):
        for stream_name, schema in self.schemas.items():
            self.assertIsInstance(schema['properties'], dict,
                                  msg=f"properties for '{stream_name}' should be a dict")
