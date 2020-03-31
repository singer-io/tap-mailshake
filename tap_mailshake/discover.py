import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_mailshake.schema import get_schemas
from tap_mailshake.streams import flatten_streams

LOGGER = singer.get_logger()


def discover():
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    flat_streams = flatten_streams()
    for stream_name, schema_dict in schemas.items():
        LOGGER.info('discover schema for stream: {}'.format(stream_name))
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]
        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=flat_streams.get(stream_name, {}).get('key_properties', None),
            schema=schema,
            metadata=mdata
        ))

    LOGGER.info('Returning catalog: {}'.format(catalog))
    return catalog
