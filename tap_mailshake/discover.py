import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_mailshake.schema import get_schemas
from tap_mailshake.streams import STREAMS, flatten_streams
from tap_mailshake.client import MailshakeInvalidApiKeyError, MailshakeNotAuthorizedError

LOGGER = singer.get_logger()


def check_stream_access(client, stream_name, stream_config) -> bool:
    """
    Probes a top-level stream endpoint using the same method and params as
    sync_endpoint() — GET with perPage=1 as a query string — to verify the
    API key has access to that stream.
    Returns True if accessible, False on auth errors. Any other exception is re-raised.
    Should only be called for top-level streams (those without a 'parent' key).
    """
    path = stream_config['path']
    url = '{}/{}'.format(client.base_url, path)
    LOGGER.info("Checking access for stream '%s' at path '%s'", stream_name, path)
    try:
        client.get(url=url, path=path, params='perPage=1', endpoint=stream_name)
        return True
    except (MailshakeInvalidApiKeyError, MailshakeNotAuthorizedError):
        return False


def discover(client) -> Catalog:
    """Run discovery mode, probing each top-level stream endpoint to verify access.
    Streams that return an auth error are excluded from the catalog.
    Child streams are included only if their parent stream is accessible.
    """
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])
    accessible_streams = set()

    flat_streams = flatten_streams()

    # Separate top-level and child streams so parents are always probed first.
    top_level = {name: cfg for name, cfg in STREAMS.items()}
    children = {}
    child_to_parent = {}
    for parent_name, parent_cfg in STREAMS.items():
        for child_name, child_cfg in parent_cfg.get('children', {}).items():
            children[child_name] = child_cfg
            child_to_parent[child_name] = parent_name

    def _add_stream(stream_name, schema_dict):
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]
        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=flat_streams.get(stream_name, {}).get('key_properties', None),
            schema=schema,
            metadata=mdata
        ))
        accessible_streams.add(stream_name)

    # First pass: top-level streams
    for stream_name, stream_config in top_level.items():
        if stream_name not in schemas:
            continue
        if not check_stream_access(client, stream_name, stream_config):
            LOGGER.warning(
                "Stream '%s' will be excluded from the catalog due to insufficient permissions.",
                stream_name,
            )
            continue
        _add_stream(stream_name, schemas[stream_name])

    # Second pass: child streams (only if parent is accessible)
    for stream_name, stream_config in children.items():
        if stream_name not in schemas:
            continue
        parent_name = child_to_parent.get(stream_name)
        if parent_name not in accessible_streams:
            LOGGER.warning(
                "Stream '%s' will be excluded from the catalog because its "
                "parent stream '%s' is not accessible.",
                stream_name,
                parent_name,
            )
            continue
        _add_stream(stream_name, schemas[stream_name])

    if not catalog.streams:
        raise Exception(
            "The credentials do not have read access to any of the supported streams. "
            "Verify that the API key is valid and has the required permissions."
        )

    return catalog
