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
    except (MailshakeInvalidApiKeyError, MailshakeNotAuthorizedError) as exc:
        LOGGER.warning(
           "Excluding unauthorized stream '%s' from catalog. HTTP-Error-Message: '%s'",
            stream_name,
            exc,
        )
        return False


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> list:
    """Remove child streams from the catalog whose parent stream was excluded.

    Iterates the flat representation of all streams (parents + their children)
    and removes any child entry whose parent is no longer present in schemas.
    Mutates schemas and field_metadata in place.
    """
    flat_streams = flatten_streams()
    inaccessible_children = []

    removed_child = True
    while removed_child:
        removed_child = False
        for stream_name, stream_cfg in list(flat_streams.items()):
            parent = stream_cfg.get('parent')
            if stream_name in schemas and parent and parent not in schemas:
                schemas.pop(stream_name, None)
                field_metadata.pop(stream_name, None)
                inaccessible_children.append(stream_name)
                removed_child = True

    return inaccessible_children


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """Probe each top-level stream for read access and remove inaccessible streams
    (and their children) from schemas and field_metadata in place.

    Child streams are skipped during probing — their removal is handled separately
    by _prune_inaccessible_children().
    Raises MailshakeNotAuthorizedError if no streams remain accessible.
    """
    flat_streams = flatten_streams()

    inaccessible_streams = [
        stream_name
        for stream_name in schemas
        if not flat_streams.get(stream_name, {}).get('parent')
        and not check_stream_access(client, stream_name, STREAMS[stream_name])
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    inaccessible_children = _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise MailshakeNotAuthorizedError(
            "HTTP-error-code: 401, Error: The credentials do not have "
            "'read' access to any supported streams."
        )

    all_inaccessible = inaccessible_streams + [
        stream_name for stream_name in inaccessible_children if stream_name not in inaccessible_streams
    ]

    if all_inaccessible:
        LOGGER.warning("Unauthorized streams excluded from catalog: %s", ", ".join(all_inaccessible))


def discover(client) -> Catalog:
    """Run discovery mode, excluding streams the credentials cannot read.

    Access to each top-level stream is verified via check_stream_access().
    Streams that return an auth error are removed from the catalog.
    Child streams are included only if their parent stream is accessible.
    """
    schemas, field_metadata = get_schemas()
    _apply_access_checks(client, schemas, field_metadata)

    flat_streams = flatten_streams()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]
        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=flat_streams.get(stream_name, {}).get('key_properties', None),
            schema=schema,
            metadata=mdata,
        ))

    return catalog
