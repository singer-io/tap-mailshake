# streams: API URL endpoints to be called
# properties:
#   <root node>: Plural stream name for the endpoint
#   path: API endpoint relative path, when added to the base URL, creates the full path,
#       default = stream_name
#   key_properties: Primary key fields for identifying an endpoint record.
#   replication_method: INCREMENTAL or FULL_TABLE
#   replication_keys: bookmark_field(s), typically a date-time, used for filtering the results
#        and setting the state
#   params: Query, sort, and other endpoint specific parameters; default = {}
#   data_key: JSON element containing the results list for the endpoint;
#        default = root (no data_key)
#   bookmark_query_field: From date-time field used for filtering the query
#   bookmark_type: Data type for bookmark, integer or datetime


STREAMS = {
    'campaigns': {
        'path': 'campaigns/list',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['created'],
        'params': {
        },
        'data_key': 'results',
        'children': {
            'recipients': {
                'path': 'recipients/list',
                'key_properties': ['id'],
                'replication_method': 'INCREMENTAL',
                'replication_keys': ['created'],
                'params': {
                    'campaignID': '<parent_id>'
                },
                'data_key': 'results'
            }
        }
    },
    'leads': {
        'path': 'leads/list',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['created'],
        'params': {
        },
        'data_key': 'results'
    },
    'senders': {
        'path': 'senders/list',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['created'],
        'params': {
        },
        'data_key': 'results'
    },
    'team_members': {
        'path': 'team/list-members',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['team_id'],
        'params': {
        },
        'data_key': 'results'
    },
    'sent_messages': {
        'path': 'activity/sent',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['actionDate'],
        'params': {
        },
        'data_key': 'results'
    },
    'opens': {
        'path': 'activity/opens',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['actionDate'],
        'params': {
        },
        'data_key': 'results'
    },
    'clicks': {
        'path': 'activity/clicks',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['actionDate'],
        'params': {
        },
        'data_key': 'results'
    },
    'replies': {
        'path': 'activity/replies',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['actionDate'],
        'params': {
        },
        'data_key': 'results'
    }
}


def flatten_streams():
    flat_streams = {}
    # Loop through parents
    for stream_name, endpoint_config in STREAMS.items():
        flat_streams[stream_name] = {
            'key_properties': endpoint_config.get('key_properties'),
            'replication_method': endpoint_config.get('replication_method'),
            'replication_keys': endpoint_config.get('replication_keys')
        }
        # Loop through children
        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_enpoint_config in children.items():
                flat_streams[child_stream_name] = {
                    'key_properties': child_enpoint_config.get('key_properties'),
                    'replication_method': child_enpoint_config.get('replication_method'),
                    'replication_keys': child_enpoint_config.get('replication_keys')
                }
    return flat_streams
