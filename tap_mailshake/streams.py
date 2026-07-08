from __future__ import annotations

from typing import TypedDict


class StreamConfig(TypedDict, total=False):
    path: str
    key_properties: list[str]
    replication_method: str
    replication_keys: list[str]
    data_key: str
    params: dict
    parent: str
    children: dict[str, StreamConfig]


STREAMS: dict[str, StreamConfig] = {
    'campaigns': {
        'path': 'campaigns/list',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['created'],
        'data_key': 'results',
        'children': {
            'recipients': {
                'path': 'recipients/list',
                'key_properties': ['id'],
                'replication_method': 'INCREMENTAL',
                'replication_keys': ['created'],
                'parent': 'campaigns',
                'params': {'campaignID': '<parent_id>'},
                'data_key': 'results'
            }
        },
    },
    'leads': {
        'path': 'leads/list',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['created'],
        'data_key': 'results'
    },
    'senders': {
        'path': 'senders/list',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['created'],
        'data_key': 'results'
    },
    'team_members': {
        'path': 'team/list-members',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'replication_keys': [],
        'data_key': 'results'
    },
    'sent_messages': {
        'path': 'activity/sent',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['actionDate'],
        'data_key': 'results'
    },
    'opens': {
        'path': 'activity/opens',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['actionDate'],
        'data_key': 'results'
    },
    'clicks': {
        'path': 'activity/clicks',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['actionDate'],
        'data_key': 'results'
    },
    'replies': {
        'path': 'activity/replies',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['actionDate'],
        'data_key': 'results'
    },
}


def flatten_streams() -> dict[str, StreamConfig]:
    """Returns a flat_streams  dict of all streams (parents + children) keyed by stream name."""
    flat_streams : dict[str, StreamConfig] = {}
    for stream_name, endpoint_config  in STREAMS.items():
        flat_streams[stream_name] = {
            'key_properties': endpoint_config.get('key_properties'),
            'replication_method': endpoint_config.get('replication_method'),
            'replication_keys': endpoint_config.get('replication_keys'),
            'parent': endpoint_config.get('parent'),
        }
        for child_name, child_enpoint_config  in endpoint_config.get('children', {}).items():
            flat_streams[child_name] = {
                'key_properties': child_enpoint_config.get('key_properties'),
                'replication_method': child_enpoint_config.get('replication_method'),
                'replication_keys': child_enpoint_config.get('replication_keys'),
                'parent': stream_name,
            }
    return flat_streams
