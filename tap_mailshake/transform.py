
def enrich_recipients(record, parent_id):
    record['campaignId'] = parent_id


def enrich_campaigns(record):
    messages = record.get('messages', [])
    for message in messages:
        message['campaignId'] = record.get('id', None)


def reformat_keys(record):
    fields = record.get('fields', {})
    for key in list(fields.keys()):
        if key == '':
            fields['blank'] = fields.pop(key)
        else:
            new_key = key.replace(' ', '_').replace('(', '').replace(')', '')
            fields[new_key] = fields.pop(key)


def transform_data(data, stream_name, parent_id=None):
    for record in data:
        if stream_name in ('recipients', 'campaigns'):
            if stream_name == 'recipients':
                enrich_recipients(record, parent_id)
            else:
                enrich_campaigns(record)

        reformat_keys(record)
    return data
