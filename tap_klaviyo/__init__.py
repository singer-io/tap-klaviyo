#!/usr/bin/env/python

import json
import os
import sys
import singer
from singer import metadata
from tap_klaviyo.utils import get_incremental_pull, get_full_pulls, get_all_pages

LOGGER = singer.get_logger()

ENDPOINTS = {
    'global_exclusions': 'https://a.klaviyo.com/api/v1/people/exclusions',
    'lists': 'https://a.klaviyo.com/api/v1/lists',
    'metrics': 'https://a.klaviyo.com/api/v1/metrics',
    'metric': 'https://a.klaviyo.com/api/v1/metric/',
    'campaigns': 'https://a.klaviyo.com/api/v1/campaigns'
}

EVENT_MAPPINGS = {
    "Received Email": "receive",
    "Clicked Email": "click",
    "Opened Email": "open",
    "Bounced Email": "bounce",
    "Unsubscribed": "unsubscribe",
    "Marked Email as Spam": "mark_as_spam",
    "Unsubscribed from List": "unsub_list",
    "Subscribed to List": "subscribe_list",
    "Updated Email Preferences": "update_email_preferences",
    "Dropped Email": "dropped_email",
}


class Stream(object):
    def __init__(self, stream, tap_stream_id, key_properties, replication_method):
        self.stream = stream
        self.tap_stream_id = tap_stream_id
        self.key_properties = key_properties
        self.replication_method = replication_method
        self.metadata = []

    def to_catalog_dict(self):
        schema = load_schema(self.stream)
        mdata = metadata.to_map(
            metadata.get_standard_metadata(
                schema = schema,
                key_properties = self.key_properties,
                replication_method = self.replication_method
            )
        )

        if self.replication_method == 'INCREMENTAL':
            mdata = metadata.write(mdata, ('properties', 'timestamp'), 'inclusion', 'automatic')
        self.metadata = metadata.to_list(mdata)

        return {
            'stream': self.stream,
            'tap_stream_id': self.tap_stream_id,
            'key_properties': [self.key_properties],
            'schema': schema,
            'metadata': self.metadata
        }

CREDENTIALS_KEYS = ["api_key"]
REQUIRED_CONFIG_KEYS = ["start_date"] + CREDENTIALS_KEYS

GLOBAL_EXCLUSIONS = Stream(
    'global_exclusions',
    'global_exclusions',
    'email',
    'FULL_TABLE'
)

LISTS = Stream(
    'lists',
    'lists',
    'id',
    'FULL_TABLE'
)

CAMPAIGNS = Stream(
    'campaigns',
    'campaigns',
    'id',
    'FULL_TABLE'
)

FULL_STREAMS = [GLOBAL_EXCLUSIONS, LISTS, CAMPAIGNS]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(name):
    return json.load(open(get_abs_path('schemas/{}.json'.format(name))))


def do_sync(config, state, catalog):
    api_key = config['api_key']
    start_date = config['start_date'] if 'start_date' in config else None

    stream_ids_to_sync = set()

    for stream in catalog.get('streams'):
        mdata = metadata.to_map(stream['metadata'])
        if metadata.get(mdata, (), 'selected'):
            stream_ids_to_sync.add(stream['tap_stream_id'])

    for stream in catalog['streams']:
        if stream['tap_stream_id'] not in stream_ids_to_sync:
            continue
        singer.write_schema(
            stream['stream'],
            stream['schema'],
            stream['key_properties']
        )

        if stream['stream'] in EVENT_MAPPINGS.values():
            get_incremental_pull(stream, ENDPOINTS['metric'], state,
                                 api_key, start_date)
        else:
            get_full_pulls(stream, ENDPOINTS[stream['stream']], api_key)


def get_available_metrics(api_key):
    metric_streams = []
    for response in get_all_pages('metric_list',
                                  ENDPOINTS['metrics'], api_key):
        for metric in response.json().get('data'):
            if metric['name'] in EVENT_MAPPINGS:
                metric_streams.append(
                    Stream(
                        stream=EVENT_MAPPINGS[metric['name']],
                        tap_stream_id=metric['id'],
                        key_properties="id",
                        replication_method='INCREMENTAL'
                    )
                )

    return metric_streams


def discover(api_key):
    metric_streams = get_available_metrics(api_key)
    return {"streams": [a.to_catalog_dict()
                        for a in metric_streams + FULL_STREAMS]}


def do_discover(api_key):
    print(json.dumps(discover(api_key), indent=2))

@singer.utils.handle_top_exception(LOGGER)
def main():

    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config['api_key'])

    else:
        catalog = args.catalog.to_dict() if args.catalog else discover(
             args.config['api_key'])

        state = args.state if args.state else {"bookmarks": {}}
        do_sync(args.config, state, catalog)

if __name__ == '__main__':
    main()
