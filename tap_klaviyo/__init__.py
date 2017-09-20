#!/usr/bin/env/python

import json
import os
import singer

from tap_klaviyo.utils import get_incremental_pull, get_full_pulls, get_all_pages

ENDPOINTS = {
    'global_exclusions': 'https://a.klaviyo.com/api/v1/people/exclusions',
    'lists': 'https://a.klaviyo.com/api/v1/lists',
    'metrics': 'https://a.klaviyo.com/api/v1/metrics',
    'metric': 'https://a.klaviyo.com/api/v1/metric/',
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
    def __init__(self, stream, tap_stream_id, key_properties, puller):
        self.stream = stream
        self.tap_stream_id = tap_stream_id
        self.key_properties = key_properties
        self.puller = puller

    def to_catalog_dict(self):
        return {
            'stream': self.stream,
            'tap_stream_id': self.tap_stream_id,
            'key_properties': self.key_properties,
            'schema': load_schema(self.stream)
        }

CREDENTIALS_KEYS = ["api_key"]
REQUIRED_CONFIG_KEYS = ["start_date"] + CREDENTIALS_KEYS

GLOBAL_EXCLUSIONS = Stream(
    'global_exclusions',
    'global_exclusions',
    'email',
    'full'
)

LISTS = Stream(
    'lists',
    'lists',
    'uuid',
    'lists'
)

FULL_STREAMS = [GLOBAL_EXCLUSIONS, LISTS]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(name):
    return json.load(open(get_abs_path('schemas/{}.json'.format(name))))


def do_sync(config, state, catalog):
    api_key = config['api_key']
    start_date = config['start_date'] if 'start_date' in config else None

    for stream in catalog['streams']:
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
                        puller='incremental'
                    )
                )

    return metric_streams


def discover(api_key):
    metric_streams = get_available_metrics(api_key)
    return {"streams": [a.to_catalog_dict()
                        for a in metric_streams + FULL_STREAMS]}


def do_discover(api_key):
    print(json.dumps(discover(api_key), indent=4))


def main():

    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config['api_key'])
        exit(1)

    else:
        catalog = args.catalog.to_dict() if args.catalog else discover(
            args.config['api_key'])
        state = args.state if args.state else {"bookmarks": {}}
        do_sync(args.config, state, catalog)


if __name__ == '__main__':
    main()
