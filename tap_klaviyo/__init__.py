#!/usr/bin/env/python

import json
import os
import sys
import singer
from singer import metadata
from tap_klaviyo.utils import get_incremental_pull, get_full_pulls, get_all_using_next

LOGGER = singer.get_logger()

API_VERSION = "2024-10-15"

# For stream global_exclusions, data related to suppressed users can be found in the /api/profiles endpoint
ENDPOINTS = {
    'global_exclusions': 'https://a.klaviyo.com/api/profiles',
    'lists': 'https://a.klaviyo.com/api/lists',
    'metrics': 'https://a.klaviyo.com/api/metrics',
    'events': 'https://a.klaviyo.com/api/events',
    'campaigns': 'https://a.klaviyo.com/api/campaigns'
}

EVENT_MAPPINGS = {
    "Received Email": "receive",
    "Clicked Email": "click",
    "Opened Email": "open",
    "Bounced Email": "bounce",
    "Unsubscribed from Email Marketing": "unsubscribe",
    "Marked Email as Spam": "mark_as_spam",
    "Unsubscribed from List": "unsub_list",
    "Subscribed to Email Marketing": "subscribed_to_email",
    "Subscribed to List": "subscribe_list",
    "Updated Email Preferences": "update_email_preferences",
    "Dropped Email": "dropped_email",
    "Clicked SMS": "clicked_sms",
    "Subscribed to SMS Marketing": "subscribed_to_sms",
    "Failed to Deliver SMS": "failed_to_deliver",
    "Failed to deliver Automated Response SMS": "failed_to_deliver_automated_response",
    "Received Automated Response SMS": "received_automated_response",
    "Received SMS": "received_sms",
    "Sent SMS": "sent_sms",
    "Unsubscribed from SMS Marketing": "unsubscribed_from_sms"
}


class Stream(object):
    def __init__(self, stream, tap_stream_id, key_properties, replication_method, replication_keys=None):
        self.stream = stream
        self.tap_stream_id = tap_stream_id
        self.key_properties = key_properties
        self.replication_method = replication_method
        self.metadata = []
        self.replication_keys = replication_keys

    def to_catalog_dict(self):
        stream_schema = load_schema(self.stream)
        
        # Load shared schema
        refs = load_shared_schema_refs()

        # Resolve shared schema
        resolved_schema = singer.resolve_schema_references(stream_schema, refs)
        mdata = metadata.to_map(
            metadata.get_standard_metadata(
                schema = resolved_schema,
                key_properties = self.key_properties,
                valid_replication_keys=self.replication_keys, # Add replication key in the metadata of catalog
                replication_method = self.replication_method
            )
        )

        if self.replication_method == 'INCREMENTAL':
            mdata = metadata.write(mdata, ('properties', 'timestamp'), 'inclusion', 'automatic')
        self.metadata = metadata.to_list(mdata)

        return {
            'stream': self.stream,
            'tap_stream_id': self.tap_stream_id,
            'key_properties': self.key_properties,
            'schema': resolved_schema,
            'metadata': self.metadata
        }

CREDENTIALS_KEYS = ["api_key"]
REQUIRED_CONFIG_KEYS = ["start_date"] + CREDENTIALS_KEYS

GLOBAL_EXCLUSIONS = Stream(
    'global_exclusions',
    'global_exclusions',
    ['id'],
    'FULL_TABLE'
)

LISTS = Stream(
    'lists',
    'lists',
    ['id'],
    'FULL_TABLE'
)

CAMPAIGNS = Stream(
    'campaigns',
    'campaigns',
    ['id'],
    'FULL_TABLE'
)

FULL_STREAMS = [GLOBAL_EXCLUSIONS, LISTS, CAMPAIGNS]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(name):
    return json.load(open(get_abs_path('schemas/{}.json'.format(name))))

def load_shared_schema_refs():
    # Get shared schema path
    shared_schemas_path = get_abs_path('schemas/shared')

    # Get shared schema file name
    shared_file_names = [f for f in os.listdir(shared_schemas_path)
                         if os.path.isfile(os.path.join(shared_schemas_path, f))]

    shared_schema_refs = {}
    for shared_file in shared_file_names:
        with open(os.path.join(shared_schemas_path, shared_file)) as data_file:
            shared_schema_refs['shared/' + shared_file] = json.load(data_file)

    return shared_schema_refs

def do_sync(config, state, catalog, headers):
    start_date = config['start_date'] if 'start_date' in config else None

    for stream in catalog.get('streams'):
        mdata = metadata.to_map(stream['metadata'])
        if metadata.get(mdata, (), 'selected'):
            singer.write_schema(
                stream['stream'],
                stream['schema'],
                stream['key_properties']
            )

            if stream['stream'] in EVENT_MAPPINGS.values():
                get_incremental_pull(stream, ENDPOINTS['events'], state,
                                    headers, start_date)
            else:
                get_full_pulls(stream, ENDPOINTS[stream['stream']], headers)


def get_available_metrics(headers):
    metric_streams = []
    for response in get_all_using_next('metric_list',
                                  ENDPOINTS['metrics'], headers, {}):
        for metric in response.json().get('data'):
            if metric['attributes']['name'] in EVENT_MAPPINGS:
                metric_streams.append(
                    Stream(
                        stream=EVENT_MAPPINGS[metric['attributes']['name']],
                        tap_stream_id=metric['id'],
                        key_properties=["id"],
                        replication_method='INCREMENTAL',
                        replication_keys=["timestamp"]
                    )
                )

    return metric_streams


def discover(headers):
    metric_streams = get_available_metrics(headers)
    return {"streams": [a.to_catalog_dict()
                        for a in metric_streams + FULL_STREAMS]}


def do_discover(headers):
    print(json.dumps(discover(headers), indent=2))

@singer.utils.handle_top_exception(LOGGER)
def main():

    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    headers = {
        "Authorization": f"Klaviyo-API-Key {args.config.get('api_key')}",
        "revision": API_VERSION
    }

    if args.discover:
        do_discover(headers)

    else:
        catalog = args.catalog.to_dict() if args.catalog else discover(headers)

        state = args.state if args.state else {"bookmarks": {}}
        do_sync(args.config, state, catalog, headers)

if __name__ == '__main__':
    main()
