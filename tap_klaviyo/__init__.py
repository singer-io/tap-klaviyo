import argparse
import requests
import json
import os
import singer
import singer.metrics as metrics

from utils import (
    dt_to_ts, ts_to_dt, update_state
)


session = requests.Session()
logger = singer.get_logger()

METRICS_ENDPOINT = 'https://a.klaviyo.com/api/v1/metrics'
METRIC_ENDPOINT = 'https://a.klaviyo.com/api/v1/metric/'
LISTS_ENDPOINT = 'https://a.klaviyo.com/api/v1/lists'
GLOBAL_EXCLUSIONS = 'https://a.klaviyo.com/api/v1/people/exclusions'


def authed_get(source, url):
    with metrics.http_request_timer(source) as timer:
        resp = session.request(method='get', url=url)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        return resp


def authed_get_all_using_next(source, url, since=None):
    while True:
        r = authed_get(source, '{}&since={}'.format(
            url, since) if since else url)
        yield r
        if 'next' in r.json() and r.json()['next']:
            since = r.json()['next']
        else:
            break


def authed_get_all_pages(source, url):
    page = 0
    while True:
        r = authed_get(source, '{}&page={}'.format(
            url, page))
        yield r
        if r.json()['end'] < r.json()['total'] - 1:
            page += 1
        else:
            break


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas(events, lists, exclusions):
    tables = events + lists + exclusions
    schemas = {}

    for table in tables:
        with open(get_abs_path('schemas/{}.json'.format(table['name']))) as file:
            schemas[table['name']] = json.load(file)

    return schemas


def get_starting_point(event, state, start_date):
    if event['name'] in state and state[event['name']] is not None:
        return dt_to_ts(state[event['name']])
    elif start_date:
        return dt_to_ts(start_date)
    else:
        return None


def get_latest_event_time(events):
    return ts_to_dt(int(events[-1]['timestamp'])) if len(events) else None


def get_events_by_type(event, state, api_key, start_date):
    latest_event_time = get_starting_point(event, state, start_date)

    with metrics.record_counter(event['name']) as counter:
        url = '{}{}/timeline?api_key={}&sort=asc'.format(
            METRIC_ENDPOINT,
            event['id'],
            api_key
        )
        for response in authed_get_all_using_next(
                event['name'], url, latest_event_time):
            events = response.json().get('data')

            counter.increment(len(events))

            singer.write_records(event['name'], events)

            update_state(state, event['name'], get_latest_event_time(events))
            singer.write_state(state)

            logger.info('Replicated events up to %s', ts_to_dt(latest_event_time))

    return state


def get_full_pulls(resource, endpoint, api_key):
    with metrics.record_counter(resource['name']) as counter:
        url = '{}?api_key={}'.format(
            endpoint,
            api_key
        )
        for response in authed_get_all_pages(resource['name'], url):
            records = response.json().get('data')

            counter.increment(len(records))

            singer.write_records(resource['name'], records)


def do_sync(config, state):
    api_key = config['api_key']
    start_date = config['start_date'] if 'start_date' in config else None
    events = config['events'] if 'events' in config else None
    lists = config.get('lists', [])
    exclusions = config.get('exclusions', [])
    schemas = load_schemas(events, lists, exclusions)

    for list in lists:
        singer.write_schema(list['name'], schemas[list['name']],
                            list['primary_key'])
        get_full_pulls(list, LISTS_ENDPOINT, api_key)

    for group in exclusions:
        singer.write_schema(
            group['name'],
            schemas[group['name']],
            group['primary_key']
        )
        get_full_pulls(group, GLOBAL_EXCLUSIONS, api_key)

    for event in events:

        if state:
            logger.info('Replicating event since %s', state[event['name']])
        else:
            logger.info('Replicating event since %s', start_date)

        singer.write_schema(event['name'], schemas[event['name']], event['primary_key'])
        state = get_events_by_type(event, state, api_key, start_date)


def get_available_metrics(api_key):
    """
    Lists available event metrics that can synced with ID for config
    """
    resp = authed_get('metric_list', '{}?api_key={}'.format(
        METRICS_ENDPOINT,
        api_key
    ))
    metric_list = resp.json()
    print(json.dumps(metric_list))


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')
    parser.add_argument(
        '-d', '--discover', help='Discover available metrics')
    # parser.add_argument(
    #     '-c', '--catalog', help='Sync the metrics provided in the catalog')

    args = parser.parse_args()

    with open(args.config) as config_file:
        config = json.load(config_file)

    if 'api_key' not in config:
        logger.fatal("Missing api_key")
        exit(1)

    if args.discover:
        get_available_metrics(config['api_key'])
        return

    if len(config['events']) < 1:
        logger.fatal("No events to track")
        exit(1)

    state = {}
    if args.state:
        with open(args.state, 'r') as state_file:
            for line in state_file:
                state = json.loads(line.strip())

    do_sync(config, state)

if __name__ == '__main__':
    main()
