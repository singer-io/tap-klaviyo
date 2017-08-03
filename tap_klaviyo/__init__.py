import argparse
import requests
import singer
import json
import os
import datetime
import time
import singer.metrics as metrics
import singer.utils as utils

session = requests.Session()
logger = singer.get_logger()

METRICS_ENDPOINT = 'https://a.klaviyo.com/api/v1/metrics'
METRIC_ENDPOINT = 'https://a.klaviyo.com/api/v1/metric/'

def authed_get(source, url):
    with metrics.http_request_timer(source) as timer:
        resp = session.request(method='get', url=url)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        return resp


def authed_get_all_pages(source, url, since):
    while True:
        r = authed_get(source, '{}&since={}'.format(
            url, since) if since else url)
        yield r
        if 'next' in r.json() and r.json()['next']:
            since = r.json()['next']
        else:
            break


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas(event_names):
    schemas = {}

    for event in event_names:
        with open(get_abs_path('tap_klaviyo/{}.json'.format(event))) as file:
            schemas[event] = json.load(file)

    return schemas


def dt_to_ts(dt):
    return int(time.mktime(datetime.datetime.strptime(
        dt, "%Y-%m-%dT%H:%M:%SZ").timetuple()))


def ts_to_dt(ts):
    return datetime.datetime.fromtimestamp(
        int(ts)).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_events_by_type(event, state, api_key, start_date):
    if event['name'] in state and state[event['name']] is not None:
        since = dt_to_ts(state[event['name']])
    elif start_date:
        since = dt_to_ts(start_date)
    else:
        since = None

    latest_event_time = None

    with metrics.record_counter(event['name']) as counter:
        url = '{}{}/timeline?api_key={}&sort=asc'.format(
            METRIC_ENDPOINT,
            event['id'],
            api_key
        )
        for response in authed_get_all_pages(event['name'], url, since):
            events = response.json().get('data')

            for e in events:
                counter.increment()

            singer.write_records(event['name'], events)
            latest_event_time = ts_to_dt(int(events[-1]['timestamp'])) \
                if len(events) else None

            logger.info('Replicated events up to %s', latest_event_time)

    state[event['name']] = latest_event_time if latest_event_time else since
    return state


def do_sync(config, state):
    api_key = config['api_key']
    start_date = config['start_date'] if 'start_date' in config else None
    events = config['events']
    schemas = load_schemas(event['name'] for event in events)

    if state:
        logger.info('Replicating events since %s', state)
    else:
        logger.info('Replicating all events')

    for event in events:
        singer.write_schema(event['name'], schemas[event['name']], event['primary_key'])
        state = get_events_by_type(event, state, api_key, start_date)

    singer.write_state(state)


def get_available_metrics(api_key):
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
