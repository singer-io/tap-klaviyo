import datetime
import time
import singer
from singer import metrics
import requests

DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"


session = requests.Session()
logger = singer.get_logger()


def dt_to_ts(dt):
    return int(time.mktime(datetime.datetime.strptime(
        dt, DATETIME_FMT).timetuple()))


def ts_to_dt(ts):
    return datetime.datetime.fromtimestamp(
        int(ts)).strftime(DATETIME_FMT)


def update_state(state, entity, dt):
    if dt is None:
        return

    if isinstance(dt, int):
        dt = ts_to_dt(dt)

    if entity not in state:
        state['bookmarks'][entity] = {'since': dt}

    if dt >= state['bookmarks'][entity]['since']:
        state['bookmarks'][entity] = {'since': dt}

    logger.info("Replicated %s up to %s" % (
        entity, state['bookmarks'][entity]))


def get_starting_point(stream, state, start_date):
    if stream['stream'] in state['bookmarks'] and \
                    state['bookmarks'][stream['stream']] is not None:
        return dt_to_ts(state['bookmarks'][stream['stream']]['since'])
    elif start_date:
        return dt_to_ts(start_date)
    else:
        return None


def get_latest_event_time(events):
    return ts_to_dt(int(events[-1]['timestamp'])) if len(events) else None


def authed_get(source, url, params):
    with metrics.http_request_timer(source) as timer:
        resp = session.request(method='get', url=url, params=params)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        return resp


def get_all_using_next(stream, url, api_key, since=None):
    while True:
        r = authed_get(stream, url, {'api_key': api_key,
                                     'since': since,
                                     'sort': 'asc'})
        yield r
        if 'next' in r.json() and r.json()['next']:
            since = r.json()['next']
        else:
            break


def get_all_pages(source, url, api_key):
    page = 0
    while True:
        r = authed_get(source, url, {'page': page, 'api_key': api_key})
        yield r
        if r.json()['end'] < r.json()['total'] - 1:
            page += 1
        else:
            break


def get_incremental_pull(stream, endpoint, state, api_key, start_date):
    latest_event_time = get_starting_point(stream, state, start_date)

    with metrics.record_counter(stream['stream']) as counter:
        url = '{}{}/timeline'.format(
            endpoint,
            stream['tap_stream_id']
        )
        for response in get_all_using_next(
                stream['stream'], url, api_key,
                latest_event_time):
            events = response.json().get('data')

            if events:
                counter.increment(len(events))

                singer.write_records(stream['stream'], events)

                update_state(state, stream['stream'], get_latest_event_time(events))
                singer.write_state(state)

    return state


def get_full_pulls(resource, endpoint, api_key):
    with metrics.record_counter(resource['stream']) as counter:
        for response in get_all_pages(resource['stream'], endpoint, api_key):
            records = response.json().get('data')

            counter.increment(len(records))

            singer.write_records(resource['stream'], records)