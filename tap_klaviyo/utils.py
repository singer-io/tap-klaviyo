import datetime
import time
import backoff
import simplejson
import singer
import urllib3.exceptions
from singer import metrics
import requests

DATETIME_FMT = "%Y-%m-%dT%H:%M:%S"

session = requests.Session()
logger = singer.get_logger()


def dt_to_ts(dt):
    # Remove microseconds
    dt = dt[:19]
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

    logger.info("Replicated %s up to %s", entity, state['bookmarks'][entity])


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


@backoff.on_exception(backoff.expo, (requests.exceptions.ConnectionError,
                                     urllib3.exceptions.ProtocolError, ConnectionResetError, simplejson.scanner.JSONDecodeError),
                      max_tries=10)
def authed_get(source, url, params):
    with metrics.http_request_timer(source) as timer:
        resp = session.request(method='get', url=url, params=params)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        return resp.json()


def get_all_using_next(stream, url, api_key, since=None):
    while True:
        r = authed_get(stream, url, {'api_key': api_key,
                                     'since': since,
                                     'sort': 'asc'})
        yield r
        if 'next' in r and r['next']:
            since = r['next']
        else:
            break


def get_all_pages(source, url, api_key):
    page = 0
    while True:
        r = authed_get(source, url, {'page': page, 'api_key': api_key})
        yield r
        if r['end'] < r['total'] - 1:
            page += 1
        else:
            break


def get_incremental_pull(stream, endpoint, state, api_key, start_date):
    latest_event_time = get_starting_point(stream, state, start_date)

    with metrics.record_counter(stream['stream']) as counter:
        url = '{}{}/timeline'.format(
            endpoint,
            stream['stream']
        )
        for response in get_all_using_next(
                stream['stream'], url, api_key,
                latest_event_time):
            events = response.get('data')
            if events:
                counter.increment(len(events))

                singer.write_records(stream['stream'], events)

                update_state(state, stream['stream'], get_latest_event_time(events))
                singer.write_state(state)

    return state


def get_full_pulls(resource, endpoint, api_key):
    with metrics.record_counter(resource['stream']) as counter:
        for response in get_all_pages(resource['stream'], endpoint, api_key):
            records = response.get('data')

            counter.increment(len(records))

            singer.write_records(resource['stream'], records)


@backoff.on_exception(backoff.expo, (
        simplejson.scanner.JSONDecodeError, requests.exceptions.ConnectionError,
        urllib3.exceptions.ProtocolError,ConnectionResetError, requests.exceptions.Timeout),
                      max_tries=10)
def request_with_retry(endpoint, params):
    while True:
        global session
        r = session.get(endpoint, params=params)
        if r.status_code != 200:
            session = requests.Session()

        if r.status_code == 429 or r.status_code == 408:
            retry_after = int(r.headers['retry-after'])
            time.sleep(retry_after)
            continue
        return r.json()


def get_list_members_pull(resource, api_key):
    with metrics.record_counter(resource['stream']) as counter:
        pushed_profile_ids = set()
        for response in get_all_pages('lists', 'https://a.klaviyo.com/api/v1/lists', api_key):
            lists = response
            lists = lists['data']
            total_lists = len(lists)
            current_list = 0
            for list in lists:
                current_list += 1
                logger.info("Syncing list " + list['id'] + " : " + str(current_list) + " of " + str(total_lists))

                list_endpoint = 'https://a.klaviyo.com/api/v2/group/' + list['id'] + '/members/all'
                next_marker = True
                marker = None
                while next_marker:
                    data = request_with_retry(list_endpoint, params={'api_key': api_key, 'marker': marker})
                    if "records" not in data:
                        break
                    records = data['records']
                    logger.info("This list " + list['id'] + " has : " + str(len(records)))
                    if resource["tap_stream_id"] == "profiles":
                        for record in records:
                            if record["id"] not in pushed_profile_ids:
                                endpoint = f"https://a.klaviyo.com/api/v1/person/{record['id']}"
                                datas = request_with_retry(endpoint, params={'api_key': api_key})
                                datas = singer.transform(datas, resource['schema'])
                                singer.write_records(resource['stream'], [datas])
                                pushed_profile_ids.add(record["id"])
                    else:
                        for record in records:
                            record['list_id'] = list['id']
                            counter.increment()
                        singer.write_records(resource['stream'], records)
                    if "marker" in data:
                        marker = data['marker']
                        next_marker = True
                    else:
                        break


def get_flow_emails(resource, api_key):
    with metrics.record_counter(resource['stream']) as counter:
        for response in get_all_pages('lists', 'https://a.klaviyo.com/api/v1/flows', api_key):
            flows = response and response["data"]
            total_flows = len(flows)
            current_flow = 0
            for flow in flows:
                current_flow += 1
                logger.info("Syncing flow " + flow['id'] + ": " + str(current_flow) + " of " + str(total_flows))

                action_endpoint = 'https://a.klaviyo.com/api/v1/flow/' + flow['id'] + '/actions'
                actions = request_with_retry(action_endpoint, params={'api_key': api_key})
                actions = actions and [action["id"] for action in actions if action["type"] == "SEND_MESSAGE"] or []
                if not actions:
                    continue

                # several actions of type "SEND_MESSAGE" are associated with the flow
                for action_id in actions:
                    email_endpoint = 'https://a.klaviyo.com/api/v1/flow/' + flow['id'] + '/action/' + str(
                        action_id) + '/email'

                    next_marker = True
                    marker = None
                    while next_marker:
                        flow_emails = request_with_retry(email_endpoint, params={'api_key': api_key, 'marker': marker})
                        if not flow_emails:
                            continue

                        if isinstance(flow_emails, list):
                            for email in flow_emails:
                                flow_emails[email]["flow_id"] = flow["id"]
                                flow_emails[email]["message_id"] = email["id"]
                                datas = singer.transform(flow_emails, resource['schema'])
                                singer.write_records(resource['stream'], [datas])
                                counter.increment()
                        else:
                            flow_emails["flow_id"] = flow["id"]
                            flow_emails["message_id"] = flow_emails["id"]

                            datas = singer.transform(flow_emails, resource['schema'])
                            singer.write_records(resource['stream'], [datas])

                        if "marker" in flow_emails:
                            marker = flow_emails['marker']
                            next_marker = True
                        else:
                            break
