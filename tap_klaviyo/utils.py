import datetime
import time
import singer
from singer import metrics, metadata, Transformer
import requests
import backoff
import simplejson

DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"

# set default timeout of 300 seconds
REQUEST_TIMEOUT = 300

session = requests.Session()
logger = singer.get_logger()

class KlaviyoError(Exception):
    pass

class KlaviyoNotFoundError(KlaviyoError):
    pass

class KlaviyoBadRequestError(KlaviyoError):
    pass

class KlaviyoUnauthorizedError(KlaviyoError):
    pass

class KlaviyoForbiddenError(KlaviyoError):
    pass

class KlaviyoInternalServiceError(KlaviyoError):
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": KlaviyoBadRequestError,
        "message": "Request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": KlaviyoUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": KlaviyoForbiddenError,
        "message": "Invalid authorization credentials or permissions."
    },
    404: {
        "raise_exception": KlaviyoNotFoundError,
        "message": "The requested resource doesn't exist."
    },
    500: {
        "raise_exception": KlaviyoInternalServiceError,
        "message": "Internal Service Error from Klaviyo."
    }
}

def raise_for_error(response):   
    try:
        response.raise_for_status()
    except requests.HTTPError:
        try:
            json_resp = response.json()
        except (ValueError, TypeError, IndexError, KeyError):
            json_resp = {}

        error_code = response.status_code
        message_text = json_resp.get("message", ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error"))
        message = "HTTP-error-code: {}, Error: {}".format(error_code, message_text)
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", KlaviyoError)
        raise exc(message) from None

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

# during 'Timeout' error there is also possibility of 'ConnectionError',
# hence added backoff for 'ConnectionError' too.
@backoff.on_exception(backoff.expo, (requests.Timeout, requests.ConnectionError), max_tries=5, factor=2)
@backoff.on_exception(backoff.expo, (simplejson.scanner.JSONDecodeError, KlaviyoInternalServiceError), max_tries=3)
def authed_get(source, url, params):
    with metrics.http_request_timer(source) as timer:
        resp = session.request(method='get', url=url, params=params, timeout=get_request_timeout())
        
        if resp.status_code != 200:
            raise_for_error(resp)
        else:
            resp.json()
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
                transfrom_and_write_records(events, stream)
                update_state(state, stream['stream'], get_latest_event_time(events))
                singer.write_state(state)

    return state

def get_full_pulls(resource, endpoint, api_key):

    with metrics.record_counter(resource['stream']) as counter:

        for response in get_all_pages(resource['stream'], endpoint, api_key):
            
            records = response.json().get('data')
            counter.increment(len(records))
            transfrom_and_write_records(records, resource)


def transfrom_and_write_records(events, stream):
    event_stream = stream['stream']
    event_schema = stream['schema']
    event_mdata = metadata.to_map(stream['metadata'])

    with Transformer() as transformer:
        for event in events:
            singer.write_record(
                event_stream, 
                transformer.transform(
                    event, event_schema, event_mdata
            ))

# return the 'timeout'
def get_request_timeout():
    args = singer.utils.parse_args([])
    # get the value of request timeout from config
    config_request_timeout = args.config.get('request_timeout')

    # only return the timeout value if it is passed in the config and the value is not 0, "0" or ""
    if config_request_timeout and float(config_request_timeout):
        # return the timeout from config
        return float(config_request_timeout)

    # return default timeout
    return REQUEST_TIMEOUT
