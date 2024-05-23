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

STREAM_PARAMS_MAP = {
    "campaigns": [
        {
            "filter": "equals(messages.channel,'email')",
            "include": "tags,campaign-messages"
        }
    ],
    "global_exclusions": [
        {
            "filter": "equals(subscriptions.email.marketing.suppression.reason,'HARD_BOUNCE')",
            "additional-fields[profile]": "subscriptions,predictive_analytics"
        },
        {
            "filter": "equals(subscriptions.email.marketing.suppression.reason,'USER_SUPPRESSED')",
            "additional-fields[profile]": "subscriptions,predictive_analytics"
        },
        {
            "filter": "equals(subscriptions.email.marketing.suppression.reason,'UNSUBSCRIBE')",
            "additional-fields[profile]": "subscriptions,predictive_analytics"
        },
        {
            "filter": "equals(subscriptions.email.marketing.suppression.reason,'INVALID_EMAIL')",
            "additional-fields[profile]": "subscriptions,predictive_analytics"
        }
    ],
    "lists": [
        {
            "include": "tags"
        }
    ]

}

class KlaviyoError(Exception):
    pass

class KlaviyoBackoffError(KlaviyoError):
    pass

class KlaviyoNotFoundError(KlaviyoError):
    pass

class KlaviyoBadRequestError(KlaviyoError):
    pass

class KlaviyoUnauthorizedError(KlaviyoError):
    pass

class KlaviyoForbiddenError(KlaviyoError):
    pass

class KlaviyoConflictError(KlaviyoError):
    pass

class KlaviyoRateLimitError(KlaviyoBackoffError):
    pass

class KlaviyoInternalServiceError(KlaviyoBackoffError):
    pass

class KlaviyoNotImplementedError(KlaviyoBackoffError):
    pass

class KlaviyoBadGatewayError(KlaviyoBackoffError):
    pass

class KlaviyoServiceUnavailableError(KlaviyoBackoffError):
    pass

class KlaviyoGatewayTimeoutError(KlaviyoBackoffError):
    pass

class KlaviyoServerTimeoutError(KlaviyoBackoffError):
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
    409: {
        "raise_exception": KlaviyoConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    429: {
        "raise_exception": KlaviyoRateLimitError,
        "message": "The API rate limit for your organization/application pairing has been exceeded."
    },
    500: {
        "raise_exception": KlaviyoInternalServiceError,
        "message": "Internal Service Error from Klaviyo."
    },
    501: {
        "raise_exception": KlaviyoNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": KlaviyoBadGatewayError,
        "message": "Server received an invalid response from another server."
    },
    503: {
        "raise_exception": KlaviyoServiceUnavailableError,
        "message": "API service is currently unavailable."
    },
    504: {
        "raise_exception": KlaviyoGatewayTimeoutError,
        "message": "Server did not return a response from another server."
    },
    524: {
        "raise_exception": KlaviyoServerTimeoutError,
        "message": "Server took too long to respond to the request."
    },
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


# Decrease timestamp(maximum replication key value) by 1 second.
# Because API returns records in which the replication key value is greater than the last saved bookmark value.
# So, sometimes records with the same bookmark value may be lost.
def get_latest_event_time(events):
    return ts_to_dt(int(events[-1]['timestamp']) - 1) if len(events) else None

# during 'Timeout' error there is also possibility of 'ConnectionError',
# hence added backoff for 'ConnectionError' too.
@backoff.on_exception(backoff.expo, (requests.Timeout, requests.ConnectionError), max_tries=5, factor=2)
@backoff.on_exception(backoff.expo, (simplejson.scanner.JSONDecodeError, KlaviyoBackoffError), max_tries=3)
def authed_get(source, url, params, headers):
    with metrics.http_request_timer(source) as timer:
        resp = requests.get(url=url, params=params, headers=headers, timeout=get_request_timeout())
        
        if resp.status_code != 200:
            raise_for_error(resp)
        else:
            resp.json()
            timer.tags[metrics.Tag.http_status_code] = resp.status_code
            return resp

def get_all_using_next(stream, url, headers, params):
    # Paginate till there is a url or next url.
    while url:
        r = authed_get(stream, url, params, headers)
        # Re-initializing params to {} as next url contains all necessary params.
        params = {}
        yield r
        url = r.json()['links'].get('next', None)

def get_incremental_pull(stream, endpoint, state, headers, start_date):
    latest_event_time = get_starting_point(stream, state, start_date)

    with metrics.record_counter(stream['stream']) as counter:
        params = {
            "filter": f"equals(metric_id,\"{stream['tap_stream_id']}\"),greater-or-equal(timestamp,{latest_event_time})",
            "include": "profile,metric",
            "sort": "datetime"
        }
        for response in get_all_using_next(stream['stream'], endpoint, headers, params):
            events = response.json().get('data')

            if events:
                included_list = response.json().get('included', [])
                # Creating a dict/map of included relationships to optimize computations
                included = {}
                for included_relationship in included_list:
                    included[included_relationship['id']] = included_relationship
                counter.increment(len(events))
                transfrom_and_write_records(events, stream, included, params.get("include","").split(","))
                update_state(state, stream['stream'], get_latest_event_time(events))
                singer.write_state(state)

    return state

def get_full_pulls(resource, endpoint, headers):

    with metrics.record_counter(resource['stream']) as counter:
        for params in STREAM_PARAMS_MAP.get(resource['stream'],[]):
            for response in get_all_using_next(resource['stream'], endpoint, headers, params):
                records = response.json().get('data')
                included_list = response.json().get('included', [])
                # Creating a dict/map of included relationships to optimize computations
                included = {}
                for included_relationship in included_list:
                    included[included_relationship['id']] = included_relationship
                counter.increment(len(records))
                transfrom_and_write_records(records, resource, included, params.get("include","").split(","))


def transfrom_and_write_records(events, stream, included, valid_relationships):
    event_stream = stream['stream']
    event_schema = stream['schema']
    event_mdata = metadata.to_map(stream['metadata'])

    with Transformer() as transformer:
        for event in events:
            # Flatten the event dict with attributes
            event.update(event['attributes'])
            for relationship_key, relationship_value in event.get('relationships',{}).items():
                if not relationship_key in valid_relationships:
                    continue
                relationship_data = relationship_value['data']
                # Generalizing relationship data to list of dicts for all streams
                # This is due to the fact that, for Full table streams, data is returned as a list
                # And, for incremental streams, data is return as a dict in API response
                if isinstance(relationship_data, dict):
                    relationship_data = [relationship_data]
                for relationship in relationship_data:
                    included_relationship = included.get(relationship['id'], None)
                    # Check if current relationship is present in included relationship dict
                    if included_relationship is not None:
                        # Flatten the included_relationship dict with attributes
                        included_relationship.update(included_relationship['attributes'])
                        relationship.update(included_relationship)
                event.update({relationship_key: relationship_data})
            # write record
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
