import unittest
from unittest import mock
import singer
from tap_klaviyo.utils import transfrom_and_write_records
from tap_klaviyo import discover


class MockResponse:
    def __init__(self, status_code, resp={}, content=None, headers=None, raise_error=False):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error

    def json(self):
        return self.json_data

    def raise_for_status(self):
        raise requests.HTTPError

def successful_200_request(*args, **kwargs):
    json_str = {"data": [{"type": "metric", "id": "abc", "attributes": {"name": "abc", "created": "2019-09-09T19:06:57+00:00", "updated": "2019-09-09T19:06:57+00:00", "integration": {"object": "integration", "id": "abc"}}}],"links": {"self": "abc", "next": None, "previous": None}}

    return MockResponse(200, json_str)

class MockParseArgs():
    """
    Mocked MockParseArgs class with custom state, discover, config attributes to pass unit test cases.
    """
    def __init__(self, state, discover, config):
        self.state = state
        self.discover = discover
        self.config = config

class TestNullValuesInSchema(unittest.TestCase):

    @mock.patch("singer.utils.parse_args")
    @mock.patch('requests.Session.request', side_effect=successful_200_request)
    def test_null_values(self, mock_200, mock_parse_args):
        """Verify that the record with a null value is written when passed through transform"""
        config = {}
        mock_parse_args.return_value = MockParseArgs(state = {}, discover = True, config=config)
        # record with null values
        records = [{'type': 'campaign', 'id': None, 'attributes': {'name': None, 'status': None, 'archived': None, 'audiences': {'included': [None], 'excluded': [None]}, 'send_options': {'use_smart_sending': None, 'ignore_unsubscribes': None}, 'tracking_options': {'is_add_utm': None, 'utm_params': [], 'is_tracking_clicks': None, 'is_tracking_opens': None}, 'send_strategy': {'method': None, 'options_static': None, 'options_throttled': None, 'options_sto': None}, 'created_at': None, 'scheduled_at': None, 'updated_at': None, 'send_time': None}, 'relationships': {'campaign-messages': {'data': [{'type': 'campaign-message', 'id': None}], 'links': {'self': None, 'related': None}}, 'tags': {'data': [], 'links': {'self': None, 'related': None}}}, 'links': {'self': None}}]
        catalog = discover("dummy_key")
        stream = None
        stream = [each for each in catalog['streams'] if each['stream'] == 'campaigns']
        # Verify by calling `transfrom_and_write_records` that it doesn't throw any exception while transform
        transfrom_and_write_records(records, stream[0], included=[], valid_relationships=[])