import tap_klaviyo
import unittest
from unittest import mock
import singer
from singer import metrics, metadata, Transformer
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
    json_str = {"data": [{'object': 'metric', 'id': 'dummy_id', 'name': 'Fulfilled Order', 'integration': {'object': 'integration', 'id': '0eMvjm', 'name': 'Shopify', 'category': 'eCommerce'}, 'created': '2021-09-15 09:54:46', 'updated': '2021-09-15 09:54:46'}], 'page': 0, 'start': 0, 'end': 28, 'total': 29, 'page_size': 29}

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
        records = [{'object': 'campaign', 'id': None, 'name': None, 'subject': None, 'from_email': None, 'from_name': None, 'lists': None, 'excluded_lists': [], 'status': None, 'status_id': None, 'status_label': None, 'sent_at': None, 'send_time': None, 'created': None, 'updated': None, 'num_recipients': None, 'campaign_type': None, 'is_segmented': None, 'message_type': None, 'template_id': None}]
        catalog = discover("dummy_key")
        stream = None
        stream = [each for each in catalog['streams'] if each['stream'] == 'campaigns']
        # Verify by calling `transfrom_and_write_records` that it doesn't throw any exception while transform
        transfrom_and_write_records(records, stream[0])