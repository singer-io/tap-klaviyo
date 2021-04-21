import tap_klaviyo.utils as utils_
import unittest
from unittest import mock
import simplejson
import singer
from singer import metrics
import json
import requests

class TestBackoff(unittest.TestCase):
    
    @mock.patch('requests.Session.request')
    def test_httperror(self, mocked_session):

        mock_resp = mock.Mock()
        klaviyo_error = utils_.KlaviyoError()
        http_error = requests.HTTPError()

        mock_resp.raise_for_error.side_effect = klaviyo_error
        mock_resp.raise_for_status.side_effect = http_error
        
        mocked_session.return_value = mock_resp

        try:
            utils_.authed_get("", "", "")
        except utils_.KlaviyoError:
            pass

        self.assertEquals(mocked_session.call_count, 3)

    @mock.patch("tap_klaviyo.utils.get_all_pages")
    def test_jsondecode(self, mock1):

        mock1.return_value = utils_.get_all_pages("lists", "http://www.youtube.com/results?abcd", "")

        data = {'stream': 'lists'}
        try:
            utils_.get_full_pulls(data, "http://www.youtube.com/results?abcd", "")
        except simplejson.scanner.JSONDecodeError:
            pass

        self.assertEquals(mock1.call_count, 2)