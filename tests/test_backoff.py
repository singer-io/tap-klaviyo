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
        mock_resp.raise_for_error.side_effect = klaviyo_error
        mocked_session.return_value = mock_resp

        try:
            utils_.authed_get("", "", "")
        except utils_.KlaviyoError:
            pass

        self.assertEquals(mocked_session.call_count, 3)

    @mock.patch("tap_klaviyo.utils.get_all_pages")
    def test_jsondecode(self, mock1):

        mock1.return_value = ["abc"]

        data = {'stream': 'lists'}
        try:
            utils_.get_full_pulls(data, "", "")
        except simplejson.scanner.JSONDecodeError:
            pass

        self.assertEquals(mock1.call_count, 3)