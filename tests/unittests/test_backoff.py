import tap_klaviyo.utils as utils_
import unittest
from unittest import mock
import simplejson
import singer
from singer import metrics
import json
import requests

@mock.patch("tap_klaviyo.utils.get_request_timeout")
class TestBackoff(unittest.TestCase):
    
    @mock.patch('requests.Session.request')
    def test_httperror(self, mocked_session, mocked_get_request_timeout):

        mock_resp = mock.Mock()
        
        klaviyo_error = utils_.KlaviyoInternalServiceError()
        http_error = requests.HTTPError()

        mock_resp.raise_for_error.side_effect = klaviyo_error
        mock_resp.raise_for_status.side_effect = http_error
        mock_resp.status_code = 500
        
        mocked_session.return_value = mock_resp

        try:
            utils_.authed_get("", "", "")
        except utils_.KlaviyoInternalServiceError:
            pass

        self.assertEquals(mocked_session.call_count, 3)

    @mock.patch("requests.Session.request")
    def test_jsondecode(self, mocked_request, mocked_get_request_timeout):

        mock_resp = mock.Mock()

        mock_resp.status_code = 200
        mock_resp.json.side_effect = simplejson.scanner.JSONDecodeError("", "", 1)

        mocked_request.return_value = mock_resp

        try:
            utils_.authed_get("", "", "")
        except simplejson.scanner.JSONDecodeError:
            pass

        self.assertEquals(mocked_request.call_count, 3)