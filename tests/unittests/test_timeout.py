
import unittest
import requests
from unittest import mock

import tap_klaviyo.utils as utils

class MockParseArgs:
    config = {}
    def __init__(self, config):
        self.config = config

def get_args(config):
    return MockParseArgs(config)

@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
@mock.patch("singer.utils.parse_args")
class TestTimeoutValue(unittest.TestCase):

    def test_timeout_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep):

        mock_config = {"request_timeout": 100}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = utils.get_request_timeout()
        # function call
        utils.authed_get("test_source", "", "", "")

        # verify that we got expected timeout value
        self.assertEquals(100.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', params='', headers='', timeout=100.0)

    def test_timeout_value_not_in_config(self, mocked_parse_args, mocked_request, mocked_sleep):

        mock_config = {}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = utils.get_request_timeout()
        # function call
        utils.authed_get("test_source", "", "","")

        # verify that we got expected timeout value
        self.assertEquals(300.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', params='', headers='', timeout=300.0)

    def test_timeout_string_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep):

        mock_config = {"request_timeout": "100"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = utils.get_request_timeout()
        # function call
        utils.authed_get("test_source", "", "","")

        # verify that we got expected timeout value
        self.assertEquals(100.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', params='', headers='', timeout=100.0)

    def test_timeout_empty_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep):

        mock_config = {"request_timeout": ""}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = utils.get_request_timeout()
        # function call
        utils.authed_get("test_source", "", "","")

        # verify that we got expected timeout value
        self.assertEquals(300.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', params='',  headers='', timeout=300.0)

    def test_timeout_0_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep):

        mock_config = {"request_timeout": 0.0}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = utils.get_request_timeout()
        # function call
        utils.authed_get("test_source", "", "","")

        # verify that we got expected timeout value
        self.assertEquals(300.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', params='',  headers='', timeout=300.0)

    def test_timeout_string_0_value_in_config(self, mocked_parse_args, mocked_request, mocked_sleep):

        mock_config = {"request_timeout": "0.0"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = utils.get_request_timeout()
        # function call
        utils.authed_get("test_source", "", "","")

        # verify that we got expected timeout value
        self.assertEquals(300.0, timeout)
        # verify that the request was called with expected timeout value
        mocked_request.assert_called_with(method='get', url='', params='',  headers='', timeout=300.0)

@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
@mock.patch("singer.utils.parse_args")
class TestTimeoutAndConnectionErrorBackoff(unittest.TestCase):

    def test_timeout_backoff(self, mocked_parse_args, mocked_request, mocked_sleep):

        # mock request and raise 'Timeout' error
        mocked_request.side_effect = requests.Timeout

        mock_config = {}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        try:
            # function call
            utils.authed_get("test_source", "", "","")
        except requests.Timeout:
            pass

        # verify that we backoff for 5 times
        self.assertEquals(5, mocked_request.call_count)

    def test_connection_error_backoff(self, mocked_parse_args, mocked_request, mocked_sleep):

        # mock request and raise 'ConnectionError' error
        mocked_request.side_effect = requests.ConnectionError

        mock_config = {}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        try:
            # function call
            utils.authed_get("test_source", "", "","")
        except requests.ConnectionError:
            pass

        # verify that we backoff for 5 times
        self.assertEquals(5, mocked_request.call_count)
