import tap_klaviyo.utils as utils_
import unittest
from unittest import mock
import simplejson
import singer
from singer import metrics
import json
import requests

@mock.patch('time.sleep')
@mock.patch("tap_klaviyo.utils.get_request_timeout")
@mock.patch('requests.Session.request')
class TestBackoff(unittest.TestCase):

    def test_internal_service_error(self, mocked_session, mocked_get_request_timeout, mocked_sleep):
        """
        Check whether the request backoffs properly for authed_get() for 3 times in case of KlaviyoInternalServiceError.
        """
        mock_resp = mock.Mock()

        klaviyo_error = utils_.KlaviyoInternalServiceError()
        http_error = requests.HTTPError()

        mock_resp.raise_for_error.side_effect = klaviyo_error
        mock_resp.raise_for_status.side_effect = http_error
        mock_resp.status_code = 500

        mocked_session.return_value = mock_resp

        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoInternalServiceError:
            pass

        # verify that we backoff for 3 times
        self.assertEquals(mocked_session.call_count, 3)

    def test_jsondecode(self, mocked_request, mocked_get_request_timeout, mocked_sleep):
        """
        Check whether the request backoffs properly for authed_get() for 3 times in case of JSONDecodeError.
        """
        mock_resp = mock.Mock()

        mock_resp.status_code = 200
        mock_resp.json.side_effect = simplejson.scanner.JSONDecodeError("", "", 1)

        mocked_request.return_value = mock_resp

        try:
            utils_.authed_get("", "", "", "")
        except simplejson.scanner.JSONDecodeError:
            pass

        # verify that we backoff for 3 times
        self.assertEquals(mocked_request.call_count, 3)

    def test_not_implemented_error(self, mocked_session, mocked_get_request_timeout, mocked_sleep):
        """
        Check whether the request backoffs properly for authed_get() for 3 times in case of KlaviyoNotImplementedError.
        """
        mock_resp = mock.Mock()

        klaviyo_error = utils_.KlaviyoNotImplementedError()
        http_error = requests.HTTPError()

        mock_resp.raise_for_error.side_effect = klaviyo_error
        mock_resp.raise_for_status.side_effect = http_error
        mock_resp.status_code = 501

        mocked_session.return_value = mock_resp

        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoNotImplementedError:
            pass

        # verify that we backoff for 3 times
        self.assertEquals(mocked_session.call_count, 3)

    def test_bad_gateway_error(self, mocked_session, mocked_get_request_timeout, mocked_sleep):
        """
        Check whether the request backoffs properly for authed_get() for 3 times in case of KlaviyoBadGatewayError.
        """
        mock_resp = mock.Mock()

        klaviyo_error = utils_.KlaviyoBadGatewayError()
        http_error = requests.HTTPError()

        mock_resp.raise_for_error.side_effect = klaviyo_error
        mock_resp.raise_for_status.side_effect = http_error
        mock_resp.status_code = 502

        mocked_session.return_value = mock_resp

        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoBadGatewayError:
            pass

        # verify that we backoff for 3 times
        self.assertEquals(mocked_session.call_count, 3)

    def test_service_unavailable_error(self, mocked_session, mocked_get_request_timeout, mocked_sleep):
        """
        Check whether the request backoffs properly for authed_get() for 3 times in case of KlaviyoServiceUnavailableError.
        """
        mock_resp = mock.Mock()

        klaviyo_error = utils_.KlaviyoServiceUnavailableError()
        http_error = requests.HTTPError()

        mock_resp.raise_for_error.side_effect = klaviyo_error
        mock_resp.raise_for_status.side_effect = http_error
        mock_resp.status_code = 503

        mocked_session.return_value = mock_resp

        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoServiceUnavailableError:
            pass

        # verify that we backoff for 3 times
        self.assertEquals(mocked_session.call_count, 3)

    def test_gateway_timeout_error(self, mocked_session, mocked_get_request_timeout, mocked_sleep):
        """
        Check whether the request backoffs properly for authed_get() for 3 times in case of KlaviyoGatewayTimeoutError.
        """
        mock_resp = mock.Mock()

        klaviyo_error = utils_.KlaviyoGatewayTimeoutError()
        http_error = requests.HTTPError()

        mock_resp.raise_for_error.side_effect = klaviyo_error
        mock_resp.raise_for_status.side_effect = http_error
        mock_resp.status_code = 504

        mocked_session.return_value = mock_resp

        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoGatewayTimeoutError:
            pass

        # verify that we backoff for 3 times
        self.assertEquals(mocked_session.call_count, 3)

    def test_server_timeout_error(self, mocked_session, mocked_get_request_timeout, mocked_sleep):
        """
        Check whether the request backoffs properly for authed_get() for 3 times in case of KlaviyoServerTimeoutError.
        """
        mock_resp = mock.Mock()

        klaviyo_error = utils_.KlaviyoServerTimeoutError()
        http_error = requests.HTTPError()

        mock_resp.raise_for_error.side_effect = klaviyo_error
        mock_resp.raise_for_status.side_effect = http_error
        mock_resp.status_code = 524

        mocked_session.return_value = mock_resp

        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoServerTimeoutError:
            pass

        # verify that we backoff for 3 times
        self.assertEquals(mocked_session.call_count, 3)


    def test_rate_limit_error(self, mocked_session, mocked_get_request_timeout, mocked_sleep):
        """
        Check whether the request backoffs properly for authed_get() for 3 times in case of KlaviyoRateLimitError.
        """
        mock_resp = mock.Mock()

        klaviyo_error = utils_.KlaviyoRateLimitError()
        http_error = requests.HTTPError()

        mock_resp.raise_for_error.side_effect = klaviyo_error
        mock_resp.raise_for_status.side_effect = http_error
        mock_resp.status_code = 429

        mocked_session.return_value = mock_resp

        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoRateLimitError:
            pass

        # verify that we backoff for 3 times
        self.assertEquals(mocked_session.call_count, 3)
