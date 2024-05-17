import tap_klaviyo.utils as utils_
import unittest
from unittest import mock
import requests


class Mockresponse:
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
    json_str = {"tap": "klaviyo", "code": 200}

    return Mockresponse(200, json_str)

def klaviyo_400_error(*args, **kwargs):

    return Mockresponse(status_code=400, raise_error=True)

def klaviyo_401_error(*args, **kwargs):

    return Mockresponse(status_code=401, raise_error=True)

def klaviyo_403_error(*args, **kwargs):

    return Mockresponse(status_code=403, raise_error=True)

def klaviyo_403_error_wrong_api_key(*args, **kwargs):
    json_str = {
    "status": 403,
    "message": "The API key specified is invalid."}

    return Mockresponse(resp=json_str, status_code=403, raise_error=True)

def klaviyo_403_error_missing_api_key(*args, **kwargs):
    json_str = {
    "status": 403,
    "message": "You must specify an API key to make requests."}

    return Mockresponse(resp=json_str, status_code=403, raise_error=True)

def klaviyo_404_error(*args, **kwargs):

    return Mockresponse(status_code=404, raise_error=True)

def klaviyo_409_error(*args, **kwargs):

    return Mockresponse(status_code=409, raise_error=True)

def klaviyo_500_error(*args, **kwargs):

    return Mockresponse(status_code=500, raise_error=True)

def klaviyo_429_error(*args, **kwargs):

    return Mockresponse(status_code=429, raise_error=True)

def klaviyo_501_error(*args, **kwargs):

    return Mockresponse(status_code=501, raise_error=True)

def klaviyo_502_error(*args, **kwargs):

    return Mockresponse(status_code=502, raise_error=True)

def klaviyo_503_error(*args, **kwargs):

    return Mockresponse(status_code=503, raise_error=True)

def klaviyo_504_error(*args, **kwargs):

    return Mockresponse(status_code=504, raise_error=True)

def klaviyo_524_error(*args, **kwargs):

    return Mockresponse(status_code=524, raise_error=True)

@mock.patch("tap_klaviyo.utils.get_request_timeout")
class TestBackoff(unittest.TestCase):

    @mock.patch('requests.Session.request', side_effect=successful_200_request)
    def test_200(self, successful_200_request, mocked_get_request_timeout):
        test_data = {"tap": "klaviyo", "code": 200}

        actual_data = utils_.authed_get("", "", "", "").json()
        self.assertEquals(actual_data, test_data)

    @mock.patch('requests.Session.request', side_effect=klaviyo_400_error)
    def test_400_error(self, klaviyo_400_error, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 400 error with proper message
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoBadRequestError as e:
            self.assertEquals(str(e), "HTTP-error-code: 400, Error: Request is missing or has a bad parameter.")

    @mock.patch('requests.Session.request', side_effect=klaviyo_401_error)
    def test_401_error(self, klaviyo_401_error, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 401 error with proper message
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoUnauthorizedError as e:
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: Invalid authorization credentials.")

    @mock.patch('requests.Session.request', side_effect=klaviyo_403_error)
    def test_403_error(self, klaviyo_403_error, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 403 error with proper message
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoForbiddenError as e:
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: Invalid authorization credentials or permissions.")

    @mock.patch('requests.Session.request', side_effect=klaviyo_403_error_wrong_api_key)
    def test_403_error_wrong_api_key(self, klaviyo_403_error_wrong_api_key, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 403 error with proper message for wrong api key
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoForbiddenError as e:
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: The API key specified is invalid.")

    @mock.patch('requests.Session.request', side_effect=klaviyo_403_error_missing_api_key)
    def test_403_error_missing_api_key(self, klaviyo_403_error_missing_api_key, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 403 error with proper message for empty api key
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoForbiddenError as e:
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: You must specify an API key to make requests.")

    @mock.patch('requests.Session.request', side_effect=klaviyo_404_error)
    def test_404_error(self, klaviyo_404_error, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 404 error with proper message
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoNotFoundError as e:
            self.assertEquals(str(e), "HTTP-error-code: 404, Error: The requested resource doesn't exist.")

    @mock.patch('requests.Session.request', side_effect=klaviyo_409_error)
    def test_409_error(self, klaviyo_409_error, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 409 error with proper message
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoConflictError as e:
            self.assertEquals(str(e), "HTTP-error-code: 409, Error: The API request cannot be completed because the requested operation would conflict with an existing item.")

    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request', side_effect=klaviyo_500_error)
    def test_500_error(self, klaviyo_500_error, mocked_sleep, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 500 error with proper message
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoInternalServiceError as e:
            self.assertEquals(str(e), "HTTP-error-code: 500, Error: Internal Service Error from Klaviyo.")

    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request', side_effect=klaviyo_429_error)
    def test_429_error(self, klaviyo_429_error, mocked_sleep, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 429 error with proper message
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoRateLimitError as e:
            self.assertEquals(str(e), "HTTP-error-code: 429, Error: The API rate limit for your organization/application pairing has been exceeded.")

    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request', side_effect=klaviyo_501_error)
    def test_501_error(self, klaviyo_501_error, mocked_sleep, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 501 error with proper message
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoNotImplementedError as e:
            self.assertEquals(str(e), "HTTP-error-code: 501, Error: The server does not support the functionality required to fulfill the request.")

    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request', side_effect=klaviyo_502_error)
    def test_502_error(self, klaviyo_502_error, mocked_sleep, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 502 error with proper message
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoBadGatewayError as e:
            self.assertEquals(str(e), "HTTP-error-code: 502, Error: Server received an invalid response from another server.")

    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request', side_effect=klaviyo_503_error)
    def test_503_error(self, klaviyo_503_error, mocked_sleep, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 503 error with proper message
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoServiceUnavailableError as e:
            self.assertEquals(str(e), "HTTP-error-code: 503, Error: API service is currently unavailable.")

    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request', side_effect=klaviyo_504_error)
    def test_504_error(self, klaviyo_504_error, mocked_sleep, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 504 error with proper message
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoGatewayTimeoutError as e:
            self.assertEquals(str(e), "HTTP-error-code: 504, Error: Server did not return a response from another server.")

    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request', side_effect=klaviyo_524_error)
    def test_524_error(self, klaviyo_524_error, mocked_sleep, mocked_get_request_timeout):
        """
        Test that `authed_get` raise 524 error with proper message
        """
        try:
            utils_.authed_get("", "", "", "")
        except utils_.KlaviyoServerTimeoutError as e:
            self.assertEquals(str(e), "HTTP-error-code: 524, Error: Server took too long to respond to the request.")
