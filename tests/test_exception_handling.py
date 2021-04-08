import tap_klaviyo.utils as utils_
import unittest
from unittest import mock
import requests
import singer

class Mockresponse:
    def __init__(self, resp, status_code, content=None, headers=None, raise_error=False):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
    
    def json(self):
            return self.json_data

def klaviyo_400_error(*args, **kwargs):
    json_str = {}

    return Mockresponse(json_str, 400, raise_error=True)

def klaviyo_401_error(*args, **kwargs):
    json_str = {}

    return Mockresponse(json_str, 401, raise_error=True)

def klaviyo_403_error(*args, **kwargs):
    json_str = {}

    return Mockresponse(json_str, 403, raise_error=True)

def klaviyo_403_error_wrong_api_key(*args, **kwargs):
    json_str = {
    "status": 403,
    "message": "The API key specified is invalid."}

    return Mockresponse(json_str, 403, raise_error=True)

def klaviyo_403_error_missing_api_key(*args, **kwargs):
    json_str = {
    "status": 403,
    "message": "You must specify an API key to make requests."}

    return Mockresponse(json_str, 403, raise_error=True)

def klaviyo_404_error(*args, **kwargs):
    json_str = {}

    return Mockresponse(json_str, 404, raise_error=True)

def klaviyo_500_error(*args, **kwargs):
    json_str = {}

    return Mockresponse(json_str, 500, raise_error=True)


class TestBackoff(unittest.TestCase):
    
    @mock.patch('requests.Session.request', side_effect=klaviyo_400_error)
    def test_400_error(self, klaviyo_400_error):

        try:
            utils_.authed_get("", "", "")
        except utils_.KlaviyoError as e:
            self.assertEquals(str(e), "HTTP-error-code: 400, Error: Request is missing or has a bad parameter.")

    @mock.patch('requests.Session.request', side_effect=klaviyo_401_error)
    def test_401_error(self, klaviyo_401_error):

        try:
            utils_.authed_get("", "", "")
        except utils_.KlaviyoError as e:
            self.assertEquals(str(e), "HTTP-error-code: 401, Error: Invalid authorization credentials.")

    @mock.patch('requests.Session.request', side_effect=klaviyo_403_error)
    def test_403_error(self, klaviyo_403_error):

        try:
            utils_.authed_get("", "", "")
        except utils_.KlaviyoError as e:
            with open("abc.txt", "w") as f:
                f.write(str(e))
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: Request is missing or has an invalid API key.")

    @mock.patch('requests.Session.request', side_effect=klaviyo_403_error_wrong_api_key)
    def test_403_error_wrong_api_key(self, klaviyo_403_error_wrong_api_key):

        try:
            utils_.authed_get("", "", "")
        except utils_.KlaviyoError as e:
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: The API key specified is invalid.")

    @mock.patch('requests.Session.request', side_effect=klaviyo_403_error_missing_api_key)
    def test_403_error_missing_api_key(self, klaviyo_403_error_missing_api_key):

        try:
            utils_.authed_get("", "", "")
        except utils_.KlaviyoError as e:
            self.assertEquals(str(e), "HTTP-error-code: 403, Error: You must specify an API key to make requests.")

    @mock.patch('requests.Session.request', side_effect=klaviyo_404_error)
    def test_404_error(self, klaviyo_404_error):

        try:
            utils_.authed_get("", "", "")
        except utils_.KlaviyoError as e:
            self.assertEquals(str(e), "HTTP-error-code: 404, Error: The requested resource doesn't exist.")

    @mock.patch('requests.Session.request', side_effect=klaviyo_500_error)
    def test_500_error(self, klaviyo_500_error):

        try:
            utils_.authed_get("", "", "")
        except utils_.KlaviyoError as e:
            self.assertEquals(str(e), "HTTP-error-code: 500, Error: Something is wrong on Klaviyo's end.")