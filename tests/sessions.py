from requests import Request

from tap_jira.context import AuthHandler


def test_api_key_encoding():
    auth_handler: AuthHandler = AuthHandler(api_key="test")
    assert auth_handler.b64_api_key == b'dGVzdDo='

def test_request_authorization_header():
    auth_handler: AuthHandler = AuthHandler(api_key="test")
    updated_request: Request = auth_handler.handle(Request(
        method="GET",
        url="www.google.com"
    ))
    assert updated_request.headers
    assert updated_request.headers["Authorization"]
    assert updated_request.headers["Authorization"] == "Basic dGVzdDo="