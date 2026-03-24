import tap_klaviyo
import unittest
from unittest import mock
from requests import Response
import json


def get_mock_http_response(status_code, contents):
    contents = json.dumps(contents)
    response = Response()
    response.status_code = status_code
    response._content = contents.encode()
    return response


class TestTranslateStreamNameToMetricId(unittest.TestCase):
    """
    Test translate_stream_to_metric_id works as intended
    """

    def test_translate_stream_name_to_metric_id(self):
        mock_catalog = {
            "receive": {
                "tap_strem_id": "YimjbS"
            },
            "click": {
                "tap_strem_id": "U6uvyh"
            },
            "open": {
                "tap_strem_id": "Si4C3N"
            },
            "bounce": {
                "tap_strem_id": "R42wNy"
            },
            "unsubscribe": {
                "tap_strem_id": "YfT9Df"
            },
            "mark_as_spam": {
                "tap_strem_id": "VfTks9"
            },
            "dropped_email": {
                "tap_strem_id": "SQsqj3"
            },
            "unsub_list": {
                "tap_strem_id": "SM3z68"
            },
            "subscribe_list": {
                "tap_strem_id": "U3MqpH"
            },
            "subscribed_to_email": {
                "tap_strem_id": "XmL8Md"
            },
            "subscribed_to_sms": {
                "tap_strem_id": "W2KTvQ"
            },
            "update_email_preferences": {
                "tap_strem_id": "WprQcH"
            },
            "global_exclusions": {
                "tap_strem_id": "global_exclusions"
            },
            "lists": {
                "tap_strem_id": "lists"
            },
            "campaigns": {
                "tap_strem_id": "campaigns"
            },
            'clicked_sms': {
                "tap_strem_id": "UejZZm"
            },
            'unsubscribed_from_sms': {
                "tap_strem_id": "TBiXnJ"
            },
            'received_sms': {
                "tap_strem_id": "SrU8uS"
            },
            'sent_sms': {
                "tap_strem_id": "XPbM2j"
            },
            'failed_to_deliver': {
                "tap_strem_id": "RNT5Sf"
            },
            'failed_to_deliver_automated_response': {
                "tap_strem_id": "Rf2Tbd"
            },
            'received_automated_response': {
                "tap_strem_id": "VsTGNN"
            }
        }
        mock_state = {
            "bookmarks": {
                "open": {
                    "since": "2022-06-01T08:59:19Z"
                },
                "click": {
                    "since": "2022-05-06T05:04:40Z"
                },
                "bounce": {
                    "since": "2022-05-27T10:09:02Z"
                },
                "receive": {
                    "since": "2022-05-27T12:16:24Z"
                },
                "sent_sms": {
                    "since": "2022-05-06T09:30:50Z"
                },
                "unsub_list": {
                    "since": "2022-05-06T05:04:00Z"
                },
                "clicked_sms": {
                    "since": "2022-05-06T09:32:12Z"
                },
                "unsubscribe": {
                    "since": "2022-04-29T13:13:22Z"
                },
                "received_sms": {
                    "since": "2022-05-05T06:24:07Z"
                },
                "subscribe_list": {
                    "since": "2024-05-06T18:40:13Z"
                },
                "failed_to_deliver": {
                    "since": "2022-05-05T08:19:12Z"
                },
                "subscribed_to_sms": {
                    "since": "2022-05-06T09:30:51Z"
                },
                "subscribed_to_email": {
                    "since": "2024-05-06T18:40:14Z"
                },
                "unsubscribed_from_sms": {
                    "since": "2024-10-17T13:48:50Z"
                },
                "update_email_preferences": {
                    "since": "2022-05-06T05:04:29Z"
                },
                "received_automated_response": {
                    "since": "2022-05-06T09:30:51Z"
                },
                "failed_to_deliver_automated_response": {
                    "since": "2022-05-05T10:01:02Z"
                }
            },
            "target_state": {
                "lists": "fullLoadEnd",
                "R42wNy": "fullLoadEnd",
                "RNT5Sf": "fullLoadEnd",
                "Rf2Tbd": "fullLoadEnd",
                "SM3z68": "fullLoadEnd",
                "Si4C3N": "fullLoadEnd",
                "SrU8uS": "fullLoadEnd",
                "TBiXnJ": "fullLoadEnd",
                "U3MqpH": "fullLoadEnd",
                "U6uvyh": "fullLoadEnd",
                "UejZZm": "fullLoadEnd",
                "VsTGNN": "fullLoadEnd",
                "W2KTvQ": "fullLoadEnd",
                "WprQcH": "fullLoadEnd",
                "XPbM2j": "fullLoadEnd",
                "XmL8Md": "fullLoadEnd",
                "YfT9Df": "fullLoadEnd",
                "YimjbS": "fullLoadEnd",
                "campaigns": "fullLoadEnd",
                "global_exclusions": "fullLoadEnd"
            },
            "target_total_batches": {}
        }

        expected_state = {
            "bookmarks": {
                "Si4C3N": {
                    "since": "2022-06-01T08:59:19Z"
                },
                "U6uvyh": {
                    "since": "2022-05-06T05:04:40Z"
                },
                "R42wNy": {
                    "since": "2022-05-27T10:09:02Z"
                },
                "YimjbS": {
                    "since": "2022-05-27T12:16:24Z"
                },
                "XPbM2j": {
                    "since": "2022-05-06T09:30:50Z"
                },
                "SM3z68": {
                    "since": "2022-05-06T05:04:00Z"
                },
                "UejZZm": {
                    "since": "2022-05-06T09:32:12Z"
                },
                "YfT9Df": {
                    "since": "2022-04-29T13:13:22Z"
                },
                "SrU8uS": {
                    "since": "2022-05-05T06:24:07Z"
                },
                "U3MqpH": {
                    "since": "2024-05-06T18:40:13Z"
                },
                "RNT5Sf": {
                    "since": "2022-05-05T08:19:12Z"
                },
                "W2KTvQ": {
                    "since": "2022-05-06T09:30:51Z"
                },
                "XmL8Md": {
                    "since": "2024-05-06T18:40:14Z"
                },
                "TBiXnJ": {
                    "since": "2024-10-17T13:48:50Z"
                },
                "WprQcH": {
                    "since": "2022-05-06T05:04:29Z"
                },
                "VsTGNN": {
                    "since": "2022-05-06T09:30:51Z"
                },
                "Rf2Tbd": {
                    "since": "2022-05-05T10:01:02Z"
                }
            },
            "target_state": {
                "lists": "fullLoadEnd",
                "R42wNy": "fullLoadEnd",
                "RNT5Sf": "fullLoadEnd",
                "Rf2Tbd": "fullLoadEnd",
                "SM3z68": "fullLoadEnd",
                "Si4C3N": "fullLoadEnd",
                "SrU8uS": "fullLoadEnd",
                "TBiXnJ": "fullLoadEnd",
                "U3MqpH": "fullLoadEnd",
                "U6uvyh": "fullLoadEnd",
                "UejZZm": "fullLoadEnd",
                "VsTGNN": "fullLoadEnd",
                "W2KTvQ": "fullLoadEnd",
                "WprQcH": "fullLoadEnd",
                "XPbM2j": "fullLoadEnd",
                "XmL8Md": "fullLoadEnd",
                "YfT9Df": "fullLoadEnd",
                "YimjbS": "fullLoadEnd",
                "campaigns": "fullLoadEnd",
                "global_exclusions": "fullLoadEnd"
            },
            "target_total_batches": {}
        }
        
        actual_state = translate_stream_to_metric_id(mock_state, mock_catalog)
        self.assertEqual(expected_state, actual_state))
