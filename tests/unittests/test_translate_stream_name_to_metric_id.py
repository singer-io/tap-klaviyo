import tap_klaviyo
import unittest

class TestTranslateStreamNameToMetricId(unittest.TestCase):
    """
    Test translate_stream_to_metric_id works as intended
    """

    def test_translate_stream_name_to_metric_id(self):
        mock_catalog = {
            "streams": [
                {
                    "stream": "receive",
                    "tap_stream_id": "YimjbS"
                },
                {
                    "stream": "click",
                    "tap_stream_id": "U6uvyh"
                },
                {
                    "stream": "open",
                    "tap_stream_id": "Si4C3N"
                },
                {
                    "stream": "bounce",
                    "tap_stream_id": "R42wNy"
                },
                {
                    "stream": "unsubscribe",
                    "tap_stream_id": "YfT9Df"
                },
                {
                    "stream": "mark_as_spam",
                    "tap_stream_id": "VfTks9"
                },
                {
                    "stream": "dropped_email",
                    "tap_stream_id": "SQsqj3"
                },
                {
                    "stream": "unsub_list",
                    "tap_stream_id": "SM3z68"
                },
                {
                    "stream": "subscribe_list",
                    "tap_stream_id": "U3MqpH"
                },
                {
                    "stream": "subscribed_to_email",
                    "tap_stream_id": "XmL8Md"
                },
                {
                    "stream": "subscribed_to_text_message",
                    "tap_stream_id": "W2KTvQ"
                },
                {
                    "stream": "update_email_preferences",
                    "tap_stream_id": "WprQcH"
                },
                {
                    "stream": "global_exclusions",
                    "tap_stream_id": "global_exclusions"
                },
                {
                    "stream": "lists",
                    "tap_stream_id": "lists"
                },
                {
                    "stream": "campaigns",
                    "tap_stream_id": "campaigns"
                },
                {
                    "stream": "clicked_text_message",
                    "tap_stream_id": "UejZZm"
                },
                {
                    "stream": "unsubscribed_from_text_message",
                    "tap_stream_id": "TBiXnJ"
                },
                {
                    "stream": "received_text_message",
                    "tap_stream_id": "SrU8uS"
                },
                {
                    "stream": "sent_text_message",
                    "tap_stream_id": "XPbM2j"
                },
                {
                    "stream":  "failed_to_deliver_text_message",
                    "tap_stream_id": "RNT5Sf"
                },
                {
                    "stream": "failed_to_deliver_text_message_automated_response",
                    "tap_stream_id": "Rf2Tbd"
                },
                {
                    "stream": "received_automated_response_text_message",
                    "tap_stream_id": "VsTGNN"
                }
            ]
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
                "sent_text_message": {
                    "since": "2022-05-06T09:30:50Z"
                },
                "unsub_list": {
                    "since": "2022-05-06T05:04:00Z"
                },
                "clicked_text_message": {
                    "since": "2022-05-06T09:32:12Z"
                },
                "unsubscribe": {
                    "since": "2022-04-29T13:13:22Z"
                },
                "received_text_message": {
                    "since": "2022-05-05T06:24:07Z"
                },
                "subscribe_list": {
                    "since": "2024-05-06T18:40:13Z"
                },
                "failed_to_deliver_text_message": {
                    "since": "2022-05-05T08:19:12Z"
                },
                "subscribed_to_text_message": {
                    "since": "2022-05-06T09:30:51Z"
                },
                "subscribed_to_email": {
                    "since": "2024-05-06T18:40:14Z"
                },
                "unsubscribed_from_text_message": {
                    "since": "2024-10-17T13:48:50Z"
                },
                "update_email_preferences": {
                    "since": "2022-05-06T05:04:29Z"
                },
                "received_automated_response_text_message": {
                    "since": "2022-05-06T09:30:51Z"
                },
                "failed_to_deliver_text_message_automated_response": {
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
        
        actual_state = tap_klaviyo.translate_stream_to_metric_id(mock_state, mock_catalog)
        self.assertEqual(expected_state, actual_state)
