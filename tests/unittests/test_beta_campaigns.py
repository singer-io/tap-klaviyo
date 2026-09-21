import unittest
from unittest import mock

import tap_klaviyo
from tap_klaviyo.utils import STREAM_PARAMS_MAP, build_variation_content, get_campaign_messages_pull


class TestBetaCampaigns(unittest.TestCase):

    def test_campaigns_stream_params_match_beta_endpoint(self):
        self.assertEqual(
            STREAM_PARAMS_MAP["campaigns"],
            [{"fields[campaign]": "created_at,definition,updated_at"}]
        )

    def test_campaigns_schema_matches_beta_definition_shape(self):
        schema = tap_klaviyo.load_schema("campaigns")
        definition_props = schema["properties"]["definition"]["properties"]

        self.assertEqual(
            set(definition_props.keys()),
            {"name", "builder", "archived", "color", "description", "send_settings"}
        )
        self.assertEqual(
            set(definition_props["send_settings"]["properties"].keys()),
            {
                "send_timezone",
                "send_strategy",
                "send_passed_rltz_immediately",
                "exit_condition_enabled",
                "exit_condition_conversion_metric_id",
            }
        )


class TestBuildVariationContent(unittest.TestCase):
    """Per https://developers.klaviyo.com/en/reference/campaigns_omni_api_overview,
    each campaign-variation's `details` shape depends on its channel. All four
    documented channels should be mapped, not just email."""

    def test_email_channel_maps_email_fields(self):
        details = {
            "template_id": "tmpl_1",
            "subject": "Hi",
            "preview_text": "preview",
            "from_email": "a@b.com",
            "from_label": "Brand",
            "reply_to_email": "reply@b.com",
            "cc_email": "cc@b.com",
            "bcc_email": "bcc@b.com",
        }
        self.assertEqual(build_variation_content("email", details), details)

    def test_sms_channel_maps_sms_fields(self):
        details = {
            "template_id": "tmpl_2",
            "body": "Sale today!",
            "shorten_links": True,
            "include_contact_card": False,
        }
        self.assertEqual(build_variation_content("sms", details), details)

    def test_push_channel_maps_push_fields(self):
        details = {
            "title": "Flash sale",
            "body": "20% off",
            "static_asset_id": 123,
            "ios_deep_link": "app://ios",
            "android_deep_link": "app://android",
            "sound": True,
            "badge": False,
        }
        self.assertEqual(build_variation_content("push", details), details)

    def test_whatsapp_channel_maps_whatsapp_fields(self):
        details = {"template_id": "wt_1", "shorten_links": True}
        self.assertEqual(build_variation_content("whatsapp", details), details)

    def test_unknown_channel_still_passes_through_details(self):
        # Unrecognized/future channels are not dropped either -- `details` is
        # returned as-is so nothing is silently lost.
        self.assertEqual(
            build_variation_content("carrier_pigeon", {"foo": "bar"}),
            {"foo": "bar"}
        )

    def test_channel_key_is_excluded_from_content(self):
        # `channel` is stored separately on the message record, so it should
        # not also be duplicated inside `content`.
        details = {"channel": "sms", "body": "Sale today!"}
        self.assertEqual(build_variation_content("sms", details), {"body": "Sale today!"})

    def test_sms_channel_includes_undocumented_fields(self):
        # Real Klaviyo sandbox responses include several sms fields beyond the
        # commonly-documented subset (e.g. add_org_prefix/cost/message_hierarchy);
        # these must not be dropped.
        details = {
            "template_id": "tmpl_2",
            "body": "Sale today!",
            "shorten_links": True,
            "include_contact_card": False,
            "add_org_prefix": True,
            "add_info_link": True,
            "add_opt_out_language": True,
            "cost": None,
            "message_hierarchy": None,
            "mms_static_image_asset_id": None,
            "mms_dynamic_image_template": "",
            "text_message_type": "SMS",
        }
        self.assertEqual(build_variation_content("sms", details), details)

    def test_missing_fields_are_simply_absent(self):
        # No fields are invented for a channel -- content mirrors exactly
        # what Klaviyo returned.
        self.assertEqual(build_variation_content("sms", {}), {})


class TestGetCampaignMessagesPull(unittest.TestCase):
    """Exercises the full pull path: flattening message attributes, joining
    page-local `included` variations, and writing every paginated record --
    not just the isolated `build_variation_content` helper."""

    def _mock_response(self, messages, included, next_url=None):
        resp = mock.Mock()
        resp.json.return_value = {
            "data": messages,
            "included": included,
            "links": {"next": next_url},
        }
        return resp

    @mock.patch("tap_klaviyo.utils.singer.write_record")
    @mock.patch("tap_klaviyo.utils.authed_get")
    def test_flattens_joins_variation_and_writes_every_paginated_record(
        self, mocked_authed_get, mocked_write_record
    ):
        page_1_message = {
            "type": "campaign-message",
            "id": "msg_1",
            "attributes": {
                "created": "2024-01-01T00:00:00Z",
                "updated": "2024-01-02T00:00:00Z",
                "definition": {"name": "Sample", "status": "draft"},
            },
            "relationships": {
                "campaign": {"data": {"type": "campaign", "id": "camp_1"}},
                "campaign-variations": {"data": [{"type": "campaign-variation", "id": "var_1"}]},
            },
        }
        page_1_variation = {
            "type": "campaign-variation",
            "id": "var_1",
            "attributes": {
                "definition": {
                    "name": "Variation A",
                    "details": {"channel": "sms", "body": "Sale today!"},
                }
            },
        }
        page_2_message = {
            "type": "campaign-message",
            "id": "msg_2",
            "attributes": {
                "created": "2024-01-03T00:00:00Z",
                "updated": "2024-01-04T00:00:00Z",
                "definition": {"name": "Sample 2", "status": "sent"},
            },
            "relationships": {
                "campaign": {"data": {"type": "campaign", "id": "camp_2"}},
                "campaign-variations": {"data": []},
            },
        }

        mocked_authed_get.side_effect = [
            self._mock_response(
                [page_1_message], [page_1_variation],
                next_url="https://a.klaviyo.com/api/campaign-messages/?page%5Bcursor%5D=next"
            ),
            self._mock_response([page_2_message], []),
        ]

        stream = {
            "stream": "campaign_messages",
            "schema": tap_klaviyo.load_schema("campaign_messages"),
            "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
        }

        get_campaign_messages_pull(stream, "https://a.klaviyo.com/api/campaign-messages/", {})

        self.assertEqual(mocked_authed_get.call_count, 2)
        self.assertEqual(mocked_write_record.call_count, 2)

        written_1 = mocked_write_record.call_args_list[0][0][1]
        self.assertEqual(written_1["id"], "msg_1")
        self.assertEqual(written_1["campaign_id"], "camp_1")
        self.assertEqual(written_1["channel"], "sms")
        self.assertEqual(written_1["label"], "Variation A")
        self.assertEqual(written_1["content"], {"body": "Sale today!"})
        self.assertEqual(written_1["name"], "Sample")
        self.assertEqual(len(written_1["variations"]), 1)
        self.assertEqual(written_1["variations"][0]["id"], "var_1")

        written_2 = mocked_write_record.call_args_list[1][0][1]
        self.assertEqual(written_2["id"], "msg_2")
        self.assertEqual(written_2["campaign_id"], "camp_2")
        self.assertNotIn("channel", written_2)
        self.assertNotIn("content", written_2)
        self.assertNotIn("variations", written_2)

    @mock.patch("tap_klaviyo.utils.singer.write_record")
    @mock.patch("tap_klaviyo.utils.authed_get")
    def test_preserves_every_variation_on_a_multi_variation_message(
        self, mocked_authed_get, mocked_write_record
    ):
        # Real sandbox data shows a single message can carry 2+ same-channel
        # (A/B test) variations; taking only the first would silently drop
        # the rest. All variations must be preserved via `variations`, while
        # `channel`/`label`/`content` mirror the first for backward compat.
        message = {
            "type": "campaign-message",
            "id": "msg_ab",
            "attributes": {
                "created": "2024-01-01T00:00:00Z",
                "updated": "2024-01-02T00:00:00Z",
                "definition": {"name": "AB test message", "status": "sent"},
            },
            "relationships": {
                "campaign": {"data": {"type": "campaign", "id": "camp_ab"}},
                "campaign-variations": {
                    "data": [
                        {"type": "campaign-variation", "id": "var_a"},
                        {"type": "campaign-variation", "id": "var_b"},
                    ]
                },
            },
        }
        variation_a = {
            "type": "campaign-variation",
            "id": "var_a",
            "attributes": {
                "definition": {
                    "name": "SMS_camp_A",
                    "details": {"channel": "sms", "body": "Variant A"},
                }
            },
        }
        variation_b = {
            "type": "campaign-variation",
            "id": "var_b",
            "attributes": {
                "definition": {
                    "name": "Copy of SMS_camp_A",
                    "details": {"channel": "sms", "body": "Variant B"},
                }
            },
        }

        mocked_authed_get.side_effect = [
            self._mock_response([message], [variation_a, variation_b]),
        ]

        stream = {
            "stream": "campaign_messages",
            "schema": tap_klaviyo.load_schema("campaign_messages"),
            "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
        }

        get_campaign_messages_pull(stream, "https://a.klaviyo.com/api/campaign-messages/", {})

        written = mocked_write_record.call_args_list[0][0][1]
        self.assertEqual(written["channel"], "sms")
        self.assertEqual(written["label"], "SMS_camp_A")
        self.assertEqual(written["content"], {"body": "Variant A"})
        self.assertEqual(len(written["variations"]), 2)
        self.assertEqual(
            [v["id"] for v in written["variations"]], ["var_a", "var_b"]
        )
        self.assertEqual(
            [v["content"]["body"] for v in written["variations"]],
            ["Variant A", "Variant B"]
        )

    @mock.patch("tap_klaviyo.utils.singer.write_record")
    @mock.patch("tap_klaviyo.utils.authed_get")
    def test_handles_single_variation_object_not_wrapped_in_a_list(
        self, mocked_authed_get, mocked_write_record
    ):
        # Klaviyo's JSON:API relationship data can be a single object instead
        # of a one-item list; this must not crash the sync.
        message = {
            "type": "campaign-message",
            "id": "msg_single",
            "attributes": {
                "created": "2024-01-01T00:00:00Z",
                "updated": "2024-01-02T00:00:00Z",
                "definition": {"name": "Single", "status": "sent"},
            },
            "relationships": {
                "campaign": {"data": {"type": "campaign", "id": "camp_single"}},
                "campaign-variations": {"data": {"type": "campaign-variation", "id": "var_1"}},
            },
        }
        variation = {
            "type": "campaign-variation",
            "id": "var_1",
            "attributes": {
                "definition": {
                    "name": "Variation A",
                    "details": {"channel": "email", "subject": "Hi"},
                }
            },
        }

        mocked_authed_get.side_effect = [self._mock_response([message], [variation])]

        stream = {
            "stream": "campaign_messages",
            "schema": tap_klaviyo.load_schema("campaign_messages"),
            "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
        }

        get_campaign_messages_pull(stream, "https://a.klaviyo.com/api/campaign-messages/", {})

        written = mocked_write_record.call_args_list[0][0][1]
        self.assertEqual(written["channel"], "email")
        self.assertEqual(len(written["variations"]), 1)

    @mock.patch("tap_klaviyo.utils.singer.write_record")
    @mock.patch("tap_klaviyo.utils.authed_get")
    def test_nullable_variation_details_do_not_crash_the_sync(
        self, mocked_authed_get, mocked_write_record
    ):
        # A draft or otherwise incomplete variation can have `details: null`;
        # this must not raise AttributeError on the following .get() calls.
        message = {
            "type": "campaign-message",
            "id": "msg_draft",
            "attributes": {
                "created": "2024-01-01T00:00:00Z",
                "updated": "2024-01-02T00:00:00Z",
                "definition": {"name": "Draft", "status": "draft"},
            },
            "relationships": {
                "campaign": {"data": {"type": "campaign", "id": "camp_draft"}},
                "campaign-variations": {"data": [{"type": "campaign-variation", "id": "var_1"}]},
            },
        }
        variation = {
            "type": "campaign-variation",
            "id": "var_1",
            "attributes": {"definition": {"name": "Draft variation", "details": None}},
        }

        mocked_authed_get.side_effect = [self._mock_response([message], [variation])]

        stream = {
            "stream": "campaign_messages",
            "schema": tap_klaviyo.load_schema("campaign_messages"),
            "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
        }

        get_campaign_messages_pull(stream, "https://a.klaviyo.com/api/campaign-messages/", {})

        written = mocked_write_record.call_args_list[0][0][1]
        self.assertIsNone(written["channel"])
        self.assertEqual(written["content"], {})
