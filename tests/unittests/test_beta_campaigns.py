import unittest

import tap_klaviyo
from tap_klaviyo.utils import STREAM_PARAMS_MAP, build_variation_content


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

    def test_unknown_channel_returns_empty_dict(self):
        self.assertEqual(build_variation_content("carrier_pigeon", {"foo": "bar"}), {})

    def test_missing_fields_default_to_none(self):
        self.assertEqual(
            build_variation_content("sms", {}),
            {
                "template_id": None,
                "body": None,
                "shorten_links": None,
                "include_contact_card": None,
            }
        )
