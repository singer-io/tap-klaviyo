import unittest

import tap_klaviyo
from tap_klaviyo.utils import STREAM_PARAMS_MAP


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
