import importlib.util
import pathlib
import unittest
from unittest import mock


SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "compare_api_revisions.py"
SPEC = importlib.util.spec_from_file_location("compare_api_revisions", SCRIPT_PATH)
compare_api_revisions = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(compare_api_revisions)


class TestCompareApiRevisions(unittest.TestCase):

    def test_compare_ids_matches_campaigns_by_business_key_instead_of_page_order(self):
        old = [
            {
                "id": "old_a",
                "attributes": {
                    "name": "Campaign A",
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-02T00:00:00Z",
                },
            },
            {
                "id": "old_b",
                "attributes": {
                    "name": "Campaign B",
                    "created_at": "2024-01-03T00:00:00Z",
                    "updated_at": "2024-01-04T00:00:00Z",
                },
            },
        ]
        new = [
            {
                "id": "extra",
                "attributes": {
                    "definition": {"name": "Campaign Z"},
                    "created_at": "2024-01-05T00:00:00Z",
                    "updated_at": "2024-01-06T00:00:00Z",
                },
            },
            {
                "id": "old_b",
                "attributes": {
                    "definition": {"name": "Campaign B"},
                    "created_at": "2024-01-03T00:00:00Z",
                    "updated_at": "2024-01-04T00:00:00Z",
                },
            },
            {
                "id": "old_a",
                "attributes": {
                    "definition": {"name": "Campaign A"},
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-02T00:00:00Z",
                },
            },
        ]

        ids_stable, mismatches = compare_api_revisions.compare_ids("campaigns", old, new)

        self.assertTrue(ids_stable)
        self.assertEqual(mismatches, [])

    def test_compare_ids_matches_campaign_messages_across_nested_and_flat_shapes(self):
        old = [
            {
                "id": "legacy_id",
                "campaign_id": "camp_1",
                "attributes": {
                    "name": "Message A",
                    "created": "2024-01-01T00:00:00Z",
                    "updated": "2024-01-02T00:00:00Z",
                    "channel": "sms",
                },
            }
        ]
        new = [
            {
                "id": "new_id",
                "attributes": {
                    "created": "2024-01-01T00:00:00Z",
                    "updated": "2024-01-02T00:00:00Z",
                    "definition": {
                        "name": "Message A",
                        "details": {"channel": "sms", "body": "Sale today!"},
                    },
                },
                "relationships": {
                    "campaign": {"data": {"id": "camp_1", "type": "campaign"}},
                },
            }
        ]

        ids_stable, mismatches = compare_api_revisions.compare_ids("campaign_messages", old, new)

        self.assertFalse(ids_stable)
        self.assertEqual(len(mismatches), 1)
        self.assertEqual(mismatches[0]["old_id"], "legacy_id")
        self.assertEqual(mismatches[0]["new_id"], "new_id")

    @mock.patch.object(compare_api_revisions, "fetch")
    def test_get_nested_campaign_messages_annotates_parent_campaign_id(self, mocked_fetch):
        response = mock.Mock()
        response.status_code = 200
        response.json.side_effect = [
            {"data": [{"id": "msg_1", "attributes": {}}]},
            {"data": [{"id": "msg_2", "attributes": {}}]},
        ]
        mocked_fetch.return_value = response

        error, records = compare_api_revisions.get_nested_campaign_messages(
            "api_key",
            "2024-10-15",
            ["camp_1", "camp_2"],
        )

        self.assertIsNone(error)
        self.assertEqual(
            records,
            [
                {"id": "msg_1", "attributes": {}, "campaign_id": "camp_1"},
                {"id": "msg_2", "attributes": {}, "campaign_id": "camp_2"},
            ],
        )
