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


class TestFieldsInclusionInMetadata(unittest.TestCase):
    """
    Test cases to verify inclusion value is available for fields in metadata and automatic for key_property
    """

    @mock.patch("tap_klaviyo.get_all_using_next")
    def test_fields_inclusion_in_metadata(self, mock_get_all_using_next):
        header = "abc"
        streams = [{"attributes": {"name": "Received Email"}, "id": "UfdfRD"},
                   {"attributes": {"name": "Clicked Email"}, "id": "UrdfRD"},
                   {"attributes": {"name": "Opened Email"}, "id": "UfdfRu"},
                   {"attributes": {"name": "Bounced Email"}, "id": "RfdfRD"},
                   {"attributes": {"name": "Unsubscribed"}, "id": "UddfRD"},
                   {"attributes": {"name": "Marked Email as Spam"}, "id": "UffgbD"},
                   {"attributes": {"name": "Unsubscribed from List"}, "id": "UsfewD"},
                   {"attributes": {"name": "Subscribed to List"}, "id": "UrDfwD"},
                   {"attributes": {"name": "Updated Email Preferences"}, "id": "AdfDfD"},
                   {"attributes": {"name": "Dropped Email"}, "id": "AfdfdD"}]
        full_table_stream = ["global_exclusions", "lists", "campaigns"]
        bookmark_key = 'timestamp'

        mock_get_all_using_next.return_value = [get_mock_http_response(
            200, {"data": streams})]

        # Get catalog
        catalog = tap_klaviyo.discover(header)
        catalog = catalog['streams']

        for catalog_entry in catalog:
            # Breadcrumbs of key_properties
            automatic_prop_breadcrumbs = {('properties', x) for x in catalog_entry['key_properties']}

            # Breadcrumbs of bookmark_key
            if catalog_entry['stream'] not in full_table_stream:
                automatic_prop_breadcrumbs.add(('properties', bookmark_key))

            for field in catalog_entry['metadata']:
                if field['breadcrumb'] in automatic_prop_breadcrumbs:
                    # Verifying that all key properties and bookmark keys are automatic inclusion
                    self.assertEqual(field['metadata']['inclusion'], 'automatic')
                else:
                    # Verifying that all normal fields are available inclusion
                    self.assertEqual(field['metadata']['inclusion'], 'available')
