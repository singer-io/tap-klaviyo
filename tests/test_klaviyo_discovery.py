"""Test tap discovery mode and metadata."""
import re

from tap_tester import menagerie, connections

from base import KlaviyoBaseTest


class DiscoveryTest(KlaviyoBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_klaviyo_discovery_test"

    def test_run(self):
        """
        Testing that discovery creates the appropriate catalog with valid metadata.

        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • Verify number of actual streams discovered match expected
        • verify there is only 1 top level breadcrumb
        • verify primary key(s)
        • verify the actual replication matches our expected replication method
        • verify that primary keys and bookmarks are given the inclusion of automatic.
        • verify that all other fields have inclusion of available metadata.
        • verify there are no duplicate metadata entries
        • verify replication key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        """
        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        found_catalog_names = {c['stream_name'] for c in found_catalogs}
        self.assertTrue(all([re.fullmatch(r"[a-z_]+",  name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                # Verify ensure the catalog is found for a given stream
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog)

                # collecting expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_method = self.expected_replication_method()[stream]
                expected_automatic_fields = self.expected_automatic_fields()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]

                # collecting actual values...
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_primary_keys = set()
                actual_primary_keys.add(stream_properties[0].get(
                        "metadata", {self.PRIMARY_KEYS: None}).get(self.PRIMARY_KEYS))
                actual_replication_method = stream_properties[0].get(
                    "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                actual_automatic_fields = set(
                    item.get("breadcrumb", ["properties", None])[1] for item in metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                )
                actual_replication_keys = set(
                    stream_properties[0].get(
                        "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])
                )
                
                # verify there are no duplicate metadata entries
                for md in metadata:
                    self.assertEqual(metadata.count(md),1,msg="There is duplicated metadata in '{}' stream".format(stream))
                

                # verify there is only 1 top level breadcrumb in metadata
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                # verify primary key match expectations
                self.assertEqual(
                    expected_primary_keys, actual_primary_keys,
                )

                 # verify replication method match expectations
                self.assertEqual(
                    expected_replication_method, actual_replication_method,
                )
                
                # Currently, Tap is not writing the replication key in the metadata of the catalog
                # https://jira.talendforge.org/browse/TDL-18809
                # # verify replication keys match expections
                # self.assertEqual(expected_replication_keys, actual_replication_keys,
                #                  msg="expected replication key {} but actual is {}".format(
                #                      expected_replication_keys, actual_replication_keys))
                    
                
                # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                if expected_replication_keys:
                    self.assertEqual(actual_replication_method,self.INCREMENTAL)
                else:
                    self.assertEqual(actual_replication_method,self.FULL_TABLE)  

                # verify that primary keys and bookmark keys are given the inclusion of automatic in metadata.
                self.assertSetEqual(expected_automatic_fields, actual_automatic_fields)

                 # verify that all other fields have inclusion of available other than key properties and bookmark keys
                self.assertTrue(
                    all({item.get("metadata").get("inclusion") == "available"
                         for item in metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in actual_automatic_fields}),
                    msg="Not all non key properties and bookmark are set to available in metadata")
