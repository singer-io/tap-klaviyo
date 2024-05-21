from tap_tester import menagerie

from tap_tester import connections, runner
from base import KlaviyoBaseTest

class AutomaticFieldsTest(KlaviyoBaseTest):
    """
    Ensure running the tap with all streams selected and all fields deselected results in the replication of just the 
    primary keys and replication keys (automatic fields).
    """
    
    def name(self):
        return "klaviyo_automatic_fields_test"
    
    def test_run(self):
        """
        • Verify we can deselect all fields except when inclusion=automatic, which is handled by base.py methods
        • Verify that only the automatic fields are sent to the target.
        • Verify that all replicated records have unique primary key values.
        """

        # Test account does not have data for untestable streams
        untestable_streams = {"unsubscribe", "mark_as_spam", "dropped_email"}
        expected_streams = self.expected_streams() - untestable_streams
        
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        
        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                                      if catalog.get('stream_name') in expected_streams]

        # Select all streams and no fields within streams
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs, select_all_fields=False)

        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()
        
        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_keys = self.expected_automatic_fields().get(stream)
                expected_primary_keys = self.expected_primary_keys()[stream]
                
                # collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row['data'].keys())
                                        for row in data.get('messages', [])]
                primary_keys_list = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in data.get('messages')
                                       if message.get('action') == 'upsert']
                
                unique_primary_keys_list = set(primary_keys_list)
                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream min limit")

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, 
                                        actual_keys, 
                                        msg="The fields sent to the target are not the automatic fields")
                    
                #Verify that all replicated records have unique primary key values.
                self.assertEqual(len(primary_keys_list), 
                                    len(unique_primary_keys_list), 
                                    msg="Replicated record does not have unique primary key values.")
