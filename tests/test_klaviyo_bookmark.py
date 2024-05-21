from tap_tester import menagerie

from tap_tester import connections, runner
from base import KlaviyoBaseTest

class BookmarkTest(KlaviyoBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""

    @staticmethod
    def name():
        return "tap_tester_klaviyo_bookmark_test"
    
    def test_run(self):
        """
        Verify that for each stream you can do a sync which records bookmarks.
        That the bookmark is the maximum value sent to the target for the replication key.
        That a second sync respects the bookmark
            All data of the second sync is >= the bookmark from the first sync
            The number of records in the 2nd sync is less then the first (This assumes that
                new data added to the stream is done at a rate slow enough that you haven't
                doubled the amount of data from the start date to the first sync between
                the first sync and second sync run in this test)

        Verify that for full table stream, all data replicated in sync 1 is replicated again in sync 2.

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """
        
        # Test account does not have data for untestable streams
        untestable_streams = {"unsubscribe", "mark_as_spam", "dropped_email"}
        expected_streams = self.expected_streams() - untestable_streams
        
        expected_bookmark_keys = self.expected_bookmark_keys()
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()
        
        ##########################################################################
        # First Sync
        ##########################################################################
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                            if catalog.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################

        new_states = {'bookmarks': dict()}
        simulated_states = self.calculated_states_by_stream(
            first_sync_bookmarks)
        for stream, new_state in simulated_states.items():
            new_states['bookmarks'][stream] = new_state
        menagerie.set_state(conn_id, new_states)

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Test By Stream
        ##########################################################################


        for stream in expected_streams:            
            with self.subTest(stream=stream):

                # expected values
                expected_replication_method = expected_replication_methods[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(
                                           stream, {}).get('messages', [])
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(
                                            stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']
                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                

                if expected_replication_method == self.INCREMENTAL:

                    # collect information specific to incremental streams from syncs 1 & 2
                    replication_key = list(expected_replication_keys[stream])[0] #Key in which state has been saved in state file
                    record_replication_key = list(expected_bookmark_keys[stream])[0]

                    first_bookmark_value = first_bookmark_key_value.get(replication_key)
                    second_bookmark_value = second_bookmark_key_value.get(replication_key)
                    
                    # We are writing bookmark value 1 second less than the maximum bookmark value.
                    # Because API returns records in which the replication key value is greater than the last saved bookmark value.
                    # So, sometimes records with the same bookmark value may be lost.
                    # That's why here we are adding 1 second to both bookmark values.
                    first_bookmark_value_ts = self.dt_to_ts(first_bookmark_value) + 1
                    
                    second_bookmark_value_ts = self.dt_to_ts(second_bookmark_value) + 1
                    
                    simulated_bookmark_value = self.dt_to_ts(new_states['bookmarks'][stream][replication_key])

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)
                    self.assertIsNotNone(first_bookmark_value)
                    
                    self.assertIsNotNone(second_bookmark_key_value)
                    self.assertIsNotNone(second_bookmark_value)

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    # assumes no changes to data during test
                    self.assertEqual(second_bookmark_value,
                                     first_bookmark_value)
                    
                    for record in first_sync_messages:

                        # Verify the first sync bookmark value is the max replication key value for a given stream
                        replication_key_value = record.get(record_replication_key)
                        self.assertLessEqual(
                            replication_key_value, first_bookmark_value_ts,
                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                    for record in second_sync_messages:
                        # Verify the second sync replication key value is Greater or Equal to the first sync bookmark
                        replication_key_value = record.get(record_replication_key)
                        self.assertGreaterEqual(replication_key_value, simulated_bookmark_value,
                                                msg="Second sync records do not repect the previous bookmark.")

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(
                            replication_key_value, second_bookmark_value_ts,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                    # verify that you get less data the 2nd time around
                    self.assertLess(
                        second_sync_count,
                        first_sync_count,
                        msg="second sync didn't have less records, bookmark usage not verified")

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

                else:

                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(
                            stream, expected_replication_method)
                    )

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(
                    second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))