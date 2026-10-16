"""Verify recovery from a partially checkpointed sync."""
from collections import Counter
from datetime import datetime

from tap_tester import connections, menagerie, runner

from base import KlaviyoBaseTest


class InterruptedSyncTest(KlaviyoBaseTest):
    """Resume completed, interrupted, and unstarted streams without losing data."""

    @staticmethod
    def name():
        return "tap_tester_klaviyo_interrupted_sync_test"

    def test_run(self):
        """Simulate interruption using page-level bookmarks from stable test data.

        Requires data in every selected stream and three incremental streams,
        including one with multiple timestamps to verify partial resumption.
        Excludes unsubscribe, mark_as_spam, and dropped_email, which have no
        test-account data, matching the other Klaviyo integration tests.

        Klaviyo does not prioritize the interrupted stream on resume. It takes
        selected streams in catalog order, which this test verifies against the
        uninterrupted sync order.
        """
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        untestable_streams = {'unsubscribe', 'mark_as_spam', 'dropped_email'}
        test_catalogs = [
            catalog for catalog in found_catalogs
            if catalog['stream_name'] not in untestable_streams
        ]
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        self.run_and_verify_sync(conn_id)
        full_sync_order = runner.get_stream_sync_order_from_target()
        full_sync_output = runner.get_records_from_target_output()
        full_sync_state = menagerie.get_state(conn_id)
        self.assertIsInstance(full_sync_state, dict)
        self.assertIsInstance(full_sync_state.get('bookmarks'), dict)
        self.assertTrue(full_sync_state['bookmarks'])
        self.assertIsNone(full_sync_state.get('currently_syncing'))
        stream_ids = {
            catalog['stream_name']: catalog['tap_stream_id']
            for catalog in test_catalogs
        }
        full_records = {
            stream: [message['data'] for message in
                     full_sync_output.get(stream, {}).get('messages', [])
                     if message.get('action') == 'upsert']
            for stream in stream_ids
        }
        self.assertCountEqual(full_sync_order, list(stream_ids))
        start_timestamp = self.dt_to_ts(self.get_properties()['start_date'])
        for stream, records in full_records.items():
            with self.subTest(stream=stream, sync='baseline'):
                self.assertGreater(
                    len(records), 0,
                    msg="Every selected stream needs data to verify recovery."
                )
                if self.is_incremental(stream):
                    self.assertIn(stream_ids[stream], full_sync_state['bookmarks'])
                    self.assertIsInstance(
                        full_sync_state['bookmarks'][stream_ids[stream]]['since'], str
                    )
                    for record in records:
                        self.assertGreaterEqual(record['timestamp'], start_timestamp)
                else:
                    self.assertNotIn(stream_ids[stream], full_sync_state['bookmarks'])
        populated_incremental_streams = [
            stream for stream in full_sync_order
            if self.is_incremental(stream) and full_records[stream]
        ]
        self.assertGreaterEqual(
            len(populated_incremental_streams), 3,
            msg="Need populated completed, interrupted, and unstarted streams."
        )
        interruptible_streams = [
            stream for stream in populated_incremental_streams[1:-1]
            if max(record['timestamp'] for record in full_records[stream]) -
            min(record['timestamp'] for record in full_records[stream]) > 1
        ]
        self.assertTrue(
            interruptible_streams,
            msg="Need an intermediate stream with timestamps over a second apart."
        )
        interrupted_stream = interruptible_streams[0]
        completed_stream = populated_incremental_streams[0]
        timestamps = sorted({
            record['timestamp'] for record in full_records[interrupted_stream]
        })
        checkpoint_timestamps = [
            timestamp for timestamp in timestamps if timestamp - 1 > timestamps[0]
        ]
        interrupted_timestamp = checkpoint_timestamps[
            (len(checkpoint_timestamps) - 1) // 2
        ] - 1
        interrupted_bookmark = datetime.fromtimestamp(
            interrupted_timestamp
        ).strftime(self.DATETIME_FMT)
        interrupted_state = {
            'bookmarks': {
                stream_ids[completed_stream]: dict(
                    full_sync_state['bookmarks'][stream_ids[completed_stream]]
                ),
                stream_ids[interrupted_stream]: {'since': interrupted_bookmark}
            }
        }
        yet_to_be_synced_streams = {
            stream for stream in populated_incremental_streams
            if stream_ids[stream] not in interrupted_state['bookmarks']
        }
        self.assertTrue(yet_to_be_synced_streams)
        menagerie.set_state(conn_id, interrupted_state)

        self.run_and_verify_sync(conn_id)
        resumed_sync_order = runner.get_stream_sync_order_from_target()
        resumed_output = runner.get_records_from_target_output()
        resumed_state = menagerie.get_state(conn_id)
        self.assertIsInstance(resumed_state, dict)
        self.assertIsInstance(resumed_state.get('bookmarks'), dict)
        self.assertTrue(resumed_state['bookmarks'])
        self.assertIsNone(resumed_state.get('currently_syncing'))
        self.assertDictEqual(
            resumed_state, full_sync_state,
            msg="Resumed sync must reach the uninterrupted sync's final state."
        )
        self.assertCountEqual(resumed_sync_order, list(stream_ids))
        self.assertEqual(
            resumed_sync_order, full_sync_order,
            msg="Klaviyo must preserve catalog sync order when resuming."
        )
        self.assertTrue(yet_to_be_synced_streams.issubset(resumed_sync_order))

        for stream, tap_stream_id in stream_ids.items():
            with self.subTest(stream=stream):
                resumed_records = [
                    message['data'] for message in
                    resumed_output.get(stream, {}).get('messages', [])
                    if message.get('action') == 'upsert'
                ]
                self.assertGreater(
                    len(resumed_records), 0,
                    msg="Every selected stream must replicate records on resume."
                )
                expected_records = full_records[stream]
                if self.is_incremental(stream):
                    self.assertIn(tap_stream_id, resumed_state['bookmarks'])
                    self.assertIsInstance(
                        resumed_state['bookmarks'][tap_stream_id]['since'], str
                    )
                    bookmark = interrupted_state['bookmarks'].get(tap_stream_id)
                    starting_timestamp = self.dt_to_ts(
                        bookmark['since'] if bookmark else
                        self.get_properties()['start_date']
                    )
                    expected_records = [
                        record for record in expected_records
                        if record['timestamp'] >= starting_timestamp
                    ]
                    for record in resumed_records:
                        self.assertGreaterEqual(
                            record['timestamp'], starting_timestamp,
                            msg="Resumed sync must respect the saved bookmark."
                        )
                    self.assertTrue(expected_records)
                    self.assertEqual(
                        min(record['timestamp'] for record in resumed_records),
                        min(record['timestamp'] for record in expected_records),
                        msg="Resume must start at the earliest expected event."
                    )
                    if stream in yet_to_be_synced_streams:
                        self.assertEqual(
                            min(record['timestamp'] for record in resumed_records),
                            min(record['timestamp'] for record in full_records[stream]),
                            msg="Unstarted streams must replay from the original start date."
                        )
                    self.assertCountEqual(resumed_records, expected_records)
                else:
                    self.assertNotIn(tap_stream_id, full_sync_state['bookmarks'])
                    self.assertNotIn(tap_stream_id, resumed_state['bookmarks'])
                    self.assertEqual(len(resumed_records), len(expected_records))
                    self.assertEqual(
                        resumed_records, expected_records,
                        msg="Full-table streams must replay the same records in order."
                    )

                primary_keys = self.expected_primary_keys()[stream]
                self.assertEqual(
                    Counter(tuple(record[key] for key in primary_keys)
                            for record in resumed_records),
                    Counter(tuple(record[key] for key in primary_keys)
                            for record in expected_records),
                    msg="Resumed sync must replicate every expected record."
                )
                if stream == interrupted_stream:
                    self.assertGreater(len(resumed_records), 0)
                    self.assertLess(len(resumed_records), len(full_records[stream]))
