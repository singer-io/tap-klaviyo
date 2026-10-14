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

        Requires three populated incremental streams, including one with
        multiple event timestamps so that resumption excludes earlier records.
        """
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        self.perform_and_verify_table_and_field_selection(conn_id, found_catalogs)

        self.run_and_verify_sync(conn_id)
        full_sync_output = runner.get_records_from_target_output()
        full_sync_state = menagerie.get_state(conn_id)
        stream_ids = {
            catalog['stream_name']: catalog['tap_stream_id']
            for catalog in found_catalogs
        }
        full_records = {
            stream: [message['data'] for message in
                     full_sync_output.get(stream, {}).get('messages', [])
                     if message.get('action') == 'upsert']
            for stream in stream_ids
        }
        populated_incremental_streams = [
            stream for stream in stream_ids
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
        menagerie.set_state(conn_id, interrupted_state)

        self.run_and_verify_sync(conn_id)
        resumed_output = runner.get_records_from_target_output()
        resumed_state = menagerie.get_state(conn_id)
        self.assertDictEqual(
            resumed_state, full_sync_state,
            msg="Resumed sync must reach the uninterrupted sync's final state."
        )

        for stream, tap_stream_id in stream_ids.items():
            with self.subTest(stream=stream):
                resumed_records = [
                    message['data'] for message in
                    resumed_output.get(stream, {}).get('messages', [])
                    if message.get('action') == 'upsert'
                ]
                expected_records = full_records[stream]
                if self.is_incremental(stream):
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
                else:
                    self.assertNotIn(tap_stream_id, resumed_state['bookmarks'])

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
