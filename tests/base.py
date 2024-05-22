import unittest
import os
from datetime import timedelta
from datetime import datetime as dt
import time
import dateutil.parser
import singer
from tap_tester import connections, menagerie, runner

class KlaviyoBaseTest(unittest.TestCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.
    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).
    """
    AUTOMATIC_FIELDS = "automatic"
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    BOOKMARK = "bookmark"
    REPLICATION_KEYS = "valid-replication-keys"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
    start_date = ""


    def setUp(self):
        required_creds = {
            "api_key": 'TAP_KLAVIYO_API_KEY'
        }
        missing_creds = [v for v in required_creds.values() if not os.getenv(v)]
        if missing_creds:
            raise Exception("set " + ", ".join(missing_creds))
        self._credentials = {k: os.getenv(v) for k, v in required_creds.items()}

    def get_credentials(self):
        self._credentials["api_key"] = os.getenv('TAP_KLAVIYO_API_KEY')
        return self._credentials

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-klaviyo"

    @staticmethod
    def get_type():
        return "platform.klaviyo"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date': '2021-04-26T00:00:00Z',
            # 'user_agent': 'email_address'
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def expected_metadata(self):
        """The expected primary key of the streams"""
        # Added new REPLICATION_KEYS field as all incremental streams save the bookmark in `since` 
        # field which is different from the bookmark key.
        return{
            "receive": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            "click": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            "open": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            "bounce": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            "unsubscribe": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            "mark_as_spam": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            "dropped_email": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            "unsub_list": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            "subscribe_list": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            "subscribed_to_email": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            "subscribed_to_sms": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            "update_email_preferences": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            "global_exclusions": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "lists": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "campaigns": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            'clicked_sms': {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            'unsubscribed_from_sms': {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            'received_sms': {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            'sent_sms': {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            'failed_to_deliver': {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            'failed_to_deliver_automated_response': {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            },
            'received_automated_response': {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS:{"since"},
                self.BOOKMARK: {"timestamp"}
            }
        }

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self):
        """return a dictionary with key of table name and value as a set of primary key fields"""
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_bookmark_keys(self):
        """return a dictionary with key of table name and value as a set of bookmark key fields"""
        return {table: properties.get(self.BOOKMARK, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """return a dictionary with key of table name and value as a set of replication key fields"""
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self):
        """return a dictionary with key of table name and set of value of automatic(primary key and bookmark field) fields"""
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) |  v.get(self.BOOKMARK, set())
        return auto_fields

    def expected_replication_method(self):
        """return a dictionary with key of table name and value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

     #########################
    #   Helper Methods      #
    #########################

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))
        print(found_catalog_names)
        # In klaviyo, on the metrics api endpoint data, we finalize the catalog
        # And, it is not necessary that all schemas defined in /schemas will be part of catalog.
        self.assertTrue(set(found_catalog_names).issubset(set(self.expected_streams())), msg="discovered schemas do not match")
        print("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self,
                                                     conn_id,
                                                     test_catalogs,
                                                     select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.

        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    print("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or \
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields


    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    def calculated_states_by_stream(self, current_state):
        timedelta_by_stream = {stream: [0,0,1]  # {stream_name: [days, hours, minutes], ...}
                               for stream in self.expected_streams()}
        
        stream_to_calculated_state = {stream: "" for stream in current_state['bookmarks'].keys()}
        for stream, state in current_state['bookmarks'].items():
            state_key, state_value = next(iter(state.keys())), next(iter(state.values()))
            state_as_datetime = dateutil.parser.parse(state_value)

            days, hours, minutes = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes)

            state_format = '%Y-%m-%dT%H:%M:%SZ'
            calculated_state_formatted = dt.strftime(calculated_state_as_datetime, state_format)

            stream_to_calculated_state[stream] = {state_key: calculated_state_formatted}

        return stream_to_calculated_state
    
    def timedelta_formatted(self, dtime, days=0):
        date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
        return_date = date_stripped + timedelta(days=days)

        return dt.strftime(return_date, self.START_DATE_FORMAT)

    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################

    def is_incremental(self, stream):
        return self.expected_metadata()[stream][self.REPLICATION_METHOD] == self.INCREMENTAL

    def dt_to_ts(self, dtime):
        return int(time.mktime(dt.strptime(
            dtime, self.DATETIME_FMT).timetuple()))
