import dateutil
from tap_tester import connections, menagerie, runner
import os
import unittest
from datetime import datetime as dt, timedelta
import time

from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER



class IterableBase(unittest.TestCase):
    """
    Base class for tap.
    """

    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    REPLICATION_KEYS = "valid-replication-keys"
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"
    OBEYS_START_DATE = "obey-start-date"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    REPLICATION_DATE_FOMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
    BOOKMARK_FOMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    START_DATE = ""
    API_WINDOWS_IN_DAYS = 60

    # Skipping streams from testing because we were unable to generate test data
    MISSING_DATA_STREAMS = {"metadata", "email_send_skip", "email_complaint", "email_click"}

    def tap_name(self):
        return "tap-iterable"

    def setUp(self):
        required_env = {
            "ITERABLE_API_KEY"
        }
        missing_envs = [v for v in required_env if not os.getenv(v)]
        if missing_envs:
            raise Exception("set " + ", ".join(missing_envs))

    def get_type(self):
        return "platform.iterable"

    def get_credentials(self):
        """
        Setting required credentials as environment variables.
        """
        return {
            "api_key": os.getenv("ITERABLE_API_KEY")
        }

    def get_properties(self, original: bool = True):
        """
        Setting required properties as environment variables.
        """
        return_value = {
            'start_date':'2023-01-25T00:00:00Z',
            'api_key':os.getenv('ITERABLE_API_KEY'),
            "api_window_in_days": 30
        }
        if original:
            return return_value

        # Reassign start date
        return_value["start_date"] = self.START_DATE
        return_value["api_window_in_days"] = self.API_WINDOWS_IN_DAYS
        return return_value

    def expected_metadata(self):
        """
        Provides the expected metadata for each stream.
        """
        return {
            "campaigns": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updatedAt"},
                self.OBEYS_START_DATE: True
            },
            "channels": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: True
            },
            "email_complaint": {
                self.PRIMARY_KEYS: set(),
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"createdAt"},
                self.OBEYS_START_DATE: True
            },
            "email_bounce": {
                self.PRIMARY_KEYS: set(),
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"createdAt"},
                self.OBEYS_START_DATE: True
            },
            "email_click": {
                self.PRIMARY_KEYS: set(),
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"createdAt"},
                self.OBEYS_START_DATE: True
            },
            "email_open": {
                self.PRIMARY_KEYS: set(),
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"createdAt"},
                self.OBEYS_START_DATE: True
            },
            "email_send": {
                self.PRIMARY_KEYS: set(),
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"createdAt"},
                self.OBEYS_START_DATE: True
            },
            "email_send_skip": {
                self.PRIMARY_KEYS: set(),
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"createdAt"},
                self.OBEYS_START_DATE: True
            },
            "email_subscribe": {
                self.PRIMARY_KEYS: set(),
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"createdAt"},
                self.OBEYS_START_DATE: True
            },
            "email_unsubscribe": {
                self.PRIMARY_KEYS: set(),
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"createdAt"},
                self.OBEYS_START_DATE: True
            },
            "lists": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: True
            },
            "list_users": {
                self.PRIMARY_KEYS: {"email", "listId"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: True
            },
            "message_types": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: True
            },
            "metadata": {
                self.PRIMARY_KEYS: {"key"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: True
            },
            "templates": {
                self.PRIMARY_KEYS: {"templateId"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updatedAt"},
                self.OBEYS_START_DATE: True
            },
            "users": {
                self.PRIMARY_KEYS: {"email"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"profileUpdatedAt"},
                self.OBEYS_START_DATE: True
            }
        }

    def expected_streams(self, include_missing_data_streams=False):
        """
        Returns expected streams for tap.
        """
        if include_missing_data_streams:
            return set(self.expected_metadata().keys())
            
        return set(self.expected_metadata().keys()) - self.MISSING_DATA_STREAMS

    def expected_automatic_fields(self):
        """Retrieving primary keys and replication keys as an automatic
        fields."""        
        return {table: (properties.get(self.PRIMARY_KEYS, set()) | properties.get(self.REPLICATION_KEYS, set())) 
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """
        Returns expected replication keys for streams in tap.
        """
        return {table: properties.get(self.REPLICATION_KEYS, set()) for table, properties
                in self.expected_metadata().items()}

    def expected_primary_keys(self):
        """
        Returns expected primary keys for streams in tap.
        """
        return {table: properties.get(self.PRIMARY_KEYS, set()) for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        """
        Returns expected replication method for streams in tap.
        """
        return {table: properties.get(self.REPLICATION_METHOD, set()) for table, properties
                in self.expected_metadata().items()}
    
    def expected_automatic_fields(self):
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set())
        return auto_fields

    def select_found_catalogs(self, conn_id, catalogs, only_streams=None,
                              deselect_all_fields: bool = False, non_selected_props=[]):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            if only_streams and catalog["stream_name"] not in only_streams:
                continue
            schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            non_selected_properties = non_selected_props if not deselect_all_fields else []
            if deselect_all_fields:
                # Get a list of all properties so that none are selected
                non_selected_properties = schema.get("annotated-schema", {}).get("properties", {})
                non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(conn_id,
                                                               catalog,
                                                               schema,
                                                               additional_md=additional_md,
                                                               non_selected_fields=non_selected_properties)
    
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
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    self.assertTrue(field_selected, msg="Field not selected.")


    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs, select_all_fields=True):
        """Perform table and field selection based off of the streams to select
        set and field selection parameters.

        Verify this results in the expected streams selected and all or
        no fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields)

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get("stream_name") for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat["stream_id"])

            # Verify all testable streams are selected
            selected = catalog_entry.get("annotated-schema").get("selected")
            LOGGER.info("Validating selection on {}: {}".format(cat["stream_name"], selected))
            if cat["stream_name"] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue  # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get("annotated-schema").get("properties").items():
                    field_selected = field_props.get("selected")
                    LOGGER.info("\tValidating selection on {}.{}: {}".format(cat["stream_name"], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat["stream_name"])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry["metadata"])
                self.assertEqual(expected_automatic_fields, selected_fields)
    
    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (field['metadata'].get('inclusion') == 'automatic'
                                               or field['metadata'].get('selected') is True)
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields
    
    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields=True):
        """Select all streams and all fields within streams."""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            non_selected_properties = []
            if not select_all_fields:
                # Get a list of all properties so that none are selected
                non_selected_properties = schema.get("annotated-schema", {}).get("properties", {}).keys()

            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema, [], non_selected_properties)

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """
        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0,
                           msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))
        LOGGER.info(found_catalog_names)
        self.assertSetEqual(set(self.expected_metadata().keys()), found_catalog_names,
                            msg="discovered schemas do not match")
        LOGGER.info("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id, streams=None):
        """
        Runs sync mode and verifies the exit status for the same.
        """
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        sync_record_count = runner.examine_target_output_file(self,
                                                              conn_id,
                                                              streams if streams else self.expected_streams(),
                                                              self.expected_primary_keys())

        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        LOGGER.info("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def dt_to_ts(self, dtime, format):
        """
        Converts datetime to timestamp format.
        """
        date_stripped = int(time.mktime(dt.strptime(dtime, format).timetuple()))
        return date_stripped
    
    @staticmethod
    def parse_date(date_value):
        """
        Pass in string-formatted-datetime, parse the value, and return it as an unformatted datetime object.
        """
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S.%fZ",
            "%Y-%m-%d %H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S.%f +00:00",
            "%Y-%m-%d %H:%M:%S +00:00",
            "%Y-%m-%d %H:%M:%S.%f UTC",
            "%Y-%m-%d %H:%M:%S UTC",
            "%Y-%m-%d"
        }
        for date_format in date_formats:
            try:
                date_stripped = dt.strptime(date_value, date_format)
                return date_stripped
            except ValueError:
                continue

        raise NotImplementedError("Tests do not account for dates of this format: {}".format(date_value))

    def is_incremental(self, stream):
        """
        Verifies if the stream provided is incremental or not and returns the corresponding
        boolean result.
        """
        return self.expected_metadata()[stream][self.REPLICATION_METHOD] == self.INCREMENTAL

    def calculated_states_by_stream(self, current_state):
        timedelta_by_stream = {stream: [0,0,5]  # {stream_name: [days, hours, minutes], ...}
                               for stream in self.expected_streams()}

        stream_to_calculated_state = {"bookmarks": {stream: {list(self.expected_replication_keys(
        )[stream])[0]: None} for stream in current_state['bookmarks'].keys()}}

        for stream, state in current_state['bookmarks'].items():
            replication_key = list(self.expected_replication_keys()[stream])[0]
            state_as_datetime = dateutil.parser.parse(state[replication_key])

            days, hours, minutes = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes)

            state_format = "%Y-%m-%dT00:00:00Z"
            calculated_state_formatted = dt.strftime(calculated_state_as_datetime, state_format)

            stream_to_calculated_state["bookmarks"][stream][replication_key] = calculated_state_formatted

        return stream_to_calculated_state

    def assertIsDateFormat(self, value, str_format):
        """
        Assertion Method that verifies a string value is a formatted datetime with
        the specified format.
        """
        try:
            _ = dt.strptime(value, str_format)
        except ValueError as err:
            raise AssertionError(
                f"Value does not conform to expected format: {str_format}"
            ) from err
