from tap_tester import connections, menagerie, runner
import os
import unittest
from datetime import datetime as dt
import time

from tap_tester import connections, menagerie, runner

class IterableBase(unittest.TestCase):
    """
    Base class for tap.
    """

    START_DATE = ""
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    REPLICATION_KEYS = "valid-replication-keys"
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"
    OBEYS_START_DATE = "obey-start-date"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    REPLICATION_DATE_FOMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
    BOOKMARK_FOMAT = "%Y-%m-%dT%H:%M:%S.%f"

    first_start_date = '2015-03-25T00:00:00Z'
    second_start_date = '2020-08-15T00:00:00Z'

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
            'start_date':'2015-03-25T00:00:00Z', # '2018-03-25T00:00:00Z' for faster test runs
            'api_key':os.getenv('ITERABLE_API_KEY'),
            "api_window_in_days": 10
        }
        if original:
            return return_value

        # Reassign start date
        return_value["start_date"] = self.START_DATE
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
                self.PRIMARY_KEYS: set(),
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
                self.PRIMARY_KEYS: set(),
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"createdAt"},
                self.OBEYS_START_DATE: True
            }
        }

    def expected_streams(self):
        """
        Returns expected streams for tap.
        """
        return set(self.expected_metadata().keys())

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
        print(found_catalog_names)
        self.assertSetEqual(set(self.expected_metadata().keys()), found_catalog_names,
                            msg="discovered schemas do not match")
        print("discovered schemas are OK")

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
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def dt_to_ts(self, dtime, format):
        """
        Converts datetime to timestamp format.
        """
        date_stripped = int(time.mktime(dt.strptime(dtime, format).timetuple()))
        return date_stripped

    def is_incremental(self, stream):
        """
        Verifies if the stream provided is incremental or not and returns the corresponding
        boolean result.
        """
        return self.expected_metadata()[stream][self.REPLICATION_METHOD] == self.INCREMENTAL

    def get_bookmark_value(self, state, stream):
        bookmark = state.get('bookmarks', {})
        stream_bookmark = bookmark.get(stream)
        stream_replication_key = self.expected_metadata().get(stream,set()).get('REPLICATION_KEYS')
        if stream_bookmark:
            return stream_bookmark.get(stream_replication_key)
        return None