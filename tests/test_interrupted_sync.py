from tap_tester import runner, connections, menagerie, LOGGER
from base import IterableBase
from dateutil import parser as parser


class InterruptedSyncTest(IterableBase):
    """
    Test to verify that if a sync is interrupted, then the next sync will continue
    from the bookmarks and currently syncing stream.
    """

    def name(self):
        return "tap_tester_iterable_interrupted_sync_test"

    def test_name(self):
        LOGGER.info("Interrupted Sync test for tap-iterable")

    def test_run(self):
        """
        Scenario: A sync job is interrupted. The state is saved with `currently_syncing`.
                  The next sync job kicks off and the tap picks back up on that
                  `currently_syncing` stream.

        Expected State Structure:
            {
                "currently_syncing": "stream_name",
                "bookmarks": {
                    "stream_1": "2010-10-10T10:10:10.100000",
                    "stream_2": "2010-10-10T10:10:10.100000"
                }
            }

        Test Cases:
        - Verify an interrupted sync can resume based on the `currently_syncing` and stream level bookmark value.
        - Verify only records with replication-key values greater than or equal to the
          stream level bookmark are replicated on the resuming sync for the interrupted stream.
        - Verify the yet-to-be-synced streams are replicated following the interrupted stream in the resuming sync.
        """

        self.start_date = self.get_properties()["start_date"]
        start_date_timestamp = self.parse_date(self.start_date)

        conn_id = connections.ensure_connection(self)

        # Note: test data not available for following streams: metadata, email_send_skip, email_complaint, email_click
        streams_to_test = self.expected_streams()

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in streams_to_test]

        # Catalog selection
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields, select_all_fields=True)

        ##########################################################################
        # First Sync
        ##########################################################################

        # Run a sync job
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()

        for stream in streams_to_test:
            self.assertGreater(
                first_sync_record_count.get(stream, -1), 0,
                msg="First sync should sync at least 1 record for testing")

        ##########################################################################
        # Set state for 2nd sync
        ##########################################################################

        # Simulated interrupted state to run 2nd sync
        interrupted_sync_state = {
            "currently_syncing": "email_open",
            "bookmarks": {
                "campaigns": {
                    "updatedAt": "2023-03-02 10:18:55.000000Z"
                },
                "templates": {
                    "updatedAt": "2023-02-22 07:31:15.000000Z"
                },
                "email_bounce": {
                    "createdAt": "2023-03-01 10:58:00.000000Z"
                },
                "email_open": {
                    "createdAt": "2023-02-22 7:43:31.000000Z"
                }
            }
        }
        menagerie.set_state(conn_id, interrupted_sync_state)

        ##########################################################################
        # Second Sync
        ##########################################################################

        # Run sync after interruption
        self.run_and_verify_sync(conn_id)
        post_interrupted_sync_records = runner.get_records_from_target_output()

        post_interrupted_sync_state = menagerie.get_state(conn_id)
        currently_syncing = post_interrupted_sync_state.get("bookmarks").get("currently_syncing")

        # Checking that the resuming sync resulted in a successfully saved state
        with self.subTest():

            # Verify sync is not interrupted by checking currently_syncing in the state for sync
            self.assertIsNone(currently_syncing,
                              msg="After final sync bookmarks should not contain 'currently_syncing' key.")

            # Verify bookmarks are saved
            self.assertIsNotNone(post_interrupted_sync_state.get("bookmarks"),
                                 msg="After final sync bookmarks should not be empty.")

            # Verify final_state is equal to uninterrupted sync"s state
            self.assertDictEqual(post_interrupted_sync_state, post_interrupted_sync_state,
                                 msg="Final state after interruption should be equal to full sync")

        # Stream level assertions
        for stream in streams_to_test:
            with self.subTest(stream=stream):
                # Get the replication key
                replication_keys = list(self.expected_replication_keys()[stream])
                replication_key = replication_keys[0] if replication_keys else None

                # Get the replication method
                replication_method = self.expected_replication_method()[stream]

                # Gather actual results
                first_sync_stream_records = [message["data"]
                                             for message
                                             in first_sync_records.get(stream, {}).get("messages", [])]

                post_interrupted_sync_stream_records = [message["data"]
                                                        for message
                                                        in post_interrupted_sync_records.get(stream, {}).get("messages", [])]

                # Get record counts
                full_sync_record_count = len(first_sync_stream_records)
                interrupted_record_count = len(post_interrupted_sync_stream_records)

                if replication_method == self.INCREMENTAL:
                    # Final bookmark after interrupted sync
                    final_stream_bookmark = post_interrupted_sync_state["bookmarks"].get(
                        stream, dict()).get(replication_key, None)

                    # Verify final bookmark matched the formatting standards for the resuming sync
                    self.assertIsNotNone(final_stream_bookmark,
                                         msg="Bookmark can not be 'None'.")
                    self.assertIsInstance(final_stream_bookmark, str,
                                          msg="Bookmark format is not as expected.")

                if stream == interrupted_sync_state["currently_syncing"]:
                    # Assign the start date to the interrupted stream
                    interrupted_stream_datetime = self.parse_date(
                        interrupted_sync_state["bookmarks"][stream][replication_key])

                    primary_key = self.expected_primary_keys()[stream].pop() if self.expected_primary_keys()[stream] else None

                    # Get primary keys of 1st sync records
                    if primary_key:
                        full_records_primary_keys = [x.get(primary_key)
                                                     for x in first_sync_stream_records]

                    for record in post_interrupted_sync_stream_records:
                        record_time = self.parse_date(record.get(replication_key))

                        # Verify resuming sync only replicates records with the replication key
                        # values greater or equal to the state for streams that were replicated
                        # during the interrupted sync.
                        self.assertGreaterEqual(record_time, interrupted_stream_datetime)

                        # Verify the interrupted sync replicates the expected record set all
                        # interrupted records are in full records
                        if primary_key:
                            self.assertIn(record[primary_key], full_records_primary_keys,
                                          msg="Incremental table record in interrupted sync not found in full sync")

                    # Record count for all streams of interrupted sync match expectations
                    records_after_interrupted_bookmark = 0
                    for record in first_sync_stream_records:
                        record_time = self.parse_date(record.get(replication_key))
                        if record_time >= interrupted_stream_datetime:
                            records_after_interrupted_bookmark += 1

                    self.assertEqual(records_after_interrupted_bookmark, interrupted_record_count,
                                     msg="Expected {} records in each sync".format(
                                         records_after_interrupted_bookmark))

                else:
                    # Get the date to start 2nd sync for non-interrupted streams
                    synced_stream_bookmark = interrupted_sync_state["bookmarks"].get(
                        stream, dict()).get(replication_key, None)

                    if synced_stream_bookmark:
                        synced_stream_datetime = self.parse_date(synced_stream_bookmark)
                    else:
                        synced_stream_datetime = start_date_timestamp

                    self.assertGreater(interrupted_record_count, 0,
                                       msg="Un-interrupted streams must sync at least 1 record.")

                    if replication_method == self.INCREMENTAL:

                        for record in post_interrupted_sync_stream_records:
                            record_time = self.parse_date(record.get(replication_key))

                            # Verify resuming sync only replicates records with the replication key
                            # values greater or equal to the state for streams that were replicated
                            # during the interrupted sync.
                            self.assertGreaterEqual(record_time, synced_stream_datetime)

                            # Verify resuming sync replicates all records that were found in the full
                            # sync (non-interrupted)
                            self.assertIn(record, first_sync_stream_records,
                                          msg="Unexpected record replicated in resuming sync.")
                    else:
                        # FULL_TABLE stream records should be same
                        self.assertEqual(interrupted_record_count, full_sync_record_count,
                                         msg=f"Record count of streams with {self.FULL_TABLE} replication method must be equal.")
