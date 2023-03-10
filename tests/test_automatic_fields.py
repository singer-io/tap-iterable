import json

from tap_tester import connections, runner, LOGGER

from base import IterableBase


class AutomaticFieldsTest(IterableBase):

    def name(self):
        return "tap_tester_iterable_automatic_test"

    def test_name(self):
        LOGGER.info("Automatic Field Test for tap-iterable")

    def test_run(self):
        """
        Verify we can deselect all fields except when inclusion=automatic, which is handled by base.py methods
        Verify that only the automatic fields are sent to the target.
        Verify that all replicated records have unique primary key values.
        """
        # instantiate connection
        conn_id = connections.ensure_connection(self)

        streams_to_test = self.expected_streams()

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(
             conn_id, test_catalogs_automatic_fields, select_all_fields=False)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        all_messages = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_keys = self.expected_automatic_fields().get(stream)

                expected_keys = expected_keys 

                # collect actual values
                stream_messages = all_messages.get(stream)
                record_messages_keys = [set(message['data'].keys())
                                        for message in stream_messages['messages']
                                        if  message['action'] == 'upsert']

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit")

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                # Get records
                records = [message.get("data") for message in stream_messages.get('messages', [])
                            if message.get('action') == 'upsert']

                # Verify there are no duplicate records
                if expected_primary_keys:
                    records_pks_list = [tuple(message.get(pk) for pk in expected_primary_keys)
                                        for message in [json.loads(t) for t in {json.dumps(d) for d in records}]]

                    # Remove duplicate primary keys
                    duplicate_records = [x for n, x in enumerate(records_pks_list) if x in records_pks_list[:n]]
                    self.assertFalse(len(duplicate_records),
                                      msg=f"Following are duplicate records: {duplicate_records}")
