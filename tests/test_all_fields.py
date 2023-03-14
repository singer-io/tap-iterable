from tap_tester import menagerie, connections, runner, LOGGER

from base import IterableBase


class AllFieldsTest(IterableBase):

	# Skipping fields since we were not able to genrate test data for following fields in the streams
    MISSING_FILEDS = {
        "templates": {
            "campaignId"
        },
        "email_subscribe": {
            "itblInternal",
            "workflowId"
        },
        "campaigns": {
            "recurringCampaignId",
            "workflowId"
        },
        "email_send": {
            "itblInternal"
        },
        "email_open": {
            "itblInternal",
			"eventName"
        },
		"email_subscribe": {
			"workflowId",
            "itblInternal",
            "eventName",
			"campaignId",
			"templateId",
			"emailListId"
		},
        "email_unsubscribe": {
            "emailListIds",
            "workflowId",
            "emailListId",
            "itblInternal",
            "channelId",
			"eventName"
        },
        "email_bounce": {
            "itblInternal",
			"eventName"
        },
        "users": {
        	"browserTokens",
			"devices.applicationName",
			"devices.endpointEnabled",
			"devices.platform",
			"devices.platformEndpoint",
			"devices.token",
			"devices",
			"featuredDeal",
			"installed_sync",
			"installedDropbox",
			"invoice.customAmount",
			"invoice.customerEmail",
			"invoice.customerName",
			"invoice.customSubTotal",
			"invoice.invoiceDate",
			"invoice.invoiceNumber",
			"invoice.merchantName",
			"invoice.totalDue",
			"invoice",
			"last_session_date",
			"level",
			"merchantId",
			"newListedVehicles.price",
			"onboardingCohort",
			"phoneNumberDetails.carrier",
			"phoneNumberDetails.countryCodeISO",
			"phoneNumberDetails.lineType",
			"phoneNumberDetails.updatedAt",
			"phoneNumberDetails",
			"scheduled_ride",
			"selected_games",
			"shopppingCartTotal",
			"vegetarian_menu.featured_item_availability",
			"vegetarian_menu.featured_item_description",
			"vegetarian_menu.featured_item_id",
			"vegetarian_menu.featured_item_image_url",
			"vegetarian_menu.featured_item_menu_availability",
			"vegetarian_menu.featured_item_menu_date",
			"vegetarian_menu"
		},
        
    }

    def name(self):
        return "tap_tester_iterable_all_fields_test"

    def test_name(self):
        LOGGER.info("All Fields Test for tap-iterable")

    def test_run(self):
        """
            • Verify no unexpected streams were replicated
            • Verify that more than just the automatic fields are replicated for each stream. 
            • verify all fields for each stream are replicated
        """
        # We need to set older start date to increase stream field coverage
        # While doing so test performace may impact so setting large window size
        self.START_DATE = '2018-09-01T00:00:00Z'
        self.API_WINDOWS_IN_DAYS = 365

        # instantiate connection
        conn_id = connections.ensure_connection(self, original_properties=False)

        streams_to_test = self.expected_streams()

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in streams_to_test]
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields, select_all_fields=True)

        # Run sync mode
        sync_record_count = self.run_and_verify_sync(conn_id)
        sync_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        self.assertSetEqual(streams_to_test, set(sync_records.keys())   )

        # get all fields metadata after performing table and field selection
        catalog_all_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            catalog_all_fields[stream_name] = set(fields_from_field_level_md)

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # verify that we get some records for each stream
                self.assertGreater(sync_record_count.get(stream), 0)
                
                # expected automatic fields
                expected_automatic_fields = self.expected_automatic_fields().get(stream)

                # expected all fields
                expected_all_fields = catalog_all_fields[stream]

                # collect actual fields
                messages = sync_records.get(stream)
                actual_all_fields = set()  # aggregate across all records
                all_record_fields = [set(message['data'].keys())
                                     for message in messages['messages']
                                     if message['action'] == 'upsert']
                for fields in all_record_fields:
                    actual_all_fields.update(fields)

                # Verify that more than just the automatic fields are replicated for each stream
                self.assertTrue(expected_automatic_fields.issubset(actual_all_fields),
                                msg=f'{expected_automatic_fields-actual_all_fields} is not in "expected_all_keys"')

                # verify all fields for each stream were replicated
                self.assertSetEqual(expected_all_fields - self.MISSING_FILEDS.get(stream, set()), actual_all_fields)
