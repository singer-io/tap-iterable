from tap_tester import menagerie, connections, runner, LOGGER

from base import IterableBase


class AllFieldsTest(IterableBase):

	# Skipping fields since we were not able to genrate test data for following fields in the streams
    MISSING_FILEDS = {
        "email_subscribe": {
            "itblInternal",
            "workflowId"
        },
        "campaigns": {
            "recurringCampaignId",
            "workflowId"
        },
        "email_send": {
            "itblInternal",
            "transactionalData"
        },
        "email_open": {
            "itblInternal",
            "eventName",
            "proxySource"
        },
		"email_subscribe": {
            "workflowId",
            "itblInternal",
            "eventName",
            "campaignId",
            "templateId",
            "emailListId",
            "userId",
            "profileUpdatedAt"
		},
        "email_unsubscribe": {
            "workflowId",
            "emailListId",
            "itblInternal",
            "channelId",
			"eventName",
            "bounceMessage",
            "status",
            "recipientState"
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
			"vegetarian_menu",
            "jobRecommendations.description",
            "times_purchased",
            "estimatedSizing",
            "auctionDigest",
            "house Districts",
            "passively_seeking",
            "accessIp",
            "offers.description",
            "interested_in_toilet_paper",
            "job_categories_interested",
            "region",
            "recommendedVehicles.estimateDescription",
            "wishList",
            "jobRecommendations.name",
            "paid_user",
            "offers.id",
            "favoriteCategories.category",
            "offers.quantity",
            "shoppingCartItems.categories",
            "offers.intro APR",
            "total_playtime",
            "loyalty_program",
            "totalSpent",
            "lifetime Dontation",
            "hasMobileApp",
            "type",
            "favoriteAnimal",
            "offers.name",
            "recommendedVehicles.imageUrl",
            "shoppingCartItems.sku",
            "recommendedVehicles.category",
            "experience",
            "wishList.name",
            "highestBidPrice",
            "newListedVehicles.noHagglePrice",
            "actively_seeking",
            "counties",
            "daysSinceLastOrder",
            "current_employer_id",
            "shoppingCartItems.quantity",
            "jobRecommendations.applicationURL",
            "times donated",
            "last_game_played",
            "city",
            "jobRecommendations.id",
            "promoCode",
            "shoppingCartItems.imageUrl",
            "swingTrader_subscription",
            "badgeCount",
            "Industry",
            "recommendedVehicles",
            "totalOrders",
            "readingList.avgRating",
            "shoppingCartItems.name",
            "auctionDigest.name",
            "newListedVehicles.name",
            "offers.categories",
            "shoppingCartItems.url",
            "shoppingCartItems.price",
            "should_receive_recommendation",
            "offers.imageUrl",
            "newListedVehicles.imageUrl",
            "last_purchased",
            "state",
            "shoppingCartItems.id",
            "favoriteRestaurant",
            "interested_in_soap",
            "sat",
            "newListedVehicles.sku",
            "offers",
            "auctionDigest.auctionHouse",
            "senate Districts",
            "congressional Districts",
            "booked_activity_before",
            "readingList.bookName",
            "shoppingCartItems",
            "totalPurchases",
            "favorite_category",
            "bestFriend",
            "age",
            "timeZone",
            "university_interest",
            "lastOrderrestaurant",
            "jobRecommendations.imageUrl",
            "date_last_booked_package",
            "shoppingCartItems.description",
            "lastKnownLatitude",
            "favoriteShowCategories",
            "is_available",
            "auctionDigest.auctionDateLocation",
            "wishList.price",
            "acquisition_source",
            "location",
            "fb_follow",
            "user_type",
            "zip",
            "tagline",
            "recommendedVehicles.TrueCar Estimate",
            "newListedVehicles.category",
            "major",
            "auctionDigest.auctionImageUrl",
            "lastAccessedAgent",
            "favoriteCuisine",
            "readingList.bookAuthor",
            "is_active",
            "offers.Intro APR",
            "loyalty_points",
            "jobRecommendations",
            "recommendedVehicles.name",
            "job_title",
            "readingList",
            "lastOrderlocation",
            "recommendedVehicles.sku",
            "averageOrderValue",
            "phoneNumber",
            "marketSmith_subscription",
            "interested_in_detergent",
            "favoriteCategory",
            "vip",
            "favoriteItem",
            "streetAddress",
            "lastKnownLongitude",
            "newListedVehicles.miles",
            "CCProvider",
            "totalOrderCount",
            "uploaded_resume",
            "lifetime_Spent",
            "last_purchased_category",
            "loyalty_member",
            "offers.sku",
            "readingList.imageUrl",
            "current_employer",
            "offers.url",
            "favoriteProduct",
            "industry",
            "gender",
            "favoriteCategories",
            "favoritedShows",
            "booked_package_before",
            "locale",
            "auctionDigest.auctionInfo",
            "newListedVehicles",
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
        self.START_DATE = '2020-09-01T00:00:00Z'
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
