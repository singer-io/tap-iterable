import unittest
from unittest.mock import patch

from tap_iterable.iterable import Iterable

mock_response_templates_data = {'templates': [
    {'templateId': 8381104, 'createdAt': 1677051074457, 'updatedAt': 1677051075145, 'name': 'stitch_template_6',
     'creatorUserId': 'lee@stitchdata.com', 'messageTypeId': 17355, 'clientTemplateId': 'template_id_6'}]}

mock_response_campaigns_data = {'campaigns': [
    {'id': 6302484, 'createdAt': 1677752293554, 'updatedAt': 1677752335081, 'startAt': 1677752335081,
     'endedAt': 1677752337587, 'name': 'stitch_campaign_7', 'templateId': 8461343, 'messageMedium': 'Email',
     'createdByUserId': 'bsloane@talend.com', 'updatedByUserId': 'bsloane@talend.com', 'campaignState': 'Finished',
     'listIds': [2306684, 2304186], 'suppressionListIds': [2319638, 2304187], 'sendSize': 5, 'labels': [],
     'type': 'Blast'}]}


class MockResponse:
    status_code = 200

    def __init__(self, json_data):
        self.json_data = json_data

    def json(self):
        return self.json_data

    def raise_for_status(self):
        return "Mock raise for status"


class TestIterable(unittest.TestCase):
    client_obj = Iterable("mock_api_key")

    @patch('tap_iterable.iterable.requests.get', return_value=MockResponse(mock_response_templates_data))
    def test_templates(self, mocked_gen_request):
        """
        Test the templates stream flow
        """

        # Arrange
        expected_value = {'templateId': 8381104, 'createdAt': 1677051074457, 'updatedAt': 1677051075145,
                          'name': 'stitch_template_6', 'creatorUserId': 'lee@stitchdata.com', 'messageTypeId': 17355,
                          'clientTemplateId': 'template_id_6'}

        # Act and Assert
        for value in self.client_obj.templates("mock_column_name", "2023-02-22T07:31:15.000000Z"):
            self.assertEqual(expected_value, value)

    @patch('tap_iterable.iterable.requests.get', return_value=MockResponse(mock_response_campaigns_data))
    def test_campaigns(self, mocked_gen_request):
        """
        Test the campaigns stream flow
        """

        # Arrange
        expected_value = {'id': 6302484, 'createdAt': 1677752293554, 'updatedAt': 1677752335081,
                          'startAt': 1677752335081, 'endedAt': 1677752337587, 'name': 'stitch_campaign_7',
                          'templateId': 8461343, 'messageMedium': 'Email', 'createdByUserId': 'bsloane@talend.com',
                          'updatedByUserId': 'bsloane@talend.com', 'campaignState': 'Finished',
                          'listIds': [2306684, 2304186], 'suppressionListIds': [2319638, 2304187], 'sendSize': 5,
                          'labels': [], 'type': 'Blast'}

        # Act and Assert
        for value in self.client_obj.campaigns("mock_column_name", "2023-03-02 10:18:55+00:00"):
            self.assertEqual(expected_value, value)
