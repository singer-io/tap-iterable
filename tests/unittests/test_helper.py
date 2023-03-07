import unittest
from unittest.mock import patch

from tap_iterable.iterable import Iterable


mock_response_data = {'templates': [{'templateId': 8381104, 'createdAt': 1677051074457, 'updatedAt': 1677051075145, 'name': 'stitch_template_6', 'creatorUserId': 'lee@stitchdata.com', 'messageTypeId': 17355, 'clientTemplateId': 'template_id_6'}]}

class MockResponse:

    status_code = 200
    def __init__(self, json_data):
        self.json_data = json_data

    def json(self):
        return self.json_data

    def raise_for_status(self):
        return "Mock raise for status"

class TestIterable(unittest.TestCase):

    @patch('tap_iterable.iterable.requests.get', return_value = MockResponse(mock_response_data))
    def test_templates(self, mocked_gen_request):
        """
        #
        """
        # Arrange
        it_client = Iterable("mock_api_key")
        # Act
        print("\nwithin test file\n")

        it_client.templates("mock_column_name", "2023-02-22T07:31:15.000000Z")
