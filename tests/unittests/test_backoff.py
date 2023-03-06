from unittest import mock
from tap_iterable.exceptions import IterableError, raise_for_error, IterableBadRequestError, IterableUnauthorizedError, IterableRateLimitError, IterableNotAvailableError
from tap_iterable.iterable import Iterable
import unittest
import requests
from parameterized import parameterized





class Mockresponse:
    """ Mock response object class."""

    def __init__(self, status_code, json, raise_error, headers={'X-RateLimit-Remaining': 1}, content=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.headers = headers
        self.content = content if content is not None else 'iterable'

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        """ Response JSON method."""
        return self.text

def get_response(status_code, json={}, raise_error=False, content=None):
    """ Returns required mock response. """
    return Mockresponse(status_code, json, raise_error, content=content)


class TestBackoff(unittest.TestCase):
    
    """
    Test Error handling backoff for Iterable.
    """

    @parameterized.expand([
        [400, IterableBadRequestError, 1],
        [401, IterableUnauthorizedError, 1],
        [429, IterableRateLimitError, 5],
        [503, IterableNotAvailableError, 5],
    ])
    @mock.patch("time.sleep")
    @mock.patch("requests.get")
    def test_backoff(self, mock_error_code, mock_exception, mock_expected_call_count, mock_sleep, mock_get):
        mock_get.side_effect = get_response(mock_error_code)
        iterable_object = Iterable("api-key")

        # with self.assertRaises(mock_exception) as e:
        iterable_object.get("dummy-path")

        # # Verifying the message formed for the custom exception
        # self.assertEqual(str(e.exception), expected_error_message)

        # Verify the call count for each error.
        self.assertEquals(mock_get.call_count, mock_expected_call_count)
