from unittest import mock
from tap_iterable.exceptions import IterableBadRequestError, IterableUnauthorizedError, IterableRateLimitError, IterableNotAvailableError
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
        [400, IterableBadRequestError, 1, "A validation exception has occurred."],
        [401, IterableUnauthorizedError, 1, "Invalid authorization credentials."],
        [429, IterableRateLimitError, 5,
            "The API rate limit for your organisation/application pairing has been exceeded."],
        [503, IterableNotAvailableError, 5, "API service is currently unavailable."],
    ])
    @mock.patch("time.sleep")
    @mock.patch("requests.get")
    def test_backoff(self, mock_error_code, mock_exception, mock_expected_call_count, expected_error_message, mock_get, mock_sleep):
        iterable_object = Iterable("api-key")
        mock_get.side_effect = [Mockresponse(
            mock_error_code, {}, True)] * mock_expected_call_count

        with self.assertRaises(mock_exception) as e:
            iterable_object._get("dummy-path")

        # Verifying the message formed for the custom exception
        self.assertEqual(str(
            e.exception), f"HTTP-error-code: {mock_error_code}, Error: {expected_error_message}")

        # Verify the call count for each error.
        self.assertEqual(mock_get.call_count, mock_expected_call_count)
