import unittest
from unittest import mock

import requests
from parameterized import parameterized

from tap_iterable.exceptions import IterableServer5xxError
from tap_iterable.iterable import Iterable


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


class TestGenericServer5xxError(unittest.TestCase):

    """
    Test Error handling for Generic 5xx errors that are not specifically mapped.
    This includes errors like 502, 504, 505, etc. that should be treated as IterableServer5xxError.
    """

    def setUp(self):
        self.iterable_object = Iterable("api-key")

    @parameterized.expand([
        [501, "Not Implemented"],
        [502, "Bad Gateway"],
        [504, "Gateway Timeout"],
        [505, "HTTP Version Not Supported"],
        [506, "Variant Also Negotiates"],
        [507, "Insufficient Storage"],
        [508, "Loop Detected"],
        [510, "Not Extended"],
        [511, "Network Authentication Required"],
    ])
    @mock.patch("time.sleep")
    @mock.patch("requests.get")
    def test_generic_5xx_errors_with_backoff(self, mock_error_code, error_description, mock_get, mock_sleep):
        """
        Test that generic 5xx errors (not in ERROR_CODE_EXCEPTION_MAPPING) raise IterableServer5xxError
        and trigger backoff retry (max_tries=7 for IterableServer5xxError).
        """
        mock_get.side_effect = [Mockresponse(
            mock_error_code, {}, True)] * 7

        with self.assertRaises(IterableServer5xxError) as e:
            self.iterable_object._get("dummy-path")

        # Verifying the message formed for the custom exception
        expected_message = f"HTTP-error-code: {mock_error_code}, Error: Unknown Error"
        self.assertEqual(str(e.exception), expected_message)

        # Verify the call count - should retry 7 times due to backoff
        self.assertEqual(mock_get.call_count, 7)

    @parameterized.expand([
        [502, {"message": "Bad Gateway Error"}],
        [504, {"message": "Gateway Timeout Error"}],
        [505, {"message": "Custom 5xx Error Message"}],
    ])
    @mock.patch("time.sleep")
    @mock.patch("requests.get")
    def test_generic_5xx_errors_with_custom_message(self, mock_error_code, json_response, mock_get, mock_sleep):
        """
        Test that generic 5xx errors include custom error messages from the API response.
        """
        mock_get.side_effect = [Mockresponse(
            mock_error_code, json_response, True)] * 7

        with self.assertRaises(IterableServer5xxError) as e:
            self.iterable_object._get("dummy-path")

        # Verifying the message includes the custom error message from response
        expected_message = f"HTTP-error-code: {mock_error_code}, Error: {json_response['message']}"
        self.assertEqual(str(e.exception), expected_message)

    @mock.patch("time.sleep")
    @mock.patch("requests.get")
    def test_504_error_specifically(self, mock_get, mock_sleep):
        """
        Test specifically for 504 error code as mentioned in the code comments.
        This error should be treated as IterableServer5xxError and retry with backoff.
        """
        mock_get.side_effect = [Mockresponse(504, {}, True)] * 7

        with self.assertRaises(IterableServer5xxError) as e:
            self.iterable_object._get("dummy-path")

        # Verifying the error is raised correctly
        self.assertIn("HTTP-error-code: 504", str(e.exception))

        # Verify retry count
        self.assertEqual(mock_get.call_count, 7)

    @mock.patch("time.sleep")
    @mock.patch("requests.get")
    def test_generic_5xx_not_503(self, mock_get, mock_sleep):
        """
        Test that generic 5xx errors (e.g., 502, 504) are different from 503
        which is specifically mapped to IterableNotAvailableError.
        """
        # Test with 502 error
        mock_get.side_effect = [Mockresponse(502, {}, True)] * 7

        with self.assertRaises(IterableServer5xxError) as e:
            self.iterable_object._get("dummy-path")

        # Ensure it's the base IterableServer5xxError, not IterableNotAvailableError
        self.assertEqual(type(e.exception).__name__, 'IterableServer5xxError')

    @mock.patch("time.sleep")
    @mock.patch("requests.get")
    def test_generic_5xx_error_with_invalid_json_response(self, mock_get, mock_sleep):
        """
        Test that generic 5xx errors handle invalid JSON responses gracefully.
        """
        # Create a mock response that will raise an exception when .json() is called
        mock_response = Mockresponse(502, None, True)
        mock_response.json = mock.Mock(side_effect=ValueError("Invalid JSON"))

        mock_get.side_effect = [mock_response] * 7

        with self.assertRaises(IterableServer5xxError) as e:
            self.iterable_object._get("dummy-path")

        # Should fall back to "Unknown Error" when JSON parsing fails
        expected_message = "HTTP-error-code: 502, Error: Unknown Error"
        self.assertEqual(str(e.exception), expected_message)

        # Verify retry count
        self.assertEqual(mock_get.call_count, 7)

    @mock.patch("time.sleep")
    @mock.patch("requests.get")
    def test_multiple_5xx_errors_eventually_succeed(self, mock_get, mock_sleep):
        """
        Test that after multiple 5xx errors, a successful response is eventually returned.
        """

        # Fail 3 times, then succeed
        mock_get.side_effect = [
            Mockresponse(502, {}, True),
            Mockresponse(504, {}, True),
            Mockresponse(502, {}, True),
            Mockresponse(200, {"data": "success"}, False),
        ]

        response = self.iterable_object._get("dummy-path")
        # Should eventually succeed
        self.assertEqual(response.status_code, 200)

        # Should have retried 3 times before success
        self.assertEqual(mock_get.call_count, 4)
