import unittest
from unittest.mock import MagicMock, patch

import requests

from tap_iterable.discover import (
    _apply_access_checks,
    _prune_inaccessible_children,
    discover_streams,
)
from tap_iterable.exceptions import (
    IterableForbiddenError,
    raise_for_error,
)
from tap_iterable.streams import STREAMS


class MockResponse:
    """Minimal requests.Response stand-in for raise_for_error tests."""

    def __init__(self, status_code, json_body=None):
        self.status_code = status_code
        self._json = json_body or {}

    def raise_for_status(self):
        raise requests.HTTPError("mock error")

    def json(self):
        return self._json


class TestRaiseForError403(unittest.TestCase):
    """Verify that a 403 response raises IterableForbiddenError (not retried by backoff)."""

    def test_403_raises_iterable_forbidden_error(self):
        response = MockResponse(403)
        with self.assertRaises(IterableForbiddenError) as ctx:
            raise_for_error(response)
        self.assertIn("403", str(ctx.exception))

    def test_403_uses_default_message_when_body_has_no_message(self):
        response = MockResponse(403, json_body={})
        with self.assertRaises(IterableForbiddenError) as ctx:
            raise_for_error(response)
        self.assertIn("read", str(ctx.exception))

    def test_403_uses_api_message_when_present(self):
        response = MockResponse(403, json_body={"message": "Forbidden by policy"})
        with self.assertRaises(IterableForbiddenError) as ctx:
            raise_for_error(response)
        self.assertIn("Forbidden by policy", str(ctx.exception))


class TestCheckAccess(unittest.TestCase):
    """Unit tests for Stream.check_access()."""

    def _make_client(self):
        return MagicMock()

    def test_check_access_returns_true_for_child_stream(self):
        """Child streams (with a parent attribute) always return True."""
        from tap_iterable.streams import ListUsers
        stream = ListUsers(client=self._make_client())
        self.assertTrue(stream.check_access())

    def test_check_access_returns_true_when_rest_endpoint_ok(self):
        """REST streams return True when the API call succeeds."""
        from tap_iterable.streams import Channels
        client = self._make_client()
        mock_response = MagicMock()
        client._get.return_value = mock_response
        stream = Channels(client=client)
        result = stream.check_access()
        client._get.assert_called_once_with("channels", stream=True)
        mock_response.close.assert_called_once()
        self.assertTrue(result)

    def test_check_access_returns_false_on_forbidden_rest(self):
        """REST streams return False when the API call raises IterableForbiddenError."""
        from tap_iterable.streams import Channels
        client = self._make_client()
        client._get.side_effect = IterableForbiddenError("403 Forbidden")
        stream = Channels(client=client)
        result = stream.check_access()
        self.assertFalse(result)

    def test_check_access_returns_true_for_export_stream_when_ok(self):
        """Data export streams return True when the export endpoint responds successfully."""
        from tap_iterable.streams import EmailBounce
        client = self._make_client()
        mock_response = MagicMock()
        client._get.return_value = mock_response
        stream = EmailBounce(client=client)
        result = stream.check_access()
        self.assertTrue(result)
        mock_response.close.assert_called_once()

    def test_check_access_returns_false_on_forbidden_export(self):
        """Data export streams return False when the export endpoint returns 403."""
        from tap_iterable.streams import EmailBounce
        client = self._make_client()
        client._get.side_effect = IterableForbiddenError("403 Forbidden")
        stream = EmailBounce(client=client)
        result = stream.check_access()
        self.assertFalse(result)

    def test_check_access_returns_true_no_endpoint(self):
        """Streams with no endpoint and no data_type_name always return True."""
        from tap_iterable.streams import Stream
        stream = Stream(client=self._make_client())
        result = stream.check_access()
        self.assertTrue(result)


class TestPruneInaccessibleChildren(unittest.TestCase):
    """Unit tests for _prune_inaccessible_children()."""

    def test_child_remains_when_parent_present(self):
        """list_users stays in the catalog when lists is accessible."""
        accessible = ["lists", "list_users", "channels"]
        _prune_inaccessible_children(accessible)
        self.assertIn("list_users", accessible)

    def test_child_removed_when_parent_absent(self):
        """list_users is removed when lists is not accessible."""
        accessible = ["list_users", "channels", "campaigns"]
        _prune_inaccessible_children(accessible)
        self.assertNotIn("list_users", accessible)

    def test_non_child_streams_unaffected(self):
        """Streams without a parent are not removed by pruning."""
        accessible = ["channels", "campaigns", "metadata"]
        _prune_inaccessible_children(accessible)
        self.assertEqual(["channels", "campaigns", "metadata"], accessible)


class TestApplyAccessChecks(unittest.TestCase):
    """Unit tests for _apply_access_checks()."""

    def _mock_client(self):
        return MagicMock()

    def test_all_streams_accessible(self):
        """No streams are removed when all check_access() calls return True."""
        client = self._mock_client()
        accessible = list(STREAMS.keys())

        with patch("tap_iterable.streams.Stream.check_access", return_value=True):
            _apply_access_checks(client, accessible)

        self.assertEqual(sorted(accessible), sorted(STREAMS.keys()))

    def test_inaccessible_stream_removed(self):
        """A stream whose check_access() returns False is removed from the catalog."""
        client = self._mock_client()
        accessible = list(STREAMS.keys())

        def fake_check_access(self):
            if self.name == "channels":
                return False
            return True

        with patch("tap_iterable.streams.Stream.check_access", fake_check_access):
            _apply_access_checks(client, accessible)

        self.assertNotIn("channels", accessible)

    def test_child_removed_when_parent_inaccessible(self):
        """list_users is removed when lists is inaccessible."""
        client = self._mock_client()
        accessible = list(STREAMS.keys())

        def fake_check_access(self):
            if self.name == "lists":
                return False
            return True

        with patch("tap_iterable.streams.Stream.check_access", fake_check_access):
            _apply_access_checks(client, accessible)

        self.assertNotIn("lists", accessible)
        self.assertNotIn("list_users", accessible)

    def test_all_parent_streams_inaccessible_raises(self):
        """IterableForbiddenError is raised when every parent stream is inaccessible."""
        client = self._mock_client()
        accessible = list(STREAMS.keys())

        def fake_check_access(self):
            # Child streams return True; all parents return False
            if getattr(self, "parent", None):
                return True
            return False

        with patch("tap_iterable.streams.Stream.check_access", fake_check_access):
            with self.assertRaises(IterableForbiddenError):
                _apply_access_checks(client, accessible)

    def test_partial_inaccessible_does_not_raise(self):
        """No exception raised when at least one parent stream is accessible."""
        client = self._mock_client()
        accessible = list(STREAMS.keys())

        parent_streams = [name for name, cls in STREAMS.items() if not getattr(cls, "parent", None)]
        # Make only the first parent inaccessible, rest accessible
        first_parent = parent_streams[0]

        def fake_check_access(self):
            if self.name == first_parent:
                return False
            return True

        with patch("tap_iterable.streams.Stream.check_access", fake_check_access):
            # Should not raise
            _apply_access_checks(client, accessible)

        self.assertNotIn(first_parent, accessible)

    def test_all_inaccessible_error_message(self):
        client = MagicMock()
        accessible = list(STREAMS.keys())
        with patch('tap_iterable.streams.Stream.check_access', return_value=False):
            with self.assertRaises(IterableForbiddenError) as ctx:
                _apply_access_checks(client, accessible)
        self.assertIn('403', str(ctx.exception))
        self.assertIn("do not have 'read' access to any supported streams", str(ctx.exception))

    def test_all_inaccessible_exact_error_message(self):
        """Validate the exact error message raised when no streams are accessible."""
        client = MagicMock()
        accessible = list(STREAMS.keys())
        expected_message = (
            "HTTP-error-code: 403, Error: The credentials "
            "do not have 'read' access to any supported streams."
        )
        with patch('tap_iterable.streams.Stream.check_access', return_value=False):
            with self.assertRaises(IterableForbiddenError) as ctx:
                _apply_access_checks(client, accessible)
        self.assertEqual(expected_message, str(ctx.exception))


class TestDiscoverStreams(unittest.TestCase):
    """Integration-style unit tests for discover_streams()."""

    def test_discover_streams_returns_all_when_accessible(self):
        """discover_streams() returns an entry for each stream when all are accessible."""
        client = MagicMock()

        with patch("tap_iterable.streams.Stream.check_access", return_value=True), \
             patch("tap_iterable.streams.Stream.load_schema", return_value={}), \
             patch("tap_iterable.streams.Stream.load_metadata", return_value=[]):
            result = discover_streams(client)

        self.assertEqual(len(result), len(STREAMS))
        stream_names = {s["stream"] for s in result}
        self.assertEqual(stream_names, {cls.name for cls in STREAMS.values()})

    def test_discover_streams_excludes_inaccessible(self):
        """discover_streams() omits a stream that check_access() rejects."""
        client = MagicMock()

        def fake_check_access(self):
            return self.name != "channels"

        with patch("tap_iterable.streams.Stream.check_access", fake_check_access), \
             patch("tap_iterable.streams.Stream.load_schema", return_value={}), \
             patch("tap_iterable.streams.Stream.load_metadata", return_value=[]):
            result = discover_streams(client)

        stream_names = [s["stream"] for s in result]
        self.assertNotIn("channels", stream_names)
        self.assertEqual(len(result), len(STREAMS) - 1)
