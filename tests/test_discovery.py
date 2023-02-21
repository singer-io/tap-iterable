import unittest

from tap_tester import menagerie, connections
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest

from base import IterableBase


class IterableDiscoveryTest(DiscoveryTest, IterableBase):
    """Standard Discovery Test"""

    @staticmethod
    def name():
        return "tt_iterable_discovery"

    def streams_to_test(self):
        return set(self.expected_metadata().keys())