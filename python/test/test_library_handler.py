import unittest

from .context import MosaicLibraryHandler
from .utils import SparkTestCase


class TestMosaicLibraryHandler(SparkTestCase):
    def test_auto_attach_enabled(self):
        handler = MosaicLibraryHandler(self.spark)
        self.assertFalse(handler.auto_attach_enabled)

    def test_mosaic_library_location(self):
        handler = MosaicLibraryHandler(self.spark)
        self.assertEqual(self.library_location, handler.mosaic_library_location)
