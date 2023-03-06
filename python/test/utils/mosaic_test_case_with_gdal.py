from test.context import api

from .mosaic_test_case import MosaicTestCase


class MosaicTestCaseWithGDAL(MosaicTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        api.enable_mosaic(cls.spark, install_gdal=True)
