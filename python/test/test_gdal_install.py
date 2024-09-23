from .utils import GDALInstaller, SparkTestCase


class TestGDALInstall(SparkTestCase):
    def setUp(self) -> None:
        return super().setUp()

    def test_setup_gdal(self):
        installer = GDALInstaller()
        try:
            self.assertTrue(installer.do_op())
        except Exception:
            self.fail("Copying objects with `setup_gdal()` raised an exception.")

        self.assertEqual(len(installer.list_files()), 1)  # <- init script
