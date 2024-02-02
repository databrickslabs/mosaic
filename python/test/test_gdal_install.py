from .utils import SparkTestCase, GDALInstaller


class TestGDALInstall(SparkTestCase):
    def setUp(self) -> None:
        return super().setUp()

    def test_setup_gdal(self):
        installer = GDALInstaller()
        try:
            self.assertTrue(installer.do_op())
        except Exception:
            self.fail("Copying objects with `setup_gdal()` raised an exception.")

        self.assertEqual(
            len(installer.list_files()), 4
        )  # <-  init script and shared objs

        try:
            installer_result = installer.run_init_script()
            self.assertEqual(installer_result, 0)
            gdalinfo_result = installer.test_gdalinfo()
            self.assertEqual(gdalinfo_result, "GDAL 3.4.1, released 2021/12/27\n")
        except Exception:
            self.fail("Execution of GDAL init script raised an exception.")
