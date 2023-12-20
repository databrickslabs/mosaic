from .utils import SparkTestCase, GDALInstaller


class TestGDALInstall(SparkTestCase):
    def test_setup_gdal(self):
        installer = GDALInstaller()
        try:
            installer.copy_objects()
        except Exception:
            self.fail("Copying objects with `setup_gdal()` raised an exception.")

        try:
            installer_result = installer.run_init_script()
        except Exception:
            self.fail("Execution of GDAL init script raised an exception.")

        self.assertEqual(installer_result, 0)

        gdalinfo_result = installer.test_gdalinfo()
        self.assertEqual(gdalinfo_result, "GDAL 3.4.1, released 2021/12/27\n")

        self.assertEquals(len(installer.list_files()), 1) # <- just init script