from .utils import SparkTestCase, FuseInstaller


class TestFuseInstall(SparkTestCase):
    def setUp(self) -> None:
        return super().setUp()

    def test_setup_no_op(self):
        installer = FuseInstaller(False, False, jar_copy=False, jni_so_copy=False)
        try:
            self.assertTrue(installer.do_op())
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

        self.assertEqual(len(installer.list_files()), 0)  # <- nothing generated

    def test_setup_jar_only(self):
        installer = FuseInstaller(False, False, jar_copy=True, jni_so_copy=False)
        try:
            self.assertTrue(installer.do_op())
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

    def test_setup_sh_pip_only(self):
        installer = FuseInstaller(True, False, jar_copy=False, jni_so_copy=False)
        try:
            self.assertTrue(installer.do_op())
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

        self.assertEqual(len(installer.list_files()), 1)  # <- just init script

    def test_setup_sh_gdal(self):
        installer = FuseInstaller(False, True, jar_copy=False, jni_so_copy=False)
        try:
            self.assertTrue(installer.do_op())
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

        self.assertEqual(len(installer.list_files()), 1)  # <- just init script

    def test_setup_sh_gdal_jni(self):
        installer = FuseInstaller(False, True, jar_copy=False, jni_so_copy=True)
        try:
            self.assertTrue(installer.do_op())
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

    def test_setup_sh_all(self):
        installer = FuseInstaller(True, True, jar_copy=True, jni_so_copy=True)
        try:
            self.assertTrue(installer.do_op())
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")
