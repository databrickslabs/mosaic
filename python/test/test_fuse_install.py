from .utils import SparkTestCase, FuseInstaller


class TestFuseInstall(SparkTestCase):
    def setUp(self) -> None:
        return super().setUp()

    def test_setup_script_only(self):
        installer = FuseInstaller(jar_copy=False, jni_so_copy=False)
        try:
            self.assertTrue(installer.do_op())
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

        self.assertEqual(len(installer.list_files()),1)  # <- script generated

    def test_setup_jar(self):
        installer = FuseInstaller(jar_copy=True, jni_so_copy=False)
        try:
            self.assertTrue(installer.do_op())
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

        self.assertEqual(len(installer.list_files()), 2)  # <-  init script and jar

    def test_setup_jni(self):
        installer = FuseInstaller(jar_copy=False, jni_so_copy=True)
        try:
            self.assertTrue(installer.do_op())
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

        self.assertEqual(len(installer.list_files()), 4)  # <-  init script and so files

    def test_setup_all(self):
        installer = FuseInstaller(jar_copy=True, jni_so_copy=True)
        try:
            self.assertTrue(installer.do_op())
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

        self.assertEqual(len(installer.list_files()), 5)  # <-  init script jar, and so files
