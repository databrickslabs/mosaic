from .utils import SparkTestCase, FuseInstaller


class TestFuseInstall(SparkTestCase):

    def test_setup_no_op(self):
        installer = FuseInstaller(False, False, jar_copy=False, jni_so_copy=False)
        try:
            installer.do_op()
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

        self.assertEquals(len(installer.list_files()), 0) 

    def test_setup_jar_only(self):
        installer = FuseInstaller(False, False, jar_copy=True, jni_so_copy=False)
        try:
            installer.do_op()
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")
        
        files = installer.list_files()
        self.assertEquals(len(files), 1)
        self.assertEquals(files[0][-4:].lower(), '.jar')
    
    def test_setup_sh_pip_only(self):
        installer = FuseInstaller(True, False, jar_copy=False, jni_so_copy=False)
        try:
            installer.do_op()
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

        try:
            installer_result = installer.run_init_script()
        except Exception:
            self.fail("Running fuse init script raised an exception.")
        self.assertEqual(installer_result, 0)

        files = installer.list_files()
        self.assertEquals(len(files), 1) 
        self.assertEquals(files[0][-3:].lower(), '.sh')

    def test_setup_sh_gdal(self):
        installer = FuseInstaller(False, True, jar_copy=False, jni_so_copy=False)
        try:
            installer.do_op()
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

        try:
            installer_result = installer.run_init_script()
        except Exception:
            self.fail("Running fuse init script raised an exception.")
        self.assertEqual(installer_result, 0)

        files = installer.list_files()
        self.assertEquals(len(files), 1) 
        self.assertEquals(files[0][-3:].lower(), '.sh')
    
    def test_setup_sh_gdal_jni(self):
        installer = FuseInstaller(False, True, jar_copy=False, jni_so_copy=True)
        try:
            installer.do_op()
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

        try:
            installer_result = installer.run_init_script()
        except Exception:
            self.fail("Running fuse init script raised an exception.")
        self.assertEqual(installer_result, 0)

        files = installer.list_files()
        self.assertEqual(len(files), 4)  

        found_sh = False
        so_cnt = 0
        for f in files:
            if f.lower().endswith('.sh'):
                found_sh = True
            elif 'libgdalall.jni.so' in f.lower():
                so_cnt += 1
        self.assertTrue(found_sh)
        self.assertEqual(so_cnt, 3)
    
    def test_setup_sh_all(self):
        installer = FuseInstaller(True, True, jar_copy=True, jni_so_copy=True)
        try:
            installer.do_op()
        except Exception:
            self.fail("Executing `setup_fuse_install()` raised an exception.")

        try:
            installer_result = installer.run_init_script()
        except Exception:
            self.fail("Running fuse init script raised an exception.")
        self.assertEqual(installer_result, 0)

        files = installer.list_files()
        self.assertEqual(len(files), 5)  

        found_sh = False
        found_jar = False
        so_cnt = 0
        for f in files:
            if f.lower().endswith('.sh'):
                found_sh = True
            elif f.lower().endswith('.jar'):
                found_jar = True
            elif 'libgdalall.jni.so' in f.lower():
                so_cnt += 1
        self.assertTrue(found_sh)
        self.assertTrue(found_jar)
        self.assertEqual(so_cnt, 3)
