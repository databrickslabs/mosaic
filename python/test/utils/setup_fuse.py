import os
import tempfile
import subprocess
from pkg_resources import working_set, Requirement

from test.context import api

class FuseInstaller:
    def __init__(
        self, with_mosaic_pip, with_gdal,
        jar_copy = False, jni_so_copy = False
    ):
        self._site_packages = working_set.find(Requirement("keplergl")).location
        self._temp_dir = tempfile.TemporaryDirectory()
        self.with_mosaic_pip = with_mosaic_pip
        self.with_gdal = with_gdal
        self.jar_copy = jar_copy
        self.jni_so_copy = jni_so_copy
        self.FUSE_INIT_SCRIPT_FILENAME = "mosaic-fuse-init.sh"

    def __del__(self):
        self._temp_dir.cleanup()

    def do_op(self):
        api.setup_fuse_install(
            self._temp_dir.name,
            self.with_mosaic_pip,
            self.with_gdal,
            self.jar_copy,
            self.jni_so_copy, 
            script_out_name=self.FUSE_INIT_SCRIPT_FILENAME
        )

    def run_init_script(self):
        fuse_install_script_target = os.path.join(
            self._temp_dir.name, self.FUSE_INIT_SCRIPT_FILENAME
        )
        os.chmod(fuse_install_script_target, mode=0x744)
        result = subprocess.run(
            [fuse_install_script_target],
            stdout=subprocess.PIPE,
            env=dict(os.environ, DATABRICKS_ROOT_VIRTUALENV_ENV=self._site_packages),
        )
        print(result.stdout.decode())
        return result.returncode

    def list_files(self):
        return os.listdir(self._temp_dir.name)
