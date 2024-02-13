import os
import shutil
import subprocess
import tempfile
from pkg_resources import working_set, Requirement

from test.context import api


class GDALInstaller:
    def __init__(self):
        self._site_packages = working_set.find(Requirement("keplergl")).location
        self._temp_dir = tempfile.mkdtemp()
        self.GDAL_INIT_SCRIPT_FILENAME = "mosaic-gdal-init.sh"

    def __del__(self):
        shutil.rmtree(self._temp_dir)

    def do_op(self) -> bool:
        return api.setup_gdal(
            to_fuse_dir=self._temp_dir,
            override_mosaic_version="main",
            script_out_name=self.GDAL_INIT_SCRIPT_FILENAME,
            jni_so_copy=True,
        )

    def run_init_script(self) -> int:
        gdal_install_script_target = os.path.join(
            self._temp_dir, self.GDAL_INIT_SCRIPT_FILENAME
        )
        os.chmod(gdal_install_script_target, mode=0x744)
        result = subprocess.run(
            [gdal_install_script_target],
            stdout=subprocess.DEVNULL,
            env=dict(os.environ, DATABRICKS_ROOT_VIRTUALENV_ENV=self._site_packages),
        )
        return result.returncode

    def list_files(self) -> list[str]:
        return os.listdir(self._temp_dir)

    def test_gdalinfo(self) -> str:
        result = subprocess.run(["gdalinfo", "--version"], stdout=subprocess.PIPE)
        return result.stdout.decode()
