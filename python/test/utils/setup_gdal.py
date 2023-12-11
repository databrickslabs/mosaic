import os
import tempfile
import subprocess
from pkg_resources import working_set, Requirement

from test.context import api


class GDALInstaller:
    def __init__(self, spark):
        self._site_packages = working_set.find(Requirement("keplergl")).location
        self._temp_dir = tempfile.TemporaryDirectory()
        self.spark = spark

    def __del__(self):
        self._temp_dir.cleanup()

    def copy_objects(self):
        api.setup_gdal(self.spark, self._temp_dir.name)

    def run_init_script(self):
        gdal_install_script_target = os.path.join(
            self._temp_dir.name, "mosaic-gdal-init.sh"
        )
        os.chmod(gdal_install_script_target, mode=0x744)
        result = subprocess.run(
            [gdal_install_script_target],
            stdout=subprocess.PIPE,
            env=dict(os.environ, DATABRICKS_ROOT_VIRTUALENV_ENV=self._site_packages),
        )
        print(result.stdout.decode())
        return result.returncode

    def test_gdalinfo(self):
        result = subprocess.run(["gdalinfo", "--version"], stdout=subprocess.PIPE)
        return result.stdout.decode()
