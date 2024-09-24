import os
import shutil
import tempfile
from test.context import api

from pkg_resources import Requirement, working_set


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
            script_out_name=self.GDAL_INIT_SCRIPT_FILENAME,
            jni_so_copy=False,
            test_mode=True,
        )

    def list_files(self) -> list[str]:
        return os.listdir(self._temp_dir)
