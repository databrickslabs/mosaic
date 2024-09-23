import os
import shutil
import subprocess
import tempfile
from test.context import api

from pkg_resources import Requirement, working_set


class FuseInstaller:
    def __init__(self, jar_copy=False, jni_so_copy=False):
        self._site_packages = working_set.find(Requirement("keplergl")).location
        self._temp_dir = tempfile.mkdtemp()
        self.jar_copy = jar_copy
        self.jni_so_copy = jni_so_copy
        self.FUSE_INIT_SCRIPT_FILENAME = "mosaic-fuse-init.sh"

    def __del__(self):
        shutil.rmtree(self._temp_dir)

    def do_op(self) -> bool:
        return api.setup_fuse_install(
            self._temp_dir,
            jar_copy=self.jar_copy,
            jni_so_copy=self.jni_so_copy,
            script_out_name=self.FUSE_INIT_SCRIPT_FILENAME,
            test_mode=True,
        )

    def run_init_script(self) -> int:
        fuse_install_script_target = os.path.join(
            self._temp_dir, self.FUSE_INIT_SCRIPT_FILENAME
        )
        os.chmod(fuse_install_script_target, mode=0x744)
        result = subprocess.run(
            [fuse_install_script_target],
            stdout=subprocess.DEVNULL,
            env=dict(os.environ, DATABRICKS_ROOT_VIRTUALENV_ENV=self._site_packages),
        )
        return result.returncode

    def list_files(self) -> list[str]:
        return os.listdir(self._temp_dir)
