import os
from setuptools import setup
from setuptools.command.install import install
import subprocess

'''
These classes are used to hook into setup.py's install process. Depending on the context:
$ pip install my-package

Can yield `setup.py install`, `setup.py egg_info`, or `setup.py develop`
from https://stackoverflow.com/questions/19569557/pip-not-picking-up-a-custom-install-cmdclass
- we are focusing just on install (could add targets for develop, and egg_info if needed)
'''

def custom_command():
    print(f"...invoking custom_command")
    try:
        dbr_version = float(os.environ['DATABRICKS_RUNTIME_VERSION'])
        if dbr_version < 11.3:
            print("...not on a databricks runtime >= 11.3 (nothing to do)")
        else:
            # -- find the resource dir
            HERE_DIR = os.path.abspath(os.path.dirname(__file__))
            print(f"...install dir '{HERE_DIR}'")
            RESOURCE_PARENT_DIR = str(os.path.join(HERE_DIR, 'databricks_mosaic_gdal'))

            # -- untar files to root
            result_tarball1 = subprocess.run(
                ["/bin/tar", "-xf", f"{RESOURCE_PARENT_DIR}/resources/gdal-3.4.3-filetree.tar.xz", "-C", "/"],
                stdout=subprocess.PIPE
            )
            print(f"...untar files {result_tarball1.stdout.decode()}")

            # -- untar symlinks to root
            result_tarball2 = subprocess.run(
                ["/bin/tar", "-xhf", f"{RESOURCE_PARENT_DIR}/resources/gdal-3.4.3-symlinks.tar.xz", "-C", "/"],
                stdout=subprocess.PIPE
            )
            print(f"...untar symlinks {result_tarball2.stdout.decode()}")
            print("...GDAL installed")
    except Exception as e:
        print("...most likely not on a databricks runtime (so nothing to do)")
        print("Install Issue --> " + str(e))


class CustomInstallCommand(install):

    def run(self):
        install.run(self)
        custom_command()


setup(
    cmdclass={
        'install': CustomInstallCommand
    },
)