import os
import subprocess
import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
from setuptools.command.install import install

class CustomInstallCommand(install):
    """Custom install command to install .deb file."""

    def run(self):
        # Run the standard installation process
        install.run(self)

        # Install the .deb file
        deb_file = os.path.join(os.path.dirname(__file__), 'mosaic', 'gdal', 'gdal_3.10.0-1_amd64.deb')

        if os.path.exists(deb_file):
            try:
                # Ensure root privileges for .deb installation
                if os.geteuid() != 0:
                    print("You need root privileges to install the .deb package.")
                    print("Please run this with sudo or as root.")
                    sys.exit(1)

                # Run dpkg to install the .deb file
                try:
                    subprocess.check_call(['dpkg', '-i', deb_file])
                except subprocess.CalledProcessError as e:
                    subprocess.check_call(['apt-get', 'install', '-f', '-y'])  # Fix dependencies if needed
                    subprocess.check_call(['dpkg', '-i', deb_file])
            except subprocess.CalledProcessError as e:
                print(f"Error installing .deb package: {e}")
                sys.exit(1)
        else:
            print(f"Error: {deb_file} not found.")
            sys.exit(1)

setup(
    cmdclass={
        "install": CustomInstallCommand
    }
)
