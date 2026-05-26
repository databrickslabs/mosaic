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

    required_os_packages = [
        "gpsbabel",
        "libavif-dev",
        "libblosc-dev",
        "libboost-dev",
        "libcairo2-dev",
        "libcfitsio-dev",
        "libcrypto++-dev",
        "libcurl4-gnutls-dev",
        "libexpat-dev",
        "libfcgi-dev",
        "libfyba-dev",
        "libfreexl-dev",
        "libgeos-dev",
        "libgeotiff-dev",
        "libgif-dev",
        "libhdf4-alt-dev",
        "libhdf5-serial-dev",
        "libjpeg-dev",
        "libkml-dev",
        "liblcms2-2",
        "liblz4-dev",
        "liblzma-dev",
        "libmysqlclient-dev",
        "libnetcdf-dev",
        "libogdi-dev",
        "libopenexr-dev",
        "libopenjp2-7-dev",
        "libpcre3-dev",
        "libpng-dev",
        "libpoppler-dev",
        "libpoppler-private-dev",
        "libpq-dev",
        "libproj-dev",
        "librasterlite2-dev",
        "libspatialite-dev",
        "libssl-dev",
        "libwebp-dev",
        "libxerces-c-dev",
        "libxml2-dev",
        "libxslt-dev",
        "libzstd-dev",
        "locales",
        "mysql-client-core-8.0",
        "netcdf-bin",
        "libodbcinst2",
        "libprotobuf32",
        "libthrift-0.19.0",
        "libodbc2",
        "libre2-10",
        "libutf8proc3",
    ]

    @staticmethod
    def am_root():
        return os.geteuid() == 0

    def run(self):

        prepend = []

        if not self.am_root():
            prepend.append("sudo")

        # Install base dependencies
        subprocess.check_call(prepend + ["apt-get", "update"])
        subprocess.check_call(prepend + ["apt-get", "install", "-y", *self.required_os_packages])

        # Install the .deb file
        deb_file = os.path.join(
            os.path.dirname(__file__), "mosaic", "gdal", "gdal_3.10.2-1_amd64.deb"
        )

        if os.path.exists(deb_file):
            try:
                subprocess.check_call(prepend + ["dpkg", "-i", deb_file])
            except subprocess.CalledProcessError as e:
                print(f"Error installing .deb package: {e}")
                sys.exit(1)
        else:
            print(f"Error: {deb_file} not found.")
            sys.exit(1)
        # Run the standard installation process
        install.run(self)


setup(cmdclass={"install": CustomInstallCommand})
