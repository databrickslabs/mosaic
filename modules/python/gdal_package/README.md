# PyPI Project 'databricks-mosaic-gdal' Overview

> Current version is 3.4.3 (to match GDAL).

This is a filetree (vs apt based) drop-in packaging of GDAL with Java Bindings for Ubuntu 20.04 (Focal Fossa) which is used by [Databricks Runtime](https://docs.databricks.com/release-notes/runtime/releases.html) (DBR) 11+. 

1. `gdal-3.4.3-filetree.tar.xz` is ~50MB - it is extracted with `tar -xf gdal-3.4.3-filetree.tar.xz -C /`
2. `gdal-3.4.3.-symlinks.tar.xz` is ~19MB - it is extracted with `tar -xhf gdal-3.4.3-symlinks.tar.xz -C /`

An [init script](https://docs.databricks.com/clusters/init-scripts.html) is provided at [gdal-3.4.3-filetree-init.sh](https://github.com/databrickslabs/mosaic/blob/main/modules/python/gdal_package/databricks-mosaic-gdal/resources/scripts/mosaic-gdal-3.4.3-filetree-init.sh) for managing install of the tarballs across a DBR cluster with install from [PyPI](https://pypi.org/project/databricks-mosaic-gdal/).

 __Notes:__

* This is a very specific packaging for GDAL + dependencies which removes any libraries that are already provided by DBR, so it will not be not useful outside Databricks.
* It additionally includes GDAL shared objects (`.so`) for Java Bindings, GDAL 3.4.3 Python bindings, and tweak for OSGEO as currently supplied by [UbuntuGIS PPA](https://launchpad.net/~ubuntugis/+archive/ubuntu/ubuntugis-unstable) based init script [install-gdal-databricks.sh](https://github.com/databrickslabs/mosaic/blob/main/src/main/resources/scripts/install-gdal-databricks.sh) provided by Mosaic. This install replaces the existing way on Mosaic, so choose one or the other.
* The GDAL JAR for 3.4 is not included but is provided by Mosaic itself and added to your Databricks cluster as part of the [enable_gdal](https://databrickslabs.github.io/mosaic/usage/install-gdal.html#enable-gdal-for-a-notebook) called when configuring Mosaic for GDAL. Separately, the JAR could be added as a [cluster-installed library](https://docs.databricks.com/libraries/cluster-libraries.html#cluster-installed-library), e.g. through Maven coordinates `org.gdal:gdal:3.4.0` from [mvnrepository](https://mvnrepository.com/artifact/org.gdal/gdal/3.4.0).
* Mosaic will soon be able to directly leverage this [PyPI](https://pypi.org/project/databricks-mosaic-gdal/) project and be able to altogether avoid the init script as a precursor to calling [enable_gdal](https://databrickslabs.github.io/mosaic/usage/install-gdal.html#enable-gdal-for-a-notebook). So check Mosaic [GDAL Installation Guide](https://databrickslabs.github.io/mosaic/usage/install-gdal.html#) for any changes on/around APR 2023.