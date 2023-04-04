# PyPI Project 'databricks-mosaic-gdal' Overview

> Current version is 3.4.3 (to match GDAL).

This is a filetree (vs apt based) drop-in packaging of GDAL with Java Bindings for Ubuntu 20.04 (Focal Fossa) which is used by [Databricks Runtime](https://docs.databricks.com/release-notes/runtime/releases.html) (DBR) 11.3+. 

1. `gdal-3.4.3-filetree.tar.xz` is ~50MB - it is extracted with `tar -xf gdal-3.4.3-filetree.tar.xz -C /`
2. `gdal-3.4.3.-symlinks.tar.xz` is ~19MB - it is extracted with `tar -xhf gdal-3.4.3-symlinks.tar.xz -C /`

__This (databricks-mosaic-gdal) package, starting in version `3.4.3.post4`, auto-detects and handles tarball unpacking without requiring an init script, e.g. install in a notebook with `%pip install databricks-mosaic-gdal`.__

 __Requirements:__

* This will not install the GDAL tarballs if not on Databricks Runtime >= 11.3
* Added benefit of not accidentally installing on your local machine which would be bad since it unpacks to root (`/`)
* This is deployed to pypi without a wheel (WHL) to avoid uneccessary duplication of the resource files and to trigger `setup.py` building on install

 __Notes:__

* This is a very specific packaging for GDAL + dependencies which removes any libraries that are already provided by DBR, so it __will not be not useful outside Databricks.__
* It additionally includes GDAL shared objects (`.so`) for Java Bindings, GDAL 3.4.3 Python bindings, and tweak for OSGEO as currently supplied by [UbuntuGIS PPA](https://launchpad.net/~ubuntugis/+archive/ubuntu/ubuntugis-unstable) based init script [install-gdal-databricks.sh](https://github.com/databrickslabs/mosaic/blob/main/src/main/resources/scripts/install-gdal-databricks.sh) provided by Mosaic. __This install replaces the existing way on Mosaic, so choose one or the other.__
* The GDAL JAR for 3.4 is not included but is provided by Mosaic itself and added to your Databricks cluster as part of the [enable_gdal](https://databrickslabs.github.io/mosaic/usage/install-gdal.html#enable-gdal-for-a-notebook) called when configuring Mosaic for GDAL. Separately, the JAR could be added as a [cluster-installed library](https://docs.databricks.com/libraries/cluster-libraries.html#cluster-installed-library), e.g. through Maven coordinates `org.gdal:gdal:3.4.0` from [mvnrepository](https://mvnrepository.com/artifact/org.gdal/gdal/3.4.0).
* Starting with version `3.4.3.post2`,  Mosaic is able to directly leverage this [PyPI](https://pypi.org/project/databricks-mosaic-gdal/) project and can altogether avoid the init script as a precursor to calling [enable_gdal](https://databrickslabs.github.io/mosaic/usage/install-gdal.html#enable-gdal-for-a-notebook):

_The install in a notebook for both Mosaic and GDAL would look like:_

```
# install order doesn't matter
%pip install databricks-mosaic databricks-mosaic-gdal
```

_Then you can initialize:_

```
import mosaic as mos
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)
```

_Check Mosaic [GDAL Installation Guide](https://databrickslabs.github.io/mosaic/usage/install-gdal.html#) for updated instructions on/around APR 2023._