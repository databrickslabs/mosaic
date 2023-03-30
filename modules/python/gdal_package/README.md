# Overview

> Current version is 3.4.3 (to match GDAL).

This is a filetree (vs apt based) drop-in packaging of GDAL with Java Bindings from [UbuntuGIS PPA](https://launchpad.net/~ubuntugis/+archive/ubuntu/ubuntugis-unstable) for Ubuntu 20.04 (Focal Fossa) which is used by [Databricks Runtime](https://docs.databricks.com/release-notes/runtime/releases.html)(DBR) 11+.  __Note: This is a very specific packaging for GDAL + dependencies which removes any libraries that are already provided by DBR, so not useful outside Databricks.__

1. `gdal-3.4.3-filetree.tar.xz` is ~50MB - it is extracted with `tar -xf gdal-3.4.3-filetree.tar.xz -C /`
2. `gdal-3.4.3.-symlinks.tar.xz` is ~19MB - it is extracted with `tar -xhf gdal-3.4.3-symlinks.tar.xz -C /`

An example [init script](https://docs.databricks.com/clusters/init-scripts.html) is provided at `./resources/scripts/mosaic-gdal-3.4.3-filetree-init.sh` for managing install of the tarballs across a DBR cluster.