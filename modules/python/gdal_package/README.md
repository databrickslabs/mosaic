# Overview

> Current version is 3.4.3 (to match GDAL).

This is a packaging of GDAL with Java Bindings from [UbuntuGIS PPA](https://launchpad.net/~ubuntugis/+archive/ubuntu/ubuntugis-unstable) for Ubuntu 20.04 (Focal Fossa) which is used by [Databricks Runtime](https://docs.databricks.com/release-notes/runtime/releases.html)(DBR) 11+.  __Note: This is a very specific packaging for GDAL + dependencies which removes any libraries that are already provided by DBR, so not useful outside Databricks.__

1. `gdal-3.4.3-filetree.tar.gz` is ~92MB - it is extracted with `tar -xzf gdal-3.4.3-filetree.tar.gz -C /`
2. `gdal-3.4.3.-symlinks.tar.gz` is ~27MB - it is extracted with `tar -xzhf gdal-3.4.3-symlinks.tar.gz -C /`

An example [init script](https://docs.databricks.com/clusters/init-scripts.html) is provided at `./resources/scripts/mosaic-gdal-3.4.3-filetree-init.sh` for managing install of the tarballs across a DBR cluster.