#!/bin/bash
#
# File: mosaic-gdal-3.4.3-filetree-init.sh
# Author: Michael Johns
# Modified: 2023-03-21
#  1. script is using custom tarballs for offline / self-contained install of GDAL
#  2. This will unpack files directly into the filetree across cluster nodes (vs run apt install)
#  3. Note: Mosaic will be able to auto-detect and handle tarball unpacking
#           without this init script on/around APR 2023 (so this is an alt to that capability)

# -- install databricks-mosaic-gdal on cluster 
# - from pypi.org (once available)
pip install databricks-mosaic-gdal==3.4.3

# -- find the install dir
# - if this were run in a notebook would use $VIRTUAL_ENV
# - since it is init script it lands in $DATABRICKS_ROOT_VIRTUALENV_ENV
GDAL_RESOURCE_DIR=$(find $DATABRICKS_ROOT_VIRTUALENV_ENV -name "databricks-mosaic-gdal")

# -- untar files to root
# - from databricks-mosaic-gdal install dir
tar -xf $GDAL_RESOURCE_DIR/resources/gdal-3.4.3-filetree.tar.xz -C /

# -- untar symlinks to root
# - from databricks-mosaic-gdal install dir
tar -xhf $GDAL_RESOURCE_DIR/resources/gdal-3.4.3-symlinks.tar.xz -C /
