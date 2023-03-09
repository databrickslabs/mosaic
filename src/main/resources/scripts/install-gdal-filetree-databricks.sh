#!/bin/bash
#
# File: mosaic-gdal-3.4.3-dbfs-init.sh
# Author: Michael Johns
# Modified: 2023-03-06
#  1. script is using custom tarballs for offline / self-contained install of GDAL
#  2. This will unpack files directly into the filetree across cluster nodes (vs run apt install)

# -- download tarballs from dbfs
# - files and symlinks
cp /dbfs/databricks-mosaic/gdal_3.4.3/gdal-3.4.3-filetree.tar.gz .
cp /dbfs/databricks-mosaic/gdal_3.4.3/gdal-3.4.3-symlinks.tar.gz .

# -- untar files
tar -xzf gdal-3.4.3-filetree.tar.gz -C /

# -- untar symlinks
tar -xzhf gdal-3.4.3-symlinks.tar.gz -C /
