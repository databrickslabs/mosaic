#!/bin/bash
# -- 
# This is for Ubuntu 22.04 (Jammy)
# - Corresponds to DBR 13+
# - Jammy offers GDAL 3.4.1
# - !!! This init script should be run from /Volumes !!!
# -     It is for Unity Catalog + Shared Clusters
# Author: Michael Johns | mjohns@databricks.com
# Last Modified: 14 NOV, 2023

GDAL_VERSION=3.4.1
# !!! UPDATE TO YOUR VOLUME DIR !!!
VOLUME_DIR=/Volumes/geospatial_docs/gdal_artifacts/data

# -- refresh package info
#  - ensue updates and backports are available
apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates main universe multiverse restricted"
apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security main multiverse restricted universe"
apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) main multiverse restricted universe"
apt-get update -y

# -- install GDAL
apt-get install -y gdal-bin libgdal-dev

# -- install GDAL python bindings (from PyPI) for version installed
#  - see requirements at https://pypi.org/project/pdal/2.3.0/
#  - make sure GDAL version matches
apt-get install -y python3-gdal

pip install --upgrade pip

# - being explicit
# - vs $(gdalinfo --version | grep -Po '(?<=GDAL )[^;]+' | cut -d, -f1)
pip install GDAL==$GDAL_VERSION

# -- add pre-build JNI shared object to the path
#  - For shared access clusters "/usr/lib/jni" is not a path
#    in System.getProperty("java.library.path"),
#    so let's use "/usr/lib"
# !!! These should come from /Volumes !!!
cp $VOLUME_DIR/libgdalalljni.so /usr/lib
cp $VOLUME_DIR/libgdalalljni.so.30 /usr/lib
cp $VOLUME_DIR/libgdalalljni.so.30.0.3 /usr/lib