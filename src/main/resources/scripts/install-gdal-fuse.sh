#!/bin/bash
# -- 
# This is for Ubuntu 22.04 (Jammy)
# - Corresponds to DBR 13+
# - Jammy offers GDAL 3.4.1
# NOTES:
#  (a) FUSE_DIR is one of '/Volumes/..', '/Workspace/..', '/dbfs/..'
#  (b) You can call Python `setup_gdal()` or Scala `prepareEnvironment()` to
#      configure this init script with specified FUSE_DIR and 
#      copy it + '.so' files to the FUSE_DIR
# Author: Michael Johns | mjohns@databricks.com
# Last Modified: 16 NOV, 2023

GDAL_VERSION=3.4.1
# !!! SPECIFY YOUR DIR !!!
FUSE_DIR=__FUSE_DIR__

# - refresh package info
# - ensue updates and backports are available
apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates main universe multiverse restricted"
apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security main multiverse restricted universe"
apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) main multiverse restricted universe"
apt-get update -y

# - install GDAL
apt-get install -y gdal-bin libgdal-dev

# - install GDAL python bindings (from PyPI) for version installed
apt-get install -y python3-gdal

pip install --upgrade pip

# - being explicit with pip
pip install GDAL==$GDAL_VERSION

# - add pre-build JNI shared object to the path
# !!! These should come from /Volumes !!!
cp $FUSE_DIR/libgdalalljni.so /usr/lib
cp $FUSE_DIR/libgdalalljni.so.30 /usr/lib
cp $FUSE_DIR/libgdalalljni.so.30.0.3 /usr/lib
