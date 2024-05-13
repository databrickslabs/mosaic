#!/bin/bash
# --
# This is for Ubuntu 22.04 (Jammy)
# [1] corresponds to DBR 13+
# [2] jammy offers GDAL 3.4.1
# [3] see Mosaic functions (python) to configure
#     and pre-stage resources:
#     - setup_fuse_install(...) and
#     - setup_gdal(...)
# [4] this script has conditional logic based on variables
# [5] stripped back in Mosaic 0.4.2+
# Author: Michael Johns | mjohns@databricks.com
# Last Modified: 29 APR, 2024

# TEMPLATE-BASED REPLACEMENT
# - can also be manually specified
FUSE_DIR='__FUSE_DIR__'

# CONDITIONAL LOGIC
WITH_FUSE_SO=0   # <- use fuse dir shared objects (vs wget)

# refresh package info
# 0.4.2 - added "-y"
sudo apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
sudo apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates main universe multiverse restricted"
sudo apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security main multiverse restricted universe"
sudo apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) main multiverse restricted universe"
sudo apt-get update -y

# install natives
# 0.4.2 added package lock wait (can change value)
sudo apt-get -o DPkg::Lock::Timeout=-1 install -y unixodbc libcurl3-gnutls libsnappy-dev libopenjp2-7
sudo apt-get -o DPkg::Lock::Timeout=-1 install -y gdal-bin libgdal-dev python3-numpy python3-gdal

# pip install gdal
# matches jammy version
pip install --upgrade pip
pip install gdal==3.4.1

# add pre-build JNI shared object to the path
if [ $WITH_FUSE_SO == 1 ]
then
  # copy from fuse dir with no-clobber
  sudo cp -n $FUSE_DIR/libgdalalljni.so /usr/lib
  sudo cp -n $FUSE_DIR/libgdalalljni.so.30 /usr/lib
  sudo cp -n $FUSE_DIR/libgdalalljni.so.30.0.3 /usr/lib
else
  # copy from github
  GITHUB_REPO_PATH=databrickslabs/mosaic/main/resources/gdal/jammy
  sudo wget -nv -P /usr/lib -nc https://raw.githubusercontent.com/$GITHUB_REPO_PATH/libgdalalljni.so
  sudo wget -nv -P /usr/lib -nc https://raw.githubusercontent.com/$GITHUB_REPO_PATH/libgdalalljni.so.30
  sudo wget -nv -P /usr/lib -nc https://raw.githubusercontent.com/$GITHUB_REPO_PATH/libgdalalljni.so.30.0.3
fi
