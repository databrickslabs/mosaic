#!/bin/bash
# --
# SCRIPT FOR 0.4.0 and 0.4.1
# NOT USED in 0.4.2+
# This is for Ubuntu 22.04 (Jammy)
# [1] corresponds to DBR 13+
# [2] jammy offers GDAL 3.4.1 (default)
#     - backported: ignoring ubuntugis
#     - repo changed to incompatible version
# [3] see Mosaic functions (python) to configure
#     and pre-stage resources:
#     - setup_fuse_install(...) and
#     - setup_gdal(...)
# [4] this script has conditional logic based on variables
# Author: Michael Johns | mjohns@databricks.com
# Last Modified: 29 APR, 2024

# TEMPLATE-BASED REPLACEMENT
# - can also be manually specified
FUSE_DIR='__FUSE_DIR__'
GITHUB_VERSION=__GITHUB_VERSION__
MOSAIC_PIP_VERSION='__MOSAIC_PIP_VERSION__'

# CONDITIONAL LOGIC
WITH_MOSAIC=0    # <- pip install mosaic?
WITH_GDAL=0      # <- install gdal?
WITH_UBUNTUGIS=0 # <- use ubuntugis ppa, now ignored!
WITH_FUSE_SO=0   # <- use fuse dir shared objects (vs wget)

# SPECIFIED VERSIONS
GDAL_VERSION=3.4.1

# - optional: install GDAL
if [ $WITH_GDAL == 1 ]
then
  # - refresh package info
  sudo apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
  sudo apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates main universe multiverse restricted"
  sudo apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security main multiverse restricted universe"
  sudo apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) main multiverse restricted universe"
  sudo apt-get update -y
  
  # - install natives
  sudo apt-get install -y unixodbc libcurl3-gnutls libsnappy-dev libopenjp2-7
  sudo apt-get install -y gdal-bin libgdal-dev python3-numpy python3-gdal

  # - pip install gdal
  pip install --upgrade pip
  pip install gdal==$GDAL_VERSION

  # - add pre-build JNI shared object to the path
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
fi

# - optional: install Mosaic
if [ $WITH_MOSAIC == 1 ]
then
  pip install --upgrade pip
  pip install "databricks-mosaic$MOSAIC_PIP_VERSION"
fi
