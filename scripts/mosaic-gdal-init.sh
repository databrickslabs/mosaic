#!/bin/bash
# -- 
# This is for Ubuntu 22.04 (Jammy)
# [1] corresponds to DBR 13+
# [2] jammy offers GDAL 3.4.1 (default)
#     - optional: Ubuntugis offers GDAL 3.4.3,
#       with additional ppa added
# [3] see Mosaic functions (python) to configure
#     and pre-stage resources:
#     - setup_fuse_install(...) and
#     - setup_gdal(...)
# [4] this script has conditional logic based on variables
# Author: Michael Johns | mjohns@databricks.com
# Last Modified: 19 NOV, 2023

# TEMPLATE-BASED REPLACEMENT
# - can also be manually specified
FUSE_DIR='__FUSE_DIR__'
GITHUB_VERSION=__GITHUB_VERSION__
MOSAIC_PIP_VERSION='__MOSAIC_PIP_VERSION__'

# CONDITIONAL LOGIC
WITH_MOSAIC=0    # <- pip install mosaic?
WITH_GDAL=0      # <- install gdal?
WITH_UBUNTUGIS=0 # <- use ubuntugis ppa?
WITH_FUSE_SO=0   # <- use fuse dir shared objects (vs wget) 

# SPECIFIED VERSIONS 
# - may be changed by conditional logic
GDAL_VERSION=3.4.1   # <- matches Jammy (default)
NUMPY_VERSION=1.21.5 # <- matches DBR 13.3

# - optional: install Mosaic
if [ $WITH_MOSAIC == 1 ]
then
  pip install "databricks-mosaic$MOSAIC_PIP_VERSION"
fi

# - optional: install GDAL
if [ $WITH_GDAL == 1 ]
then
  # - refresh package info
  sudo apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
  sudo apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates main universe multiverse restricted"
  sudo apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security main multiverse restricted universe"
  sudo apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) main multiverse restricted universe"
  if [ $WITH_UBUNTUGIS == 1 ]
  then
    sudo add-apt-repository ppa:ubuntugis/ppa
    GDAL_VERSION=3.4.3 # <- update gdal version
  fi
  sudo apt-get update -y

  # - install numpy first
  pip install --upgrade pip
  pip install "numpy>=$NUMPY_VERSION"

  # - install natives
  sudo apt-get install -y gdal-bin libgdal-dev python3-gdal

  # - install gdal with numpy
  pip install --no-cache-dir --force-reinstall GDAL[numpy]==$GDAL_VERSION

  # - add pre-build JNI shared object to the path
  if [ $WITH_FUSE_SO == 1 ]
  then
    # copy from fuse dir
    cp $FUSE_DIR/libgdalalljni.so /usr/lib
    cp $FUSE_DIR/libgdalalljni.so.30 /usr/lib
    cp $FUSE_DIR/libgdalalljni.so.30.0.3 /usr/lib
  else
    # copy from github
    # - !!! TODO: MODIFY PATH ONCE PR MERGES !!! 
    # - THIS WILL USE GITHUB_VERSION
    GITHUB_REPO_PATH=databrickslabs/mosaic/main/src/main/resources/gdal/ubuntu
    
    sudo wget -P /usr/lib -nc https://raw.githubusercontent.com/$GITHUB_REPO_PATH/libgdalalljni.so
    sudo wget -P /usr/lib -nc https://raw.githubusercontent.com/$GITHUB_REPO_PATH/libgdalalljni.so.30
    #sudo wget -P /usr/lib -nc https://raw.githubusercontent.com/$GITHUB_REPO_PATH/libgdalalljni.so.30.0.3
  fi
fi
