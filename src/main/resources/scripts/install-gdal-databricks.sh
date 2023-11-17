#!/bin/bash
# -- 
# This is for Ubuntu 22.04 (Jammy)
# - Corresponds to DBR 13+
# - Jammy offers GDAL 3.4.1
# Author: Michael Johns | mjohns@databricks.com
# Last Modified: 17 NOV, 2023

GDAL_VERSION=3.4.1
NUMPY_VERSION=1.21.5

# - refresh package info
# - ensue updates and backports are available
sudo apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
sudo apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates main universe multiverse restricted"
sudo apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security main multiverse restricted universe"
sudo apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) main multiverse restricted universe"
sudo apt-get update -y

 # - install numpy first
pip install --upgrade pip
pip install "numpy>=$NUMPY_VERSION"

# - install natives
sudo apt-get install -y gdal-bin libgdal-dev python3-gdal

# - install gdal with numpy
pip install --no-cache-dir --force-reinstall GDAL[numpy]==$GDAL_VERSION

# - add pre-build JNI shared object to the path
sudo wget -P /usr/lib -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so
sudo wget -P /usr/lib -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so.30
#sudo wget -P /usr/lib -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so.30.0.3
