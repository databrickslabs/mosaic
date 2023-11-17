#!/bin/bash
# -- 
# This is for Ubuntu 22.04 (Jammy)
# - Corresponds to DBR 13+
# - Jammy offers GDAL 3.4.1
# Author: Michael Johns | mjohns@databricks.com
# Last Modified: 15 NOV, 2023

GDAL_VERSION=3.4.1

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
pip install GDAL[numpy]==$GDAL_VERSION

# - add pre-build JNI shared object to the path
wget -P /usr/lib -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so
wget -P /usr/lib -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so.30
#wget -P /usr/lib -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so.30.0.3
