#!/bin/bash
# -- 
# This is for Ubuntu 22.04 (Jammy)
# - Corresponds to DBR 13+
# - Ubuntugis Jammy offers GDAL 3.4.3
# Author: Michael Johns | mjohns@databricks.com
# Last Modified: 13 NOV, 2023

GDAL_VERSION=3.4.3

# -- refresh package info
#  - ensue updates and backports are available
#  - add ubuntugis ppa
apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates main universe multiverse restricted"
apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security main multiverse restricted universe"
apt-add-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) main multiverse restricted universe"
add-apt-repository ppa:ubuntugis/ppa
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
wget -P /usr/lib -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so
wget -P /usr/lib -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so.30
