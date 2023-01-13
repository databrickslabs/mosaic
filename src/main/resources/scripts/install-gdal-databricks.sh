#!/bin/bash
#
# File: init-gdal_3.4.3_ubuntugis.sh
# Author: Michael Johns
# Created: 2022-08-19
#

sudo rm -r /var/lib/apt/lists/*
sudo add-apt-repository main
sudo add-apt-repository universe
sudo add-apt-repository restricted
sudo add-apt-repository multiverse
sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable
sudo apt clean && sudo apt -o Acquire::Retries=3 update --fix-missing -y
sudo apt-get -o Acquire::Retries=3 update -y
sudo apt-get -o Acquire::Retries=3 install -y gdal-bin=3.4.3+dfsg-1~focal0 libgdal-dev=3.4.3+dfsg-1~focal0 python3-gdal=3.4.3+dfsg-1~focal0

# fix python file naming in osgeo package
cd /usr/lib/python3/dist-packages/osgeo \
  && mv _gdal.cpython-38-x86_64-linux-gnu.so _gdal.so \
  && mv _gdal_array.cpython-38-x86_64-linux-gnu.so _gdal_array.so \
  && mv _gdalconst.cpython-38-x86_64-linux-gnu.so _gdalconst.so \
  && mv _ogr.cpython-38-x86_64-linux-gnu.so _ogr.so \
  && mv _gnm.cpython-38-x86_64-linux-gnu.so _gnm.so \
  && mv _osr.cpython-38-x86_64-linux-gnu.so _osr.so

# add pre-build JNI shared object to the path
# please run MosaicGDAL.copySharedObjects("/dbfs/FileStore/geospatial/mosaic/gdal/") before enabling this init script
mkdir -p /usr/lib/jni
cp /dbfs/FileStore/geospatial/mosaic/gdal/libgdalalljni.so /usr/lib/jni
cp /dbfs/FileStore/geospatial/mosaic/gdal/libgdalalljni.so.30 /usr/lib/jni