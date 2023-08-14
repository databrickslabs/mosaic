#!/bin/bash
#
# File: init-gdal_3.4.3_ubuntugis.sh
# Author: Michael Johns
# Created: 2022-08-19
#

MOSAIC_GDAL_JNI_DIR="${MOSAIC_GDAL_JNI_DIR:-__DEFAULT_JNI_PATH__}"

rm -rf /var/lib/apt/lists/
mkdir -p /var/lib/apt/lists/
add-apt-repository main
add-apt-repository universe
add-apt-repository restricted
add-apt-repository multiverse
add-apt-repository ppa:ubuntugis/ubuntugis-unstable
apt clean && sudo apt -o Acquire::Retries=3 update --fix-missing -y
apt-get -o Acquire::Retries=3 update -y
apt-get -o Acquire::Retries=3 install -y gdal-bin=3.6.4+dfsg-1~jammy0 libgdal-dev=3.6.4+dfsg-1~jammy0 python3-gdal=3.6.4+dfsg-1~jammy0

# fix python file naming in osgeo package
cd /usr/lib/python3/dist-packages/osgeo
# Loop over all .so files
for file in *.so; do
    # Strip part of name bookended with full-stops and rename file
    new_name=$(echo "$file" | sed 's/\.cpython.*\./\./')
    mv "$file" "$new_name"
done
#cd /usr/lib/python3/dist-packages/osgeo \
#  && mv _gdal.cpython-38-x86_64-linux-gnu.so _gdal.so \
#  && mv _gdal_array.cpython-38-x86_64-linux-gnu.so _gdal_array.so \
#  && mv _gdalconst.cpython-38-x86_64-linux-gnu.so _gdalconst.so \
#  && mv _ogr.cpython-38-x86_64-linux-gnu.so _ogr.so \
#  && mv _gnm.cpython-38-x86_64-linux-gnu.so _gnm.so \
#  && mv _osr.cpython-38-x86_64-linux-gnu.so _osr.so

# add pre-build JNI shared object to the path
# please run MosaicGDAL.copySharedObjects("/dbfs/FileStore/geospatial/mosaic/gdal/") before enabling this init script
#mkdir -p /usr/lib/jni
#cp "${MOSAIC_GDAL_JNI_DIR}/libgdalalljni.so" /usr/lib/jni
#cp "${MOSAIC_GDAL_JNI_DIR}/libgdalalljni.so.30" /usr/lib/jni