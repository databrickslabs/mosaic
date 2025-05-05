#!/bin/bash

sudo apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
sudo apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates main universe multiverse restricted"
sudo apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security main multiverse restricted universe"
sudo apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) main multiverse restricted universe"
sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable
sudo apt-get update -y

# install natives
# 0.4.2 added package lock wait (can change value)
sudo apt-get -o DPkg::Lock::Timeout=-1 install -y unixodbc libcurl3-gnutls libsnappy-dev libopenjp2-7
sudo apt-get -o DPkg::Lock::Timeout=-1 install -y gdal-bin libgdal-dev python3-numpy python3-gdal

# pip install gdal
# matches jammy version
pip install --upgrade pip
pip install gdal==3.9.3

mkdir -p mkdir -p /usr/lib/jni/
cp /Volumes/milostest/eytest/mosaic43/libgdalalljni.so /usr/lib/jni/libgdalalljni.so
cp /Volumes/milostest/eytest/mosaic43/libgdalalljni.so /usr/lib/jni/gdalalljni.so
cp /Volumes/milostest/eytest/mosaic43/libgdalalljni.so /usr/lib/libgdalalljni.so
cp /Volumes/milostest/eytest/mosaic43/libgdalalljni.so /usr/lib/gdalalljni.so
cp /Volumes/milostest/eytest/mosaic43/mosaic-0.4.3-jar-with-dependencies.jar /databricks/jars
