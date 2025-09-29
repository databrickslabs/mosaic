#!/bin/bash

sudo apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
sudo apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates main universe multiverse restricted"
sudo apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security main multiverse restricted universe"
sudo apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) main multiverse restricted universe"
sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable
sudo apt-get update -y

# install natives
sudo apt-get -o DPkg::Lock::Timeout=-1 install -y unixodbc libcurl3-gnutls libsnappy-dev libopenjp2-7
sudo apt-get -o DPkg::Lock::Timeout=-1 install -y gdal-bin libgdal-dev python3-numpy python3-gdal

# pip install gdal
# - make sure python version matches
# - ubuntugis-unstable does change
pip install --upgrade pip
pip install gdal==3.11.3

# update to the actual volume path
cp /Volumes/[path to the .so file in Volumes]/libgdalalljni.so /usr/lib/libgdalalljni.so
cp /Volumes/[path to the .jar file in Volumes]/spatial-0.1.0-jar-with-dependencies.jar /databricks/jars
