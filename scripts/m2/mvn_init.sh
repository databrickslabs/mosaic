#!/bin/bash

# [1] unset variable for this script
echo -e "\n::: [1] ... unsetting JAVA_TOOL_OPTIONS (probably need to do in container as well) :::"
unset JAVA_TOOL_OPTIONS

# [2] copy custom settings.xml
# - defaults to new skipScoverage profile
# - compliments the pom config (profile sCoverage also added there)
# - sets .m2 folder to be in project
echo -e "\n::: [2] ... setting up new .m2 (in project) + new skipScoverage profile (as default) :::"
mv /usr/local/share/maven/conf/settings.xml /usr/local/share/maven/conf/settings.xml.BAK
cp /root/mosaic/scripts/m2/settings.xml /usr/local/share/maven/conf
echo -e "        ... mvn active profile(s)\n"
cd /root/mosaic && mvn help:active-profiles

# [3] build JVM code
# this is building for container JDK
# see settings.xml for overrides
echo -e "\n::: [3] ... build JVM code version? :::\n"
echo -e "        $(javac -version)"
cd /root/mosaic && mvn clean package -DskipTests

# [4] build python
echo -e "\n::: [4] ... build python :::\n"
pip install --upgrade pip
cd /root/mosaic/python && pip install .
pip install black build isort py4j requests

# [5] run gdal install script
# - this is adapted from 0.4.2/mosaic-gdal-init.sh
echo -e "\n::: [5] ... setup gdal :::\n" 

# refresh package info
apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates main universe multiverse restricted"
apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security main multiverse restricted universe"
apt-add-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) main multiverse restricted universe"
apt-get update -y

# install natives
# - gdal natives already installed
apt-get -o DPkg::Lock::Timeout=-1 install -y python3-numpy unixodbc libcurl3-gnutls libsnappy-dev libopenjp2-7

# pip install gdal
# - match the container version (jammy default)
pip install gdal==3.4.1
