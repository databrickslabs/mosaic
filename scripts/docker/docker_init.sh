#!/bin/bash

# [1] unset variable for this script
echo "\n::: [1] ... unsetting JAVA_TOOL_OPTIONS (probably need to do in container as well) :::"
unset JAVA_TOOL_OPTIONS

# [2] copy custom settings.xml
# - defaults to new skipScoverage profile
# - compliments the pom config (profile sCoverage also added there)
# - sets .m2 folder to be in project
echo "\n::: [2] ... setting up new .m2 (in project) + new skipScoverage profile (as default) :::"
mv /usr/local/share/maven/conf/settings.xml /usr/local/share/maven/conf/settings.xml.BAK
cp /root/mosaic/scripts/docker/m2/settings.xml /usr/local/share/maven/conf
echo "        ... mvn active profile(s)\n"
cd /root/mosaic && mvn help:active-profiles

# [3] build JVM code
# this is building for container JDK
# see settings.xml for overrides
echo "\n::: [3] ... maven package - JVM code version? :::\n"
echo "        $(javac -version)"
cd /root/mosaic && mvn package -DskipTests

# [4] build python
# - refer to dockerfile for what is already built
echo "\n::: [4] ... build python :::\n"
cd /root/mosaic/python && pip install .