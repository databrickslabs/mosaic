# [1] checkout https://github.com/r3stl355/mosaic-docker
# - you need to first build the image (under mosaic-docker repo) for Mosaic 0.4 series:
# `GDAL_VERSION=3.4.1  LIBPROJ_VERSION=7.1.0 SPARK_VERSION=3.4.0 CORES=4 ./build`
# - produces image 'ubuntu22-gdal3.4.1-spark3.4.0' [default is JDK 8]
# [2] run this in root of (mosaic repo), e.g. `sh scripts/mosaic-docker.sh`
# - for IDE driven or Jupyter notebook testing
# [3] if you want to run tests within the container shell
# - [a] after launch type `unset JAVA_TOOL_OPTIONS` from the container
# - [b] then can test e.g. `mvn -X test -DskipTests=false -Dsuites=com.databricks.labs.mosaic.core.raster.TestRasterGDAL` and
#       `python3 -m unittest mosaic test/test_fuse_install.py` from python
# - [c] you may need to run `mvn clean` occasionally, especially around initial setup as intellij is JDK 11
#       and docker is JDK 8.
# ... don't need to specify -PskipCoverage (see settings.xml)
docker run --platform linux/amd64 --name mosaic-dev --rm -p 5005:5005 -p 8888:8888 \
-v $PWD:/root/mosaic -e JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n" \
-it mosaic-dev:ubuntu22-gdal3.4.1-spark3.4.0 /bin/bash -c \
"/root/mosaic/scripts/m2/mvn_init.sh && cd /root/mosaic && /bin/bash"
