#!/bin/bash

# [1] Build the image under 'docker-build':
# `GDAL_VERSION=3.4.1  LIBPROJ_VERSION=7.1.0 SPARK_VERSION=3.4.1 CORES=4 ./build`
# - produces image 'ubuntu22-gdal3.4.1-spark3.4.1' [default is JDK 8]
# [2] run this in root of (mosaic repo), e.g. `sh scripts/docker/spatial-docker.sh`
# - for IDE driven or Jupyter notebook testing
# [3] if you want to run tests within the container shell
# - [a] might need to `unset JAVA_TOOL_OPTIONS` is needed to execute JVM tests (if using `mosaic-docker-java-tool-options.sh`)
# - [b] then can test e.g. `mvn -X test -DskipTests=false -Dsuites=com.databricks.labs.mosaic.core.raster.TestRasterGDAL`
#       and `python3 -m unittest mosaic test/test_fuse_install.py` from ./python dir;
#       can also test scalastyle with `mvn org.scalastyle:scalastyle-maven-plugin:1.0.0:check`
# - [c] you may need to run `mvn clean` occasionally, especially around initial setup as intellij is JDK 11
#       and docker is JDK 8.
# ... don't need to specify -PskipCoverage (see settings.xml)
# [4] get shell with, e.g. `docker exec -it mosaic-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && cd /root/mosaic && /bin/bash"`,
# - can have multiple shells going; call `sh scripts/docker/exec-shell.sh` also
# [5] `docker stop mosaic-dev` whenever done to terminate the container
# NOTE: Ignore 'ERRO[0000] error waiting for container: context canceled'; also had to update Docker Desktop to 4.32
#       to address an issue that came up with update to MacOS Sonoma 14.5
docker run -q --privileged --platform linux/amd64 --name mosaic-dev -p 8888:8888 -v $PWD:/root/mosaic \
-itd --rm mosaic-dev:ubuntu22-gdal3.4.1-spark3.4.1 /bin/bash
docker exec -it mosaic-dev /bin/bash -c "sh /root/mosaic/scripts/docker/mosaic-dev/docker_init.sh"
docker exec -it mosaic-dev /bin/bash -c "cd /root/mosaic && /bin/bash"