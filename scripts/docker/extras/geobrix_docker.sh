#!/bin/bash
# [1] Build the image with 'build_mac.sh':
# [2] Assumes you are executing this from project root, e.g. 'sh scripts/docker/mac/geobrix_docker_mac.sh'
# [3] if you want to run tests within the container shell
# - [a] `unset JAVA_TOOL_OPTIONS` is needed to execute JVM tests
# - [b] then can test e.g. `mvn -X test -DskipTests=false -Dsuites=com.databricks.labs.gbx.ds.WhitelistDSTest`
#       and `pytest` from ./python/geobrix dir (you can build with `python3 -m build`)
# - [c] you may need to run `mvn clean` occasionally
# ... don't need to specify -PskipCoverage (see settings.xml)
# [4] `docker stop geobrix-dev` whenever done to terminate the container
# NOTES:
#  * Ignore 'ERRO[0000] error waiting for container: context canceled'
#  * Removed --privileged
docker run -q --platform linux/amd64 --name geobrix-dev -p 5005:5005 -p 8888:8888 -p 4040:4040 \
-v $PWD:/root/geobrix -e JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n" \
-itd --rm geobrix-dev:ubuntu24-gdal311-spark /bin/bash

docker exec -it geobrix-dev /bin/bash -c "sh /root/geobrix/scripts/docker/extras/docker_init.sh"
docker exec -it geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && cd /root/geobrix && /bin/bash"