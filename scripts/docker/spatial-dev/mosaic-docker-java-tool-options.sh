#!/bin/bash

docker run -q --privileged --platform linux/amd64 --name spatial-dev -p 5005:5005 -p 8888:8888 \
-v $PWD:/root/mosaic -e JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n" \
-itd --rm spatial-dev:ubuntu22-gdal3.4.1-spark3.5.0 /bin/bash
docker exec -it spatial-dev /bin/bash -c "sh /root/mosaic/scripts/docker/spatial-dev/docker_init.sh"
docker exec -it spatial-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && cd /root/mosaic && /bin/bash"