#!/bin/bash

docker run -q --privileged --platform linux/amd64 --name mosaic-dev -p 5005:5005 -p 8888:8888 \
-v $PWD:/root/mosaic -e JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n" \
-itd --rm mosaic-dev:ubuntu22-gdal3.4.1-spark3.4.1 /bin/bash
docker exec -it mosaic-dev /bin/bash -c "sh /root/mosaic/scripts/docker/mosaic-dev/docker_init.sh"
docker exec -it mosaic-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && cd /root/mosaic && /bin/bash"