#!/bin/bash

docker run -q --privileged --platform linux/amd64 --name xpl-dev -p 5005:5005 -p 8888:8888 -p 4040:4040 \
-v $PWD:/root/mosaic -e JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n" \
-itd xpl-dev:ubuntu24-gdal311-spark35 /bin/bash
docker exec -it xpl-dev /bin/bash -c "sh /root/mosaic/scripts/docker/docker_init.sh"
docker exec -it xpl-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && cd /root/mosaic && /bin/bash"