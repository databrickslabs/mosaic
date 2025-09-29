#!/bin/bash
# assumes you are running from the project root
# e.g. 'sh scripts/docker/start_docker.sh'
docker run --platform linux/amd64 --name geobrix-dev -p 5005:5005 -p 8888:8888 -p 4040:4040 \
-v $PWD:/root/geobrix -e JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n" \
-itd geobrix-dev:ubuntu24-gdal311-spark /bin/bash