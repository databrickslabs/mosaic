docker run --name rasterx-dev -p 5005:5005 -p 8888:8888 -p 4040:4040 \
-v $PWD:/root/mosaic -e JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n" \
-itd rasterx-dev:ubuntu24-gdal311-spark4 /bin/bash