docker run -q --privileged --name mosaic-dev -p 5005:5005 -p 8888:8888 \
-v $PWD:/root/mosaic -e JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n" \
-itd mosaic-dev /bin/bash