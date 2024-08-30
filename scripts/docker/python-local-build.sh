#!/bin/bash

# run from within docker container
# run from the repo root ('mosaic') level

# [1] delete existing jars in python dir
rm python/mosaic/lib/*.jar

# [2] package
mvn package -DskipTests=true

# [3] build
cd python && python3 -m build