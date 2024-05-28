# Docker Build

> This is adapted from [Mosaic-Docker](https://github.com/r3stl355/mosaic-docker) repo, focused on DBR 13.3 LTS which is Ubuntu 22.04.
> It is needed when you want to build and run tests on non Ubuntu Jammy machines, e.g. MacOS.

## Steps

1. Cmd `GDAL_VERSION=3.4.1 LIBPROJ_VERSION=7.1.0 SPARK_VERSION=3.4.1 CORES=4 ./build` 
   builds the docker image for DBR 13.3 LTS. Name will be 'mosaic-dev:ubuntu22-gdal3.4.1-spark3.4.1'.
2. Cmd `sh scripts/docker/mosaic-docker.sh` to run. That script launches a container and further (optionally) configures.

## Additional Notes

* Image is configured to JDK 8 to match DBR 13; python 3.10 as well
* Support IDE driven or Jupyter notebook testing in addition to straight shell, 
  see more at [Mosaic-Docker](https://github.com/r3stl355/mosaic-docker). Recommend placing any test notebooks
  in '<project_root>/python/notebooks' which is already added to .gitignore
* If you want to run tests within a container shell:
  - `unset JAVA_TOOL_OPTIONS` is needed to execute JVM tests
  - then can test e.g. `mvn -X test -DskipTests=false -Dsuites=com.databricks.labs.mosaic.core.raster.TestRasterGDAL`
    and `python3 -m unittest mosaic test/test_fuse_install.py` from ./python dir
  - you may need to run `mvn clean` occasionally, especially around initial setup as intellij is JDK 11 (pom.xml)
    and docker is JDK 8
  - you don't need to specify -PskipCoverage (see 'm2/settings.xml' and pom.xml)
* Get shell with `docker exec -it mosaic-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && cd /root/mosaic && /bin/bash"`,
  can have multiple shells going; call `sh scripts/docker/exec-shell.sh` also
* `docker stop mosaic-dev` whenever done to terminate the container
* NOTE: Ignore 'ERRO[0000] error waiting for container: context canceled' if you get this on MacOS