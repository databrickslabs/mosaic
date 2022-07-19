FROM databricksruntime/standard:10.4-LTS

ARG SPARK_VERSION=3.2.1

ENV VIRTUAL_ENV=/databricks/python3
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN apt update && apt-get install -y maven locales

# Ensure that we always use UTF-8 and with American English locale
RUN locale-gen en_US.UTF-8

ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en

RUN pip install --upgrade pip \
    && pip install -U setuptools \
    && pip install build wheel pyspark==$SPARK_VERSION

COPY pom.xml scalastyle-config.xml src /tmp/
COPY src/. /tmp/src/
WORKDIR /tmp
RUN mvn -DskipTests clean install
COPY python/. /tmp/python/
WORKDIR /tmp/python
RUN cp ../target/mosaic*jar-with-dependencies.jar ./mosaic/lib
RUN pip install .
ENTRYPOINT ["/databricks/python3/bin/python", "-m", "unittest"]