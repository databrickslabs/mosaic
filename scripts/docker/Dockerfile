FROM --platform=linux/amd64 ubuntu:24.04

# refresh package info
RUN apt-get update -y

# Add Azul Zulu repository
RUN apt-get update -y && apt-get install -y wget gnupg && \
    wget -qO - https://repos.azul.com/azul-repo.key | apt-key add - && \
    echo "deb https://repos.azul.com/zulu/deb stable main" | tee /etc/apt/sources.list.d/zulu.list && \
    apt-get update -y

# Install Zulu OpenJDK 17
RUN apt-get install -y zulu17-jdk --no-install-recommends

# Install native dependencies
RUN apt-get install -y python3-numpy unixodbc libcurl3-gnutls libsnappy-dev libopenjp2-7

ENV JAVA_HOME /usr/lib/jvm/zulu17

# Install dependencies
RUN set -ex \
    && deps=" \
       python3-dev \
       python3-numpy \
       python3-pip \
       python3-venv \
       bash-completion \
       libspatialite-dev \
       libpq-dev \
       libcurl4-gnutls-dev \
       libxml2-dev \
       libgeos-dev \
       libnetcdf-dev \
       libpoppler-dev \
       libhdf4-alt-dev \
       libhdf5-serial-dev \
       libpoppler-private-dev \
       sqlite3 \
       libsqlite3-dev \
       libtiff-dev \
       wget \
       curl \
    " \
    && buildDeps=" \
       build-essential \
       autoconf \
       automake \
       libtool \
       curl \
       libwebp-dev \
       cmake \
       swig \
       ant \
       pkg-config \
    "\
    && apt-get update -y && apt-get install -y $buildDeps $deps --no-install-recommends

# Install the remaining components
ENV ROOTDIR /usr/local
ENV LD_LIBRARY_PATH /usr/local/lib
ENV HADOOP_HOME /usr/local/hadoop
ENV SPARK_VERSION 4.0.0
ENV GDAL_VERSION 3.11.3
ENV LIBPROJ_VERSION 9.3.1
ENV HADOOP_VERSION 3.4.0
ENV CORES 8

WORKDIR $ROOTDIR/
RUN mkdir -p $ROOTDIR/src

# Install Hadoop
RUN wget -qO- https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | \
    tar -xzC $ROOTDIR/src/ && \
    mv $ROOTDIR/src/hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    ln -s $HADOOP_HOME/bin/hadoop /usr/local/bin/hadoop && \
    ln -s $HADOOP_HOME/bin/hdfs /usr/local/bin/hdfs && \
    ln -s $HADOOP_HOME/bin/yarn /usr/local/bin/yarn && \
    ln -s $HADOOP_HOME/bin/mapred /usr/local/bin/mapred
# Configure Hadoop environment
RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Install PROJ
RUN apt-get install proj-bin

# Install GDAL
RUN wget -qO- https://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz | \
    tar -xzC $ROOTDIR/src/

RUN cd src/gdal-${GDAL_VERSION} \
    && mkdir -p build \
    && cd build \
    && cmake .. -DGDAL_USE_JAVA=ON \
               -DJAVA_HOME=$JAVA_HOME \
               -DCMAKE_BUILD_TYPE=Release \
               -DCMAKE_INSTALL_PREFIX=/usr/local \
               -DCMAKE_C_FLAGS="-mtune=generic" \
               -DCMAKE_CXX_FLAGS="-mtune=generic" \
    && make -j${CORES} \
    && make install \
    && ldconfig


# Copy binaries to the location expected to be by the project
RUN ln -s $ROOTDIR/lib/libgdal.so /usr/lib/libgdal.so
RUN ln -s $ROOTDIR/lib/libgdal.so.30 /usr/lib/libgdal.so.30
RUN ln -s $ROOTDIR/lib/libgdal.so.30.0.3 /usr/lib/libgdal.so.30.0.3
RUN ln -s $ROOTDIR/lib/jni/libgdalalljni.so /usr/lib/libgdalalljni.so
RUN mkdir -p /usr/lib/jni && ln -s $ROOTDIR/lib/jni/libgdalalljni.so /usr/lib/jni/libgdalalljni.so.30
RUN mkdir -p /usr/lib/ogdi && ln -s $ROOTDIR/lib/libgdal.so /usr/lib/ogdi/libgdal.so

# Add Maven
ARG MAVEN_VERSION=3.9.11
ARG USER_HOME_DIR="/root"
ARG BASE_URL=https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries
ARG SHA=bcfe4fe305c962ace56ac7b5fc7a08b87d5abd8b7e89027ab251069faebee516b0ded8961445d6d91ec1985dfe30f8153268843c89aa392733d1a3ec956c9978

RUN mkdir -p $ROOTDIR/share/maven $ROOTDIR/share/maven/ref \
  && echo "Downloading maven" \
  && curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  \
  && echo "Checking download hash" \
  && echo "${SHA}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  \
  && echo "Unzipping maven" \
  && tar -xzf /tmp/apache-maven.tar.gz -C $ROOTDIR/share/maven --strip-components=1 \
  \
  && echo "Cleaning and setting links" \
  && rm -f /tmp/apache-maven.tar.gz \
  && ln -s $ROOTDIR/share/maven/bin/mvn $ROOTDIR/bin/mvn

ENV MAVEN_HOME $ROOTDIR/share/maven
ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"

# Python packages
# - Adds additional needed packages
RUN pip3 install --upgrade pip --ignore-installed --break-system-packages
RUN pip3 install build wheel keplergl ipython pyspark==$SPARK_VERSION --break-system-packages
RUN pip3 install black build isort py4j requests --break-system-packages
RUN pip3 install gdal==$GDAL_VERSION --break-system-packages
RUN pip3 install pytest --break-system-packages

# Clean up
RUN apt-get purge -y --auto-remove $buildDeps \
    && rm -rf /var/lib/apt/lists/*
