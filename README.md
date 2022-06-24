# Mosaic by Databricks Labs
![mosaic-logo](src/main/resources/mosaic_logo.png)

An extension to the [Apache Spark](https://spark.apache.org/) framework that allows easy and fast processing of very large geospatial datasets.

[![build](https://github.com/databrickslabs/mosaic/actions/workflows/build.yml/badge.svg)](https://github.com/databrickslabs/mosaic/actions/workflows/build.yml)
[![docs](https://github.com/databrickslabs/mosaic/actions/workflows/docs.yml/badge.svg)](https://github.com/databrickslabs/mosaic/actions/workflows/docs.yml)
[![codecov](https://codecov.io/gh/databrickslabs/mosaic/branch/main/graph/badge.svg?token=aEzZ8ITxdg)](https://codecov.io/gh/databrickslabs/mosaic)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/databrickslabs/mosaic.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/databrickslabs/mosaic/context:python)


Mosaic provides:
- easy conversion between common spatial data encodings (WKT, WKB and GeoJSON);
- constructors to easily generate new geometries from Spark native data types;
- many of the OGC SQL standard `ST_` functions implemented as Spark Expressions for transforming, aggregating and joining spatial datasets;
- high performance through implementation of Spark code generation within the core Mosaic functions;
- optimisations for performing point-in-polygon joins using an approach we co-developed with Ordnance Survey ([blog post](https://databricks.com/blog/2021/10/11/efficient-point-in-polygon-joins-via-pyspark-and-bng-geospatial-indexing.html)); and 
- the choice of a Scala, SQL and Python API.


  ![mosaic-logo](src/main/resources/MosaicLogicalDesign.png)
Image1: Mosaic logical design.

## Getting started

### Requirements
The only requirement to start using Mosaic is a Databricks cluster running Databricks Runtime 10.0 (or later) with either of the following attached:
- (for Python API users) the Python .whl file; or
- (for Scala or SQL users) the Scala JAR; or
- (for R users) the Scala JAR and the R library [see the sparkR readme](R/sparkR-mosaic/README.md).

The .whl, JAR, and R artefacts can be found in the 'Releases' section of the Mosaic GitHub repository.

Instructions for how to attach libraries to a Databricks cluster can be found [here](https://docs.databricks.com/libraries/cluster-libraries.html).

### Releases
You can access the latest artifacts and binaries [here](https://github.com/databrickslabs/mosaic/releases).

### Ecosystem
Mosaic is intended to augment the existing system and unlock the potential by integrating spark, delta and 3rd party frameworks into the Lakehouse architecture.

![mosaic-logo](src/main/resources/MosaicEcosystem.png)
Image2: Mosaic ecosystem - Lakehouse integration.

### Example notebooks
This repository contains several example notebooks in `notebooks/examples`. You can import them into your Databricks workspace using the instructions [here](https://docs.databricks.com/notebooks/notebooks-manage.html#import-a-notebook).

### Project Support
Please note that all projects in the `databrickslabs` github space are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.
