# Databricks
![mosaic-logo](../src/main/resources/mosaic_logo.png)

An extension to the [Apache Spark](https://spark.apache.org/) framework that allows easy and fast processing of very large geospatial datasets.

Mosaic provides:
- easy conversion between common spatial data encodings (WKT, WKB and GeoJSON);
- constructors to easily generate new geometries from Spark native data types;
- many of the OGC SQL standard `ST_` functions implemented as Spark Expressions for transforming, aggregating and joining spatial datasets;
- high performance through implementation of Spark code generation within the core Mosaic functions;
- optimisations for performing point-in-polygon joins using an approach we co-developed with Ordnance Survey ([blog post](https://databricks.com/blog/2021/10/11/efficient-point-in-polygon-joins-via-pyspark-and-bng-geospatial-indexing.html)); and 
- the choice of a Scala, SQL and Python API. 

## Getting started

### Requirements
The only requirement to start using Mosaic is a Databricks cluster running Databricks Runtime 10.0 (or later) with either of the following attached:
- (for Python API users) the Python .whl file; or
- (for Scala or SQL users) the Scala JAR.

Both the .whl and JAR can be found in the 'Releases' section of the Mosaic GitHub repository.

Instructions for how to attach libraries to a Databricks cluster can be found [here](https://docs.databricks.com/libraries/cluster-libraries.html).

### Example notebooks
This repository contains several example notebooks in `notebooks/examples`. You can import them into your Databricks workspace using the instructions [here](https://docs.databricks.com/notebooks/notebooks-manage.html#import-a-notebook).

### Project Support
Please note that all projects in the `databrickslabs` github space are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.