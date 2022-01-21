# Databricks
![mosaic-logo](src/main/resources/mosaic_logo.png)

An extension to the [Apache Spark](https://spark.apache.org/) framework that allows easy and fast processing of very large geospatial datasets.

Mosaic provides:
- easy conversion between common spatial data encodings (WKT, WKB and GeoJSON);
- constructors to easily generate new geometries from Spark native data types;
- many of the OGC SQL standard `ST_` functions as Spark Expressions for transforming, aggregating and joining spatial datasets;
- high performance through implementation of Spark code generation within the core Mosaic functions;
- optimisations for performing point-in-polygon joins using an approach we co-developed with Ordnance Survey ([blog post](https://databricks.com/blog/2021/10/11/efficient-point-in-polygon-joins-via-pyspark-and-bng-geospatial-indexing.html)); and 
- the choice of a Scala, SQL and Python API. 

## Getting started

### Requirements
The only requirement to start using Mosaic is a Databricks cluster running Databricks Runtime 9.1 LTS with either of the following attached:
- (for Python API users) the Python .whl file (here); or
- (for Scala or SQL users) the Scala JAR (here).

Instructions for how to attach libraries to a Databricks cluster can be found [here](https://docs.databricks.com/libraries/cluster-libraries.html).

### Example notebooks
This repository contains several example notebooks in `notebooks/examples`. You can import them into your Databricks workspace using the instructions [here](https://docs.databricks.com/notebooks/notebooks-manage.html#import-a-notebook).