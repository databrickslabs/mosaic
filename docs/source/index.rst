.. Mosaic documentation master file, created by
   sphinx-quickstart on Wed Feb  2 11:01:42 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: images/mosaic_logo.png
   :width: 50%
   :alt: mosaic
   :align: left

Mosaic is an extension to the `Apache Spark <https://spark.apache.org/>`__ framework that allows easy and fast processing of very large geospatial datasets.

Mosaic provides:
   - easy conversion between common spatial data encodings (WKT, WKB and GeoJSON);
   - constructors to easily generate new geometries from Spark native data types;
   - many of the OGC SQL standard `ST_` functions implemented as Spark Expressions for transforming, aggregating and joining spatial datasets;
   - high performance through implementation of Spark code generation within the core Mosaic functions;
   - optimisations for performing point-in-polygon joins using an approach we co-developed with Ordnance Survey (`blog post <https://databricks.com/blog/2021/10/11/efficient-point-in-polygon-joins-via-pyspark-and-bng-geospatial-indexing.html>`__); and 
   - the choice of a Scala, SQL and Python API.


Documentation
=============

.. toctree::
   :caption: Contents:

   usage/installation
   usage/quickstart
   usage/grid-indexes
   usage/automatic-sql-registration
   usage/kepler
   api/geometry-constructors
   api/geometry-accessors
   api/spatial-functions
   api/spatial-predicates
   api/spatial-aggregations


Indices and tables
==================

* :ref:`genindex`
* :ref:`search`


.. * :ref:`modindex`


Project Support
===============

Please note that all projects in the ``databrickslabs`` github space are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.
