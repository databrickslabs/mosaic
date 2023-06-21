.. Mosaic documentation master file, created by
   sphinx-quickstart on Wed Feb  2 11:01:42 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: images/mosaic_logo.png
   :alt: mosaic
   :class: mosaic-logo


.. container:: package_health

    .. image:: https://badge.fury.io/py/databricks-mosaic.svg
       :target: https://badge.fury.io/py/databricks-mosaic
       :alt: PyPI Version

    .. image:: https://img.shields.io/pypi/dm/databricks-mosaic?style=plastic
       :alt: PyPI Monthly Downloads

    .. image:: https://codecov.io/gh/databrickslabs/mosaic/branch/main/graph/badge.svg?token=aEzZ8ITxdg
       :target: https://codecov.io/gh/databrickslabs/mosaic
       :alt: Codecov

    .. image:: https://github.com/databrickslabs/mosaic/actions/workflows/build_main.yml/badge.svg
       :target: https://github.com/databrickslabs/mosaic/actions?query=workflow%3A%22build+main%22
       :alt: build

    .. image:: https://github.com/databrickslabs/mosaic/actions/workflows/docs.yml/badge.svg
       :target: https://github.com/databrickslabs/mosaic/actions/workflows/docs.yml
       :alt: Mosaic sphinx docs

    .. image:: https://img.shields.io/lgtm/grade/python/g/databrickslabs/mosaic.svg?logo=lgtm&logoWidth=18
       :target: https://lgtm.com/projects/g/databrickslabs/mosaic/context:python
       :alt: Language grade: Python

    .. image:: https://img.shields.io/badge/code%20style-black-000000.svg
       :target: https://github.com/psf/black
       :alt: Code style: black



Mosaic is an extension to the `Apache Spark <https://spark.apache.org/>`_ framework that allows easy and fast processing of very large geospatial datasets.

.. warning::
    From version 0.4.0, Mosaic will require either
     * Databricks Runtime 11.2+ with Photon enabled
     * Databricks Runtime for ML 11.2+
    
    Mosaic 0.3 series does not support DBR 13 (coming soon with Mosaic 0.4 series); 
    also, DBR 10 is no longer supported in Mosaic. 

We recommend using Databricks Runtime versions 11.3 LTS or 12.2 LTS with Photon enabled; 
this will leverage the Databricks H3 expressions when using H3 grid system. 

Mosaic provides:
   * easy conversion between common spatial data encodings (WKT, WKB and GeoJSON);
   * constructors to easily generate new geometries from Spark native data types;
   * many of the OGC SQL standard `ST_` functions implemented as Spark Expressions for transforming, aggregating and joining spatial datasets;
   * high performance through implementation of Spark code generation within the core Mosaic functions;
   * optimisations for performing point-in-polygon joins using an approach we co-developed with Ordnance Survey (`blog post <https://databricks.com/blog/2021/10/11/efficient-point-in-polygon-joins-via-pyspark-and-bng-geospatial-indexing.html>`_); and
   * the choice of a Scala, SQL and Python API.



Documentation
=============

.. toctree::
   :glob:
   :titlesonly:
   :maxdepth: 2
   :caption: Contents:

   api/api
   usage/usage
   models/models
   literature/videos


Indices and tables
==================

* :ref:`genindex`
* :ref:`search`


.. * :ref:`modindex`


Project Support
===============

Please note that all projects in the ``databrickslabs`` github space are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.
