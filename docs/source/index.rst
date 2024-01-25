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

We currently recommend using Databricks Runtime with Photon enabled;
this will leverage the Databricks H3 expressions when using H3 grid system. 

Mosaic provides:
   * easy conversion between common spatial data encodings (WKT, WKB and GeoJSON);
   * constructors to easily generate new geometries from Spark native data types;
   * many of the OGC SQL standard :code:`ST_` functions implemented as Spark Expressions for transforming, aggregating and joining spatial datasets;
   * high performance through implementation of Spark code generation within the core Mosaic functions;
   * optimisations for performing point-in-polygon joins using an approach we co-developed with Ordnance Survey (`blog post <https://databricks.com/blog/2021/10/11/efficient-point-in-polygon-joins-via-pyspark-and-bng-geospatial-indexing.html>`_); and
   * the choice of a Scala, SQL and Python API.

.. note::
   For Mosaic versions < 0.4 please use the `0.3 docs <https://databrickslabs.github.io/mosaic/v0.3.x/index.html>`_.


Version 0.4.x Series
====================

We recommend using Databricks Runtime versions 13.3 LTS with Photon enabled.


Mosaic 0.4.x series only supports DBR 13.x DBRs. If running on a different DBR it will throw an exception:

**DEPRECATION ERROR: Mosaic v0.4.x series only supports Databricks Runtime 13. You can specify `%pip install 'databricks-mosaic<0.4,>=0.3'` for DBR < 13.**

Mosaic 0.4.x series issues the following ERROR on a standard, non-Photon cluster `ADB <https://learn.microsoft.com/en-us/azure/databricks/runtime/>`_ | `AWS <https://docs.databricks.com/runtime/index.html/>`_ | `GCP <https://docs.gcp.databricks.com/runtime/index.html/>`_ :

**DEPRECATION ERROR: Please use a Databricks Photon-enabled Runtime for performance benefits or Runtime ML for spatial AI benefits; Mosaic 0.4.x series restricts executing this cluster.**

As of Mosaic 0.4.0 (subject to change in follow-on releases)
   * No Mosaic SQL expressions cannot yet be registered with `Unity Catalog <https://www.databricks.com/product/unity-catalog>`_ due to API changes affecting DBRs >= 13.
   * `Assigned Clusters <https://docs.databricks.com/en/compute/configure.html#access-modes>`_ : Mosaic Python, R, and Scala APIs.
   * `Shared Access Clusters <https://docs.databricks.com/en/compute/configure.html#access-modes>`_ : Mosaic Scala API (JVM) with Admin `allowlisting <https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/allowlist.html>`_ ; Python bindings to Mosaic Scala APIs are blocked by Py4J Security on Shared Access Clusters.

.. note::
   As of Mosaic 0.4.0 (subject to change in follow-on releases)

   * `Unity Catalog <https://www.databricks.com/product/unity-catalog>`_ : Enforces process isolation which is difficult to accomplish with custom JVM libraries; as such only built-in (aka platform provided) JVM APIs can be invoked from other supported languages in Shared Access Clusters.
   * `Volumes <https://docs.databricks.com/en/connect/unity-catalog/volumes.html>`_ : Along the same principle of isolation, clusters (both assigned and shared access) can read Volumes via relevant built-in readers and writers or via custom python calls which do not involve any custom JVM code.


Version 0.3.x Series
====================

We recommend using Databricks Runtime versions 12.2 LTS with Photon enabled.
For Mosaic versions < 0.4.0 please use the `0.3.x docs <https://databrickslabs.github.io/mosaic/v0.3.x/index.html>`_.

.. warning::
   Mosaic 0.3.x series does not support DBR 13.x DBRs.

As of the 0.3.11 release, Mosaic issues the following WARNING when initialized on a cluster that is neither Photon Runtime nor Databricks Runtime ML `ADB <https://learn.microsoft.com/en-us/azure/databricks/runtime/>`_ | `AWS <https://docs.databricks.com/runtime/index.html/>`_ | `GCP <https://docs.gcp.databricks.com/runtime/index.html/>`_ :

**DEPRECATION WARNING: Please use a Databricks Photon-enabled Runtime for performance benefits or Runtime ML for spatial AI benefits; Mosaic will stop working on this cluster after v0.3.x.**

If you are receiving this warning in v0.3.11+, you will want to begin to plan for a supported runtime. The reason we are making this change is that we are streamlining Mosaic internals to be more aligned with future product APIs which are powered by Photon. Along this direction of change, Mosaic has standardized to JTS as its default and supported Vector Geometry Provider.


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
   v0.3.x/index


Indices and tables
==================

* :ref:`genindex`
* :ref:`search`


.. * :ref:`modindex`


Project Support
===============

Please note that all projects in the ``databrickslabs`` github space are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.
