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

    .. image:: https://img.shields.io/badge/code%20style-black-000000.svg
       :target: https://github.com/psf/black
       :alt: Code style: black

| Mosaic is an extension to the `Apache Spark <https://spark.apache.org/>`_ framework for fast + easy processing
  of very large geospatial datasets. It provides:
|
|  [1] The choice of a Scala, SQL and Python language bindings (written in Scala).
|  [2] Raster and Vector APIs.
|  [3] Easy conversion between common spatial data encodings (WKT, WKB and GeoJSON).
|  [4] Constructors to easily generate new geometries from Spark native data types.
|  [5] Many of the OGC SQL standard :code:`ST_` functions implemented as Spark Expressions for transforming,
|      aggregating and joining spatial datasets.
|  [6] High performance through implementation of Spark code generation within the core Mosaic functions.
|  [7] Performing point-in-polygon joins using an approach we co-developed with Ordnance Survey
       (`blog post <https://databricks.com/blog/2021/10/11/efficient-point-in-polygon-joins-via-pyspark-and-bng-geospatial-indexing.html>`_).

.. note::
   We recommend using Databricks Runtime with Photon enabled to leverage the Databricks H3 expressions.

Version 0.4.x Series
====================

.. warning::
   For Mosaic <= 0.4.1 :code:`%pip install databricks-mosaic` will no longer install "as-is" in DBRs due to the fact that Mosaic
   left geopandas unpinned in those versions. With geopandas 0.14.4, numpy dependency conflicts with the limits of
   scikit-learn in DBRs. The workaround is :code:`%pip install geopandas==0.14.3 databricks-mosaic`.
   Mosaic 0.4.2+ limits the geopandas version.

Mosaic 0.4.x series only supports DBR 13.x DBRs. If running on a different DBR it will throw an exception:

   DEPRECATION ERROR: Mosaic v0.4.x series only supports Databricks Runtime 13.
   You can specify :code:`%pip install 'databricks-mosaic<0.4,>=0.3'` for DBR < 13.

Mosaic 0.4.x series issues an ERROR on standard, non-Photon clusters `ADB <https://learn.microsoft.com/en-us/azure/databricks/runtime/>`_ |
`AWS <https://docs.databricks.com/runtime/index.html/>`_ |
`GCP <https://docs.gcp.databricks.com/runtime/index.html/>`_:

   DEPRECATION ERROR: Please use a Databricks Photon-enabled Runtime for performance benefits or Runtime ML for
   spatial AI benefits; Mosaic 0.4.x series restricts executing this cluster.

As of Mosaic 0.4.0 / DBR 13.3 LTS (subject to change in follow-on releases):

* `Assigned Clusters <https://docs.databricks.com/en/compute/configure.html#access-modes>`_
   * Mosaic Python, SQL, R, and Scala APIs.
* `Shared Access Clusters <https://docs.databricks.com/en/compute/configure.html#access-modes>`_
   * Mosaic Scala API (JVM) with Admin `allowlisting <https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/allowlist.html>`_.
   * Mosaic Python bindings (to Mosaic Scala APIs) are blocked by Py4J Security on Shared Access Clusters.
   * Mosaic SQL expressions cannot yet be registered due to `Unity Catalog <https://www.databricks.com/product/unity-catalog>`_.
     API changes, more `here <https://docs.databricks.com/en/udf/index.html>`_.

.. note::
   Mosaic is a custom JVM library that extends spark, which has the following implications in DBR 13.3 LTS:

   * `Unity Catalog <https://www.databricks.com/product/unity-catalog>`_ enforces process isolation which is difficult
     to accomplish with custom JVM libraries; as such only built-in (aka platform provided) JVM APIs can be invoked from
     other supported languages in Shared Access Clusters.
   * Clusters can read `Volumes <https://docs.databricks.com/en/connect/unity-catalog/volumes.html>`_ via relevant
     built-in (aka platform provided) readers and writers or via custom python calls which do not involve any custom JVM code.


Version 0.3.x Series
====================

We recommend using Databricks Runtime versions 12.2 LTS with Photon enabled.
For Mosaic versions < 0.4.0 please use the `0.3.x docs <https://databrickslabs.github.io/mosaic/v0.3.x/index.html>`_.

As of the 0.3.11 release, Mosaic issues the following WARNING when initialized on a cluster that is neither Photon Runtime
nor Databricks Runtime ML `ADB <https://learn.microsoft.com/en-us/azure/databricks/runtime/>`_ |
`AWS <https://docs.databricks.com/runtime/index.html/>`_ |
`GCP <https://docs.gcp.databricks.com/runtime/index.html/>`_:

    DEPRECATION WARNING: Please use a Databricks Photon-enabled Runtime for performance benefits or Runtime ML for spatial
    AI benefits; Mosaic will stop working on this cluster after v0.3.x.

If you are receiving this warning in v0.3.11+, you will want to begin to plan for a supported runtime. The reason we are
making this change is that we are streamlining Mosaic internals to be more aligned with future product APIs which are
powered by Photon. Along this direction of change, Mosaic has standardized to JTS as its default and supported Vector
Geometry Provider.

.. note::
   For Mosaic versions < 0.4 please use the `0.3 docs <https://databrickslabs.github.io/mosaic/v0.3.x/index.html>`_.


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
