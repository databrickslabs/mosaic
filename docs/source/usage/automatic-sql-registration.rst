==========================
Automatic SQL registration
==========================

If you are looking to use only the SQL functions exposed by Mosaic, without the need
to execute any Python or Scala set-up code, this can be achieved through the automatic SQL
registration process described on this page.

An example of when this might be useful would be connecting a business intelligence tool
to your Spark / Databricks cluster to perform spatial queries or integrating Spark
with a geospatial middleware component such as [Geoserver](https://geoserver.org/).

.. warning::
    Mosaic 0.4.x SQL bindings for DBR 13 can register with Assigned clusters (as Spark Expressions), but not Shared Access due
    to `Unity Catalog <https://www.databricks.com/product/unity-catalog>`_ API changes, more `here <https://docs.databricks.com/en/udf/index.html>`_.

Pre-requisites
**************

In order to use Mosaic, you must have access to a Databricks cluster running
Databricks Runtime 13. If you have cluster creation permissions in your Databricks
workspace, you can create a cluster using the instructions
`here <https://docs.databricks.com/clusters/create.html#use-the-cluster-ui>`_.

You will also need "Can Manage" permissions on this cluster in order to attach init script
to your cluster. A workspace administrator will be able to grant
these permissions and more information about cluster permissions can be found 
in our documentation
`here <https://docs.databricks.com/security/access-control/cluster-acl.html#cluster-level-permissions>`_.

Installation
************

To install Mosaic on your Databricks cluster, take the following steps:

#. Upload Mosaic jar to a dedicated fuse mount location. E.g. "dbfs:/FileStore/mosaic/jars/".

#. Create an init script that fetches the mosaic jar and copies it to "databricks/jars".

    You can also use the output from (0.4 series) python function :code:`setup_fuse_install`, e.g.
    :code:`setup_fuse_intall(<to_fuse_dir>, jar_copy=True)` which can help to copy the JAR used in
    the init script below.

    .. code-block:: bash

        %sh

        # Create init script directory for Mosaic
        mkdir -p /dbfs/FileStore/mosaic/scripts

        # Create init script
        cat > /dbfs/FileStore/mosaic/scripts/mosaic-init.sh <<'EOF'
        #!/bin/bash
        #
        # File: mosaic-init.sh
        # On cluster startup, this script will copy the Mosaic JAR to the cluster's default jar directory.

        cp /dbfs/FileStore/mosaic/jars/*.jar /databricks/jars

        EOF

#. Configure the init script for the cluster following the instructions `here <https://docs.databricks.com/clusters/init-scripts.html#configure-a-cluster-scoped-init-script>`_.

#. Add the following spark configuration values for your cluster following the instructions `here <https://docs.databricks.com/clusters/configure.html#spark-configuration>`_.

    .. code-block:: bash

        # H3 or BNG
        spark.databricks.labs.mosaic.index.system H3
        # JTS only
        spark.databricks.labs.mosaic.geometry.api JTS
        # MosaicSQL or MosaicSQLDefault, MosaicSQLDefault corresponds to (H3, JTS)
        spark.sql.extensions com.databricks.labs.mosaic.sql.extensions.MosaicSQL

Testing
*******

To test the installation, create a new Python notebook and run the following commands (similar for :code:`grid_` and :code:`rst_`, not shown):

.. code-block:: py

    sql("""SHOW FUNCTIONS""").where("startswith(function, 'st_')").display()

You should see all the supported :code:`ST_` functions registered by Mosaic appear in the output.

.. figure:: ../images/functions_show.png
   :figclass: doc-figure

   Fig 1. Show Functions Example

.. note::
    You may see some :code:`ST_` functions from other libraries, so pay close attention to the provider;
    also, function auto-complete in the UI may not list custom registered SQL expressions.

.. code-block:: py

    sql("""DESCRIBE FUNCTION st_buffer""")

.. figure:: ../images/function_describe.png
   :figclass: doc-figure

   Fig 2. Describe Function Example

.. warning::
    Issue 317: https://github.com/databrickslabs/mosaic/issues/317
    Mosaic jar needs to be installed via init script and not through the cluster UI.
    Automatic SQL registration needs to happen at the cluster start up time when Spark context is created.
    Cluster UI installed libraries are made available too late and the Automatic SQL registration
    will not work, but there is no way to print an Error message in that case.

.. warning::
   Issue 297: https://github.com/databrickslabs/mosaic/issues/297
   Since Mosaic V0.3.6 Automatic SQL Registration can fail with the following error message:
   "java.lang.Exception: spark.databricks.labs.mosaic.raster.api". This is due to a missing key in the spark
   configuration. The issue has been fixed since Mosaic V0.3.10. For releases between V0.3.6 and V0.3.10
   please add the following configuration to your cluster spark configs: (spark.databricks.labs.mosaic.raster.api, "GDAL"),
   or alternatively in python/scala code: spark.conf.set("spark.databricks.labs.mosaic.raster.api", "GDAL")