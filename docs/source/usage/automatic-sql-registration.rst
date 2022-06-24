==========================
Automatic SQL registration
==========================

If you are looking to use only the SQL functions exposed by Mosaic, without the need
to execute any Python or Scala set-up code, this can be achieved through the automatic SQL
registration process described on this page.

An example of when this might be useful would be connecting a business intelligence tool
to your Spark / Databricks cluster to perform spatial queries or integrating Spark
with a geospatial middleware component such as [Geoserver](https://geoserver.org/).

Pre-requisites
**************

In order to use Mosaic, you must have access to a Databricks cluster running
Databricks Runtime 10.0 or later. If you have cluster creation permissions in your Databricks
workspace, you can create a cluster using the instructions
`here <https://docs.databricks.com/clusters/create.html#use-the-cluster-ui>`__.

You will also need "Can Manage" permissions on this cluster in order to attach init script
to your cluster. A workspace administrator will be able to grant
these permissions and more information about cluster permissions can be found 
in our documentation
`here <https://docs.databricks.com/security/access-control/cluster-acl.html#cluster-level-permissions>`__.

Installation
************

To install Mosaic on your Databricks cluster, take the following steps:

#. Upload Mosaic jar to a dedicated dbfs location. E.g. dbfs:/FileStore/mosaic/jars/.
#. Create an init script that fetches the mosaic jar and copies it to databricks/jars.

    .. code-block:: bash

        %sh

        # Create init script directory for Mosaic
        mkdir -p /dbfs/FileStore/mosaic/scripts

        # Create init script
        cat > /dbfs/FileStore/mosaic/scripts/mosaic-init.sh <<'EOF'
        #!/bin/bash
        #
        # File: mosaic-init.sh
        # On cluster startup, this script will copy the Mosaic jars to the cluster's default jar directory.

        cp /dbfs/FileStore/mosaic/jars/*.jar /databricks/jars

        EOF

#. Configure the init script for the cluster following the instructions `here <https://docs.databricks.com/clusters/init-scripts.html#configure-a-cluster-scoped-init-script>`__.
#. Add the following spark configuration values for your cluster following the instructions `here <https://docs.databricks.com/clusters/configure.html#spark-configuration>`__.

    .. code-block:: bash

        # H3 or BNG
        spark.databricks.labs.mosaic.index.system H3
        # JTS or ESRI
        spark.databricks.labs.mosaic.geometry.api JTS
        # MosaicSQL or MosaicSQLDefault, MosaicSQLDefault corresponds to (H3, ESRI)
        spark.sql.extensions com.databricks.labs.mosaic.sql.extensions.MosaicSQL

Testing
*******

To test the installation, create a new Python notebook and run the following command:

.. code-block:: python

    spark.sql("""show functions""").where("startswith(function, 'st_')").display()

You should see all the supported functions registered by Mosaic appear in the output.
