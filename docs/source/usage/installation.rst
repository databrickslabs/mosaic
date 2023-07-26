==================
Installation guide
==================

Supported platforms
###################

.. warning::
    From version 0.4.0, Mosaic will require either
     * Databricks Runtime 11.2+ with Photon enabled
     * Databricks Runtime for ML 11.2+
    
    Mosaic 0.3 series does not support DBR 13 (coming soon with Mosaic 0.4 series); 
    also, DBR 10 is no longer supported in Mosaic. 

We recommend using Databricks Runtime versions 11.3 LTS or 12.2 LTS with Photon enabled; 
this will leverage the Databricks H3 expressions when using H3 grid system.  
As of the 0.3.11 release, Mosaic issues the following warning when initialized on a cluster
that is neither Photon Runtime nor Databricks Runtime ML [`ADB <https://learn.microsoft.com/en-us/azure/databricks/runtime/>`__ | `AWS <https://docs.databricks.com/runtime/index.html>`__ | `GCP <https://docs.gcp.databricks.com/runtime/index.html>`__]:

    DEPRECATION WARNING: Mosaic is not supported on the selected Databricks Runtime. Mosaic will stop working on this cluster from version v0.4.0+. Please use a Databricks Photon-enabled Runtime (for performance benefits) or Runtime ML (for spatial AI benefits).

If you are receiving this warning in v0.3.11+, you will want to change to a supported runtime prior 
to updating Mosaic to run 0.4.0. The reason we are making this change is that we are streamlining Mosaic
internals to be more aligned with future product APIs which are powered by Photon. Along this direction 
of change, Mosaic will be standardizing to JTS as its default and supported Vector Geometry Provider.

If you have cluster creation permissions in your Databricks
workspace, you can create a cluster using the instructions
`here <https://docs.databricks.com/clusters/create.html#use-the-cluster-ui>`__.

You will also need "Can Manage" permissions on this cluster in order to attach the
Mosaic library to your cluster. A workspace administrator will be able to grant 
these permissions and more information about cluster permissions can be found 
in our documentation
`here <https://docs.databricks.com/security/access-control/cluster-acl.html#cluster-level-permissions>`__.

Package installation
####################

Installation from PyPI
**********************
Python users can install the library directly from `PyPI <https://pypi.org/project/databricks-mosaic/>`__
using the instructions `here <https://docs.databricks.com/libraries/cluster-libraries.html>`__
or from within a Databricks notebook using the :code:`%pip` magic command, e.g.

.. code-block:: bash

    %pip install databricks-mosaic

Installation from release artifacts
***********************************
Alternatively, you can access the latest release artifacts `here <https://github.com/databrickslabs/mosaic/releases>`__
and manually attach the appropriate library to your cluster.

Which artifact you choose to attach will depend on the language API you intend to use.

* For Python API users, choose the Python .whl file.
* For Scala users, take the Scala JAR (packaged with all necessary dependencies).
* For R users, download the Scala JAR and the R bindings library [see the sparkR readme](R/sparkR-mosaic/README.md).

Instructions for how to attach libraries to a Databricks cluster can be found `here <https://docs.databricks.com/libraries/cluster-libraries.html>`__.

Automated SQL registration
**************************
If you would like to use Mosaic's functions in pure SQL (in a SQL notebook, from a business intelligence tool,
or via a middleware layer such as Geoserver, perhaps) then you can configure
"Automatic SQL Registration" using the instructions `here <https://databrickslabs.github.io/mosaic/usage/automatic-sql-registration.html>`__.

Enabling the Mosaic functions
#############################
The mechanism for enabling the Mosaic functions varies by language:

.. tabs::
   .. code-tab:: py

    from mosaic import enable_mosaic
    enable_mosaic(spark, dbutils)

   .. code-tab:: scala

    import com.databricks.labs.mosaic.functions.MosaicContext
    import com.databricks.labs.mosaic.H3
    import com.databricks.labs.mosaic.JTS

    val mosaicContext = MosaicContext.build(H3, JTS)
    import mosaicContext.functions._

   .. code-tab:: r R

    library(sparkrMosaic)
    enableMosaic()


SQL usage
*********
If you have not employed :ref:`Automatic SQL registration`, you will need to
register the Mosaic SQL functions in your SparkSession from a Scala notebook cell:

.. code-block:: scala

    import com.databricks.labs.mosaic.functions.MosaicContext
    import com.databricks.labs.mosaic.H3
    import com.databricks.labs.mosaic.JTS

    val mosaicContext = MosaicContext.build(H3, JTS)
    mosaicContext.register(spark)

