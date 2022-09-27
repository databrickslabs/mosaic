==================
Installation guide
==================

Supported platforms
###################
In order to use Mosaic, you must have access to a Databricks cluster running
Databricks Runtime 10.0 or higher (11.2 with photon or later is recommended).
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

    >>> from mosaic import enable_mosaic
    >>> enable_mosaic(spark, dbutils)

   .. code-tab:: scala

    >>> import com.databricks.labs.mosaic.functions.MosaicContext
    >>> import com.databricks.labs.mosaic.H3
    >>> import com.databricks.labs.mosaic.ESRI
    >>>
    >>> val mosaicContext = MosaicContext.build(H3, ESRI)
    >>> import mosaicContext.functions._

   .. code-tab:: r R

    >>> library(sparkrMosaic)
    >>> enableMosaic()


SQL usage
*********
If you have not employed :ref:`Automatic SQL registration`, you will need to
register the Mosaic SQL functions in your SparkSession from a Scala notebook cell:

.. code-block:: scala

    import com.databricks.labs.mosaic.functions.MosaicContext
    import com.databricks.labs.mosaic.H3
    import com.databricks.labs.mosaic.ESRI

    val mosaicContext = MosaicContext.build(H3, ESRI)
    mosaicContext.register(spark)

