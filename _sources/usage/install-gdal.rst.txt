=======================
GDAL Installation guide
=======================

Supported platforms
###################
In order to use Mosaic 0.4 series, you must have access to a Databricks cluster running
Databricks Runtime 13.3 LTS.
If you have cluster creation permissions in your Databricks
workspace, you can create a cluster using the instructions
`here <https://docs.databricks.com/clusters/create.html#use-the-cluster-ui>`_.

You will also need "Can Manage" permissions on this cluster in order to attach the
Mosaic library to your cluster. A workspace administrator will be able to grant 
these permissions and more information about cluster permissions can be found 
in our documentation
`here <https://docs.databricks.com/security/access-control/cluster-acl.html#cluster-level-permissions>`_.

.. warning::
    These instructions assume an Assigned cluster is being used (vs a Shared Access cluster),
    more on access modes `here <https://docs.databricks.com/en/compute/configure.html#access-modes>`_.

GDAL Installation
####################

Setup GDAL files and scripts
****************************
Mosaic requires GDAL to be installed on the cluster. The easiest way to do this is to use the
the :code:`setup_gdal` function.

.. note::
   - This is close in behavior to Mosaic < 0.4 series (prior to DBR 13), with new options
     to pip install Mosaic for either ubuntugis gdal (3.4.3) or jammy default (3.4.1).
   - Param "to_fuse_dir" can be one of "/Volumes/..", "/Workspace/..", "/dbfs/..";
     however, you should consider :code:`setup_fuse_install()` for Volume based installs as that
     exposes more options, to include copying JAR and JNI Shared Objects.

.. function:: setup_gdal()

    Generate an init script that will install GDAL native libraries on each worker node.
    All of the listed parameters are optional. You can have even more control with :code:`setup_fuse_install` function.

    :param to_fuse_dir: Path to write out the init script for GDAL installation;
                        default is "/Workspace/Shared/geospatial/mosaic/gdal/jammy".
    :type to_fuse_dir: str
    :param with_mosaic_pip: Whether to configure a script that pip installs databricks-mosaic,
                            fixed to the current version; default is False.
    :type with_mosaic_pip: bool
    :param with_ubuntugis: Whether to use ubuntugis ppa for GDAL instead of built-in;
                           default is False.
    :type with_ubuntugis: bool
    :param script_out_name: name of the script to be written;
                            default is "mosaic-gdal-init.sh".
    :type script_out_name: str
    :param override_mosaic_version: String value to use to override the mosaic version to install,
                                    e.g. "==0.4.0" or "<0.5,>=0.4"; default is None.
    :type override_mosaic_version: str
    :rtype: bool

    :example:

.. code-block:: py

    import mosaic as mos

    mos.enable_mosaic(spark, dbutils)
    mos.setup_gdal()

    +-----------------------------------------------------------------------------------------------------------+
    | ::: Install setup complete :::                                                                            |
    +-----------------------------------------------------------------------------------------------------------+
    | - Settings: 'with_mosaic_pip'? False, 'with_gdal'? True, 'with_ubuntugis'? False                          |
    |             'jar_copy'? False, 'jni_so_copy'? False, 'override_mosaic_version'? None                      |
    | - Derived:  'mosaic_version'? 0.4.0, 'github_version'? 0.4.0, 'release_version'? None, 'pip_str'? ==0.4.0 |
    | - Fuse Dir: '/Workspace/Shared/geospatial/mosaic/gdal/jammy'                                              |
    | - Init Script: configured and stored at 'mosaic-gdal-init.sh'; add to your cluster and restart,           |
    |               more at https://docs.databricks.com/en/init-scripts/cluster-scoped.html                     |
    +-----------------------------------------------------------------------------------------------------------+


Configure the init script
**************************
After the :code:`setup_gdal` function has been run, you will need to configure the cluster to use the
init script. The init script can be set by clicking on the "Edit" button on the cluster page and adding
the following to the "Advanced Options" section:

.. figure:: ../images/init_script.png
   :figclass: doc-figure

   Fig 1. Init script configuration

Enable GDAL for a notebook
***********************************
Once the cluster has been restarted, you can enable GDAL for a notebook by running the following
code at the top of the notebook:

.. code-block:: py

    import mosaic as mos

    mos.enable_mosaic(spark, dbutils)
    mos.enable_gdal(spark)

.. code-block:: text

    GDAL enabled.
    GDAL 3.4.1, released 2021/12/27

.. note::
    You can configure init script from default ubuntu GDAL (3.4.1) to ubuntugis ppa @ https://launchpad.net/~ubuntugis/+archive/ubuntu/ppa (3.4.3)
    with `setup_gdal(with_ubuntugis=True)`

GDAL Configuration
####################

Here are spark session configs available for raster, e.g. :code:`spark.conf.set("<key>", "<val>")`.

.. list-table:: Title
   :widths: 25 25 50
   :header-rows: 1

   * - Config
     - Default
     - Comments
   * - spark.databricks.labs.mosaic.raster.checkpoint
     - "/dbfs/tmp/mosaic/raster/checkpoint"
     - Checkpoint location, see :ref:`rst_maketiles` for more
   * - spark.databricks.labs.mosaic.raster.tmp.prefix
     - "" (will use "/tmp")
     - Local directory for workers
   * - spark.databricks.labs.mosaic.raster.blocksize
     - "128"
     - Blocksize in pixels, see :ref:`rst_convolve` and :ref:`rst_filter` for more
