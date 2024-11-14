===================================
Checkpointing for raster operations
===================================

Mosaic offers the ability to checkpoint raster operations to disk. This is useful when working with large rasters and
complex operations that may require multiple stages of computation. Checkpointing can be used to save intermediate results
to the cloud object store, which can be loaded back into memory at a later stage.
This can help to reduce the amount of memory required to store intermediate results, and can also help to improve
performance by reducing the amount of data that needs to be transferred between nodes during wide transformations.

Checkpointing is enabled by setting the :code:`spark.databricks.labs.mosaic.raster.use.checkpoint` configuration to :code:`true`.
By default, checkpointing is disabled. When checkpointing is enabled, Mosaic will save intermediate results to the checkpoint directory
specified by the :code:`spark.databricks.labs.mosaic.raster.checkpoint` configuration.
The checkpoint directory must be a valid DBFS path (by default it is set to :code:`/dbfs/tmp/mosaic/raster/checkpoint`).

There are also a number of helper functions that can be used to manage checkpointing in Mosaic.

The simplest way to enable checkpointing is to specify a checkpoint directory when calling :code:`enable_gdal()` from the Python interface...

    .. code-block:: python

          import mosaic as mos

          mos.enable_mosaic(spark, dbutils)
          mos.enable_gdal(spark, checkpoint_dir="/dbfs/tmp/mosaic/raster/checkpoint")


... or directly from Scala using :code:`MosaicGDAL.enableGDALWithCheckpoint()`:

    .. code-block:: scala

          import com.databricks.labs.mosaic.functions.MosaicContext
          import com.databricks.labs.mosaic.gdal.MosaicGDAL
          import com.databricks.labs.mosaic.H3
          import com.databricks.labs.mosaic.JTS

          val mosaicContext = MosaicContext.build(H3, JTS)
          import mosaicContext.functions._

          MosaicGDAL.enableGDALWithCheckpoint(session, "/dbfs/tmp/mosaic/raster/checkpoint")

Checkpointing can be modified  within the Python interface using the functions

  * :code:`update_checkpoint_path(spark: SparkSession, path: str)`
  * :code:`set_checkpoint_on(spark: SparkSession)`
  * :code:`set_checkpoint_off(spark: SparkSession)`

