package com.databricks.labs.mosaic.core.raster.io

/**
  * RasterWriter is a trait that defines the interface for writing raster data
  * to a file system path or as bytes. It is used by the [[com.databricks.labs.mosaic.core.raster.api.GDAL]]
  * Raster API to write rasters from the internal [[com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL]]
  * object.
  */
trait RasterWriter {

    /**
      * Writes a raster to a byte array.
      *
      * @param doDestroy
      *   A boolean indicating if the raster object should be destroyed after writing.
      *   - file paths handled separately.
      * @param manualMode
      *   Skip deletion of interim file writes, e.g. for subdatasets.
      * @return
      *   A byte array containing the raster data.
      */
    def writeToBytes(doDestroy: Boolean, manualMode: Boolean): Array[Byte]

    /**
      * Writes a raster to a specified file system path.
      *
      * @param newPath
      *   The path to write the raster.
      * @param doDestroy
      *   A boolean indicating if the raster object should be destroyed after writing.
      *   - file paths handled separately.
      * @param manualMode
      *   Skip deletion of interim file writes, if any.
      * @return
      *   The path where written (may differ, e.g. due to subdatasets).
      */
    def writeToPath(newPath: String, doDestroy: Boolean, manualMode: Boolean): String

}
