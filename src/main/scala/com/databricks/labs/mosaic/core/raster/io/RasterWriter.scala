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
     * @param destroy
     *   A boolean indicating if the raster should be destroyed after writing.
     * @return
     *   A byte array containing the raster data.
     */
    def writeToBytes(destroy: Boolean = true): Array[Byte]

    /**
      * Writes a raster to a specified file system path.
      *
      * @param newPath
      *   The path to write the raster.
      * @param destroy
      *   A boolean indicating if the raster should be destroyed after writing.
      * @return
      *   The path where written (may differ, e.g. due to subdatasets).
      */
    def writeToPath(newPath: String, destroy: Boolean = true): String

}
