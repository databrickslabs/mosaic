package com.databricks.labs.mosaic.core.raster.io

/**
  * RasterWriter is a trait that defines the interface for writing raster data
  * to a file system path or as bytes. It is used by the RasterAPI to write
  * rasters.
  */
trait RasterWriter {

    /**
      * Writes a raster to a file system path.
      *
      * @param path
      *   The path to the raster file.
      * @param destroy
      *   A boolean indicating if the raster should be destroyed after writing.
      * @return
      *   A boolean indicating if the write was successful.
      */
    def writeToPath(path: String, destroy: Boolean = true): String

    /**
      * Writes a raster to a byte array.
      *
      * @param destroy
      *   A boolean indicating if the raster should be destroyed after writing.
      * @return
      *   A byte array containing the raster data.
      */
    def writeToBytes(destroy: Boolean = true): Array[Byte]

}
