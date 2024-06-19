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
     * @return
     *   A byte array containing the raster data.
     */
    def writeToBytes(doDestroy: Boolean): Array[Byte]

    /**
     * Writes a raster to a specified file system path.
     *
     * @param newPath
     *   The path to write the raster.
     * @param doDestroy
     *   A boolean indicating if the raster object should be destroyed after writing.
     *   - file paths handled separately.
     *   Skip deletion of interim file writes, if any.
     * @return
     *   The path where written (may differ, e.g. due to subdatasets).
     */
    def writeToPath(newPath: String, doDestroy: Boolean): String

    /**
     * Writes a raster to the configured checkpoint directory.
     *
     * @param doDestroy
     *   A boolean indicating if the raster object should be destroyed after writing.
     *   - file paths handled separately.
     *   Skip deletion of interim file writes, if any.
     * @return
     *   The path where written (may differ, e.g. due to subdatasets).
     */
    def writeToCheckpointDir(doDestroy: Boolean): String

}
