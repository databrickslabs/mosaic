package com.databricks.labs.mosaic.core.raster.gdal_raster

/**
 * RasterWriter is a trait that defines the interface for writing raster data
 * to a file system path or as bytes. It is used by the RasterAPI to write rasters.
 */
trait RasterWriter {

    /**
     * Writes a raster to a file system path.
     *
     * @param path
     *   The path to the raster file.
     * @return
     *   A boolean indicating if the write was successful.
     */
    def writeToPath(path: String): String

    /**
     * Writes a raster to a byte array.
     *
     * @return
     *   A byte array containing the raster data.
     */
    def writeToBytes(): Array[Byte]

}
