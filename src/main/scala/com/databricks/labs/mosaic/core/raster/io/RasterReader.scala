package com.databricks.labs.mosaic.core.raster.io

import com.databricks.labs.mosaic.core.raster.gdal.{MosaicRasterBandGDAL, MosaicRasterGDAL}

trait RasterReader {

    /**
     * Reads a raster band from a file system path. Reads a subdataset band if
     * the path is to a subdataset. Assumes "path" is a key in createInfo.
     *
     * @example
     *   Raster: path = "/path/to/file.tif" Subdataset: path =
     *   "FORMAT:/path/to/file.tif:subdataset"
     * @param bandIndex
     *  The band index to read (1+ indexed).
     * @param createInfo
     *   Map of create info for the raster.
     * @return
     *   A [[MosaicRasterBandGDAL]] object.
     */
    def readBand(bandIndex: Int, createInfo: Map[String, String]): MosaicRasterBandGDAL

    /**
     * Reads a raster from a byte array. Expects "driver" in createInfo.
     * @param contentBytes
     *   The byte array containing the raster data.
     * @param createInfo
     *   Mosaic creation info of the raster. Note: This is not the same as the
     *   metadata of the raster. This is not the same as GDAL creation options.
     * @return
     *   A [[MosaicRasterGDAL]] object.
     */
    def readRaster(contentBytes: Array[Byte], createInfo: Map[String, String]): MosaicRasterGDAL

    /**
     * Reads a raster from a file system path. Reads a subdataset if the path
     * is to a subdataset. Assumes "path" is a key in createInfo.
     *
     * @example
     *   Raster: path = "/path/to/file.tif" Subdataset: path =
     *   "FORMAT:/path/to/file.tif:subdataset"
     * @param createInfo
     *   Map of create info for the raster.
     * @return
     *   A [[MosaicRasterGDAL]] object.
     */
    def readRaster(createInfo: Map[String, String]): MosaicRasterGDAL

}
