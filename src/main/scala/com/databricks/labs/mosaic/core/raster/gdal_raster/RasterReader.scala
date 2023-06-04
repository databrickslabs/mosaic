package com.databricks.labs.mosaic.core.raster.gdal_raster

import com.databricks.labs.mosaic.core.raster.{MosaicRaster, MosaicRasterBand}
import org.apache.spark.internal.Logging

/**
  * RasterReader is a trait that defines the interface for reading raster data
  * from a file system path. It is used by the RasterAPI to read raster and
  * raster band data.
  * @note
  *   For subdatasets the path should be the path to the subdataset and not to
  *   the file.
  */
trait RasterReader extends Logging {

    /**
      * Reads a raster from a file system path. Reads a subdataset if the path
      * is to a subdataset.
      *
      * @example
      *   Raster: path = "/path/to/file.tif" Subdataset: path =
      *   "FORMAT:/path/to/file.tif:subdataset"
      * @param path
      *   The path to the raster file.
      * @return
      *   A MosaicRaster object.
      */
    def readRaster(path: String): MosaicRaster

    /**
      * Reads a raster from an in memory buffer. Use the buffer bytes to produce
      * a uuid of the raster.
      *
      * @param contentBytes
      *   The file bytes.
      * @return
      *   A MosaicRaster object.
      */
    def readRaster(contentBytes: Array[Byte]): MosaicRaster

    /**
      * Reads a raster band from a file system path. Reads a subdataset band if
      * the path is to a subdataset.
      * @example
      *   Raster: path = "/path/to/file.tif" Subdataset: path =
      *   "FORMAT:/path/to/file.tif:subdataset"
      * @param path
      *   The path to the raster file.
      * @param bandIndex
      *   The band index to read.
      * @return
      *   A MosaicRaster object.
      */
    def readBand(path: String, bandIndex: Int): MosaicRasterBand

}
