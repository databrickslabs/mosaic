package com.databricks.labs.mosaic.core.raster.io

import com.databricks.labs.mosaic.core.raster.gdal.{MosaicRasterBandGDAL, MosaicRasterGDAL}
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
      * @param createInfo
      *   The create info for the raster. This should contain the following
      *   keys:
      *   - path: The path to the raster file.
      *   - parentPath: The path of the parent raster file.
      *   - driver: Optional: The driver short name of the raster file
      * @return
      *   A MosaicRaster object.
      */
    def readRaster(createInfo: Map[String, String]): MosaicRasterGDAL

    /**
      * Reads a raster from an in memory buffer. Use the buffer bytes to produce
      * a uuid of the raster.
      *
      * @param contentBytes
      *   The file bytes.
      * @param createInfo
      *   The create info for the raster. This should contain the following
      *   keys:
      *   - parentPath: The path of the parent raster file.
      *   - driver: The driver short name of the raster file
      * @return
      *   A MosaicRaster object.
      */
    def readRaster(contentBytes: Array[Byte], createInfo: Map[String, String]): MosaicRasterGDAL

    /**
      * Reads a raster band from a file system path. Reads a subdataset band if
      * the path is to a subdataset.
      *
      * @example
      *   Raster: path = "/path/to/file.tif" Subdataset: path =
      *   "FORMAT:/path/to/file.tif:subdataset"
      * @param createInfo
      *   The create info for the raster. This should contain the following
      *   keys:
      *   - path: The path to the raster file.
      *   - parentPath: The path of the parent raster file.
      *   - driver: Optional: The driver short name of the raster file
      * @return
      *   A MosaicRaster object.
      */
    def readBand(bandIndex: Int, createInfo: Map[String, String]): MosaicRasterBandGDAL

}
