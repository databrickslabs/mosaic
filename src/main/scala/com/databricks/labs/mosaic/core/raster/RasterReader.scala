package com.databricks.labs.mosaic.core.raster

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
      *   Raster: path = "file:///path/to/file.tif" Subdataset: path =
      *   "file:///path/to/file.tif:subdataset"
      * @param path
      *   The path to the raster file.
      * @return
      *   A MosaicRaster object.
      */
    def readRaster(path: String): MosaicRaster

    /**
      * Reads a raster band from a file system path. Reads a subdataset band if
      * the path is to a subdataset.
      * @example
      *   Raster: path = "file:///path/to/file.tif" Subdataset: path =
      *   "file:///path/to/file.tif:subdataset"
      * @param path
      *   The path to the raster file.
      *
      * @param bandIndex
      *   The band index to read.
      * @return
      *   A MosaicRaster object.
      */
    def readBand(path: String, bandIndex: Int): MosaicRasterBand

    /**
      * Take a geo transform matrix and x and y coordinates of a pixel and
      * returns the x and y coordinates in the projection of the raster.
      *
      * @param geoTransform
      *   The geo transform matrix of the raster.
      *
      * @param x
      *   The x coordinate of the pixel.
      * @param y
      *   The y coordinate of the pixel.
      * @return
      *   A tuple of doubles with the x and y coordinates in the projection of
      *   the raster.
      */
    def toWorldCoord(geoTransform: Seq[Double], x: Int, y: Int): (Double, Double)

    /**
      * Take a geo transform matrix and x and y coordinates of a point and
      * returns the x and y coordinates of the raster pixel.
      *
      * @param geoTransform
      *   The geo transform matrix of the raster.
      *
      * @param x
      *   The x coordinate of the point.
      * @param y
      *   The y coordinate of the point.
      * @return
      *   A tuple of integers with the x and y coordinates of the raster pixel.
      */
    def fromWorldCoord(geoTransform: Seq[Double], x: Double, y: Double): (Int, Int)

}
