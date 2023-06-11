package com.databricks.labs.mosaic.core.raster

/**
  * A base trait for all Raster API's.
  * @param reader
  *   The RasterReader to use for reading the raster.
  */
abstract class RasterAPI(reader: RasterReader) extends Serializable {

    /**
      * This method should be called in every raster expression if the RasterAPI
      * requires enablement on worker nodes.
      */
    def enable(): Unit

    /** @return Returns the name of the raster API. */
    def name: String

    /**
      * Reads a raster from the given path.
      *
      * @param path
      *   The path to the raster. This path has to be a path to a single raster.
      *   Rasters with subdatasets are supported.
      * @return
      *   Returns a Raster object.
      */
    def raster(path: String): MosaicRaster = reader.readRaster(path)

    /**
      * Reads a raster from the given path. It extracts the specified band from
      * the raster.
      *
      * @param path
      *   The path to the raster. This path has to be a path to a single raster.
      *   Rasters with subdatasets are supported.
      * @param bandIndex
      *   The index of the band to read from the raster.
      * @return
      *   Returns a Raster band object.
      */
    def band(path: String, bandIndex: Int): MosaicRasterBand = reader.readBand(path, bandIndex)

    /**
      * Converts raster x, y coordinates to lat, lon coordinates.
      * @param gt
      *   Geo transform of the raster.
      * @param x
      *   X coordinate of the raster.
      * @param y
      *   Y coordinate of the raster.
      * @return
      *   Returns a tuple of (lat, lon).
      */
    def toWorldCoord(gt: Seq[Double], x: Int, y: Int): (Double, Double) = {
        val (xGeo, yGeo) = reader.toWorldCoord(gt, x, y)
        (xGeo, yGeo)
    }

    /**
      * Converts lat, lon coordinates to raster x, y coordinates.
      * @param gt
      *   Geo transform of the raster.
      * @param x
      *   Latitude of the raster.
      * @param y
      *   Longitude of the raster.
      * @return
      *   Returns a tuple of (xPixel, yPixel).
      */
    def fromWorldCoord(gt: Seq[Double], x: Double, y: Double): (Int, Int) = {
        val (xPixel, yPixel) = reader.fromWorldCoord(gt, x, y)
        (xPixel, yPixel)
    }

}