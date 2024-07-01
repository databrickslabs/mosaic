package com.databricks.labs.mosaic.core.raster.operator.transform

trait RasterTransform {

    /**
      * Take a geo transform matrix and x and y coordinates of a pixel and
      * returns the x and y coors in the projection of the raster. As per GDAL
      * documentation, the origin is the top left corner of the top left pixel
      *
      * @see
      *   https://gdal.org/tutorials/raster_api_tut.html
      * @param geoTransform
      *   The geo transform matrix of the raster.
      * @param x
      *   The x coordinate of the pixel.
      * @param y
      *   The y coordinate of the pixel.
      * @return
      *   A tuple of doubles with the x and y coordinates in the projection of
      *   the raster.
      */
    def toWorldCoord(geoTransform: Seq[Double], x: Int, y: Int): (Double, Double) = {
        val Xp = geoTransform.head + x * geoTransform(1) + y * geoTransform(2)
        val Yp = geoTransform(3) + x * geoTransform(4) + y * geoTransform(5)
        (Xp, Yp)
    }

    /**
      * Take a geo transform matrix and x and y coordinates of a point and
      * returns the x and y coordinates of the raster pixel.
      *
      * @see
      *   // Reference:
      *   https://gis.stackexchange.com/questions/221292/retrieve-pixel-value-with-geographic-coordinate-as-input-with-gdal
      * @param geoTransform
      *   The geo transform matrix of the raster.
      * @param xGeo
      *   The x coordinate of the point.
      * @param yGeo
      *   The y coordinate of the point.
      * @return
      *   A tuple of integers with the x and y coordinates of the raster pixel.
      */
    def fromWorldCoord(geoTransform: Seq[Double], xGeo: Double, yGeo: Double): (Int, Int) = {
        val x = ((xGeo - geoTransform.head) / geoTransform(1)).toInt
        val y = ((yGeo - geoTransform(3)) / geoTransform(5)).toInt
        (x, y)
    }

}
