package com.databricks.labs.mosaic.core.crs

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint

/**
  * CRSBounds captures lower left and upper right extreme points for a given
  * CRS. Extreme points are provided as MosaicPoints. The CRSBounds instances
  * are constructed via geometry API.
  * @param lowerLeft
  *   Lower left extreme point (xmin, ymin).
  * @param upperRight
  *   Upper right extreme point (xmax, ymax).
  */
case class CRSBounds(lowerLeft: MosaicPoint, upperRight: MosaicPoint)

object CRSBounds {

    /**
      * Construct CRSBounds instance for give extreme coordinate values.
      * Construction is bound for the selected geometry API at runtime.
      * @param geometryAPI
      *   Geometry API attached to Mosaic Context.
      * @param x1
      *   Minimum x coordinate value.
      * @param y1
      *   Minimum y coordinate value.
      * @param x2
      *   Maximum x coordinate value.
      * @param y2
      *   Maximum y coordinate value.
      * @return
      */
    def apply(geometryAPI: GeometryAPI, x1: Double, y1: Double, x2: Double, y2: Double): CRSBounds = {
        CRSBounds(geometryAPI.fromCoords(Seq(x1, y1)), geometryAPI.fromCoords(Seq(x2, y2)))
    }
}
