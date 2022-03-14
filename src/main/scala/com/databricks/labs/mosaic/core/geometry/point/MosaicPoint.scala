package com.databricks.labs.mosaic.core.geometry.point

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.uber.h3core.util.GeoCoord
import org.locationtech.jts.geom.Coordinate

trait MosaicPoint extends MosaicGeometry {

    def getX: Double

    def getY: Double

    def getZ: Double

    def geoCoord: GeoCoord

    def coord: Coordinate

    def asSeq: Seq[Double]

}
