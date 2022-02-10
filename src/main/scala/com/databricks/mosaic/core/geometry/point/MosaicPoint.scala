package com.databricks.mosaic.core.geometry.point

import com.uber.h3core.util.GeoCoord
import org.locationtech.jts.geom.Coordinate

import com.databricks.mosaic.core.geometry.MosaicGeometry

trait MosaicPoint extends MosaicGeometry {

    def getX: Double

    def getY: Double

    def getZ: Double

    def geoCoord: GeoCoord

    def coord: Coordinate

    def asSeq: Seq[Double]

}
