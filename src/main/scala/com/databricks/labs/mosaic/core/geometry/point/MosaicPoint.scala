package com.databricks.labs.mosaic.core.geometry.point

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.MosaicCoreException
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.uber.h3core.util.GeoCoord
import org.locationtech.jts.geom.Coordinate

trait MosaicPoint extends MosaicGeometry {

    def getX: Double

    def getY: Double

    def getZ: Double

    def geoCoord: GeoCoord

    def coord: Coordinate

    def asSeq: Seq[Double]

    override def flatten: Seq[MosaicGeometry] = List(this)

    override def getShellPoints: Seq[Seq[MosaicPoint]] = Seq(Seq(this))

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]] = Nil

    override def getShells: Seq[MosaicLineString] =
        throw MosaicCoreException.InvalidGeometryOperation("getShells should not be called on a Point.")

    override def getHoles: Seq[Seq[MosaicLineString]] = Nil

}
