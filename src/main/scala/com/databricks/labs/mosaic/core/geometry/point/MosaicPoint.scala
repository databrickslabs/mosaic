package com.databricks.labs.mosaic.core.geometry.point

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.MosaicCoreException
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.types.model.Coordinates
import org.locationtech.jts.geom.Coordinate

trait MosaicPoint extends MosaicGeometry {

    def getX: Double

    def getY: Double

    def getZ: Double

    def geoCoord: Coordinates

    def coord: Coordinate

    def asSeq: Seq[Double]

    override def flatten: Seq[MosaicGeometry]

    override def getShellPoints: Seq[Seq[MosaicPoint]]

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

    override def getShells: Seq[MosaicLineString] =
        throw MosaicCoreException.InvalidGeometryOperation("getShells should not be called on a Point.")

    override def getHoles: Seq[Seq[MosaicLineString]]

}
