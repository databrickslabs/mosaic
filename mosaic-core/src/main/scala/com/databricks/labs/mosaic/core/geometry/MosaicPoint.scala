package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.types.Coordinates

/**
 * A trait that adds Point functionality to MosaicGeometry.
 */
trait MosaicPoint extends MosaicGeometry {

    def getX: Double

    def getY: Double

    def getZ: Double

    def geoCoord: Coordinates

    def asSeq: Seq[Double]

    override def flatten: Seq[MosaicGeometry]

    override def getShellPoints: Seq[Seq[MosaicPoint]]

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

    override def getShells: Seq[MosaicLineString]

    override def getHoles: Seq[Seq[MosaicLineString]]

}
