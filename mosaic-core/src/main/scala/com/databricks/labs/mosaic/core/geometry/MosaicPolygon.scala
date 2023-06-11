package com.databricks.labs.mosaic.core.geometry

/**
 * A trait that adds Polygon functionality to MosaicGeometry.
 */
trait MosaicPolygon extends MosaicGeometry {

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

    override def flatten: Seq[MosaicGeometry]

    override def getShellPoints: Seq[Seq[MosaicPoint]]

    def asSeq: Seq[MosaicLineString]

}
