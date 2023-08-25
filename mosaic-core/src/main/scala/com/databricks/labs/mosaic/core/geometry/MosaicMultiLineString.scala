package com.databricks.labs.mosaic.core.geometry

/**
 * A trait that adds MultiLineString functionality to MosaicGeometry.
 */
trait MosaicMultiLineString extends MosaicGeometry {

    def asSeq: Seq[MosaicLineString]

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

    override def getShellPoints: Seq[Seq[MosaicPoint]]

    override def getHoles: Seq[Seq[MosaicLineString]]

    override def flatten: Seq[MosaicGeometry]

}
