package com.databricks.labs.mosaic.core.geometry

/**
 * A trait that adds MultiPoint functionality to MosaicGeometry.
 */
trait MosaicMultiPoint extends MosaicGeometry {

    def asSeq: Seq[MosaicPoint]

    override def getHoles: Seq[Seq[MosaicLineString]]

    override def flatten: Seq[MosaicGeometry]

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

    override def getShellPoints: Seq[Seq[MosaicPoint]]

    override def getShells: Seq[MosaicLineString]

}
