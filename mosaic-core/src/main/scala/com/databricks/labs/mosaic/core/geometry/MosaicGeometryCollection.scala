package com.databricks.labs.mosaic.core.geometry

/**
 * A trait that adds GeometryCollection functionality to MosaicGeometry.
 */
//noinspection DuplicatedCode
trait MosaicGeometryCollection extends MosaicGeometry {

    def asSeq: Seq[MosaicGeometry]

    override def flatten: Seq[MosaicGeometry]

    override def getShellPoints: Seq[Seq[MosaicPoint]]

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

}
