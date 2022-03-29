package com.databricks.labs.mosaic.core.geometry.multipoint

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.MosaicCoreException

trait MosaicMultiPoint extends MosaicGeometry {

    def asSeq: Seq[MosaicPoint]

    override def getHoles: Seq[Seq[MosaicLineString]] = Nil

    override def flatten: Seq[MosaicGeometry] = asSeq

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]] = Nil

    override def getShellPoints: Seq[Seq[MosaicPoint]] = Seq(asSeq)

    override def getShells: Seq[MosaicLineString] =
        throw MosaicCoreException.InvalidGeometryOperation("getShells should not be called on MultiPoints.")

}
