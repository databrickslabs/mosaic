package com.databricks.labs.mosaic.core.geometry.multilinestring

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint

trait MosaicMultiLineString extends MosaicGeometry {

    def asSeq: Seq[MosaicLineString]

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]] = Nil

    override def getShellPoints: Seq[Seq[MosaicPoint]] = getShells.map(_.asSeq)

    override def getHoles: Seq[Seq[MosaicLineString]] = Nil

    override def flatten: Seq[MosaicGeometry] = asSeq

}
