package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint

trait MosaicPolygon extends MosaicGeometry {

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]] = getHoles.map(_.map(_.asSeq))

    override def flatten: Seq[MosaicGeometry] = List(this)

    override def getShellPoints: Seq[Seq[MosaicPoint]] = getShells.map(_.asSeq)

}
