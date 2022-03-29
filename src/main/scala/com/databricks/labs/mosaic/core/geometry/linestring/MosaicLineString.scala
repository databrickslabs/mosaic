package com.databricks.labs.mosaic.core.geometry.linestring

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint

trait MosaicLineString extends MosaicGeometry {

    def asSeq: Seq[MosaicPoint] = getShellPoints.head

    override def getHoles: Seq[Seq[MosaicLineString]] = Nil

    override def getShells: Seq[MosaicLineString] = Seq(this)

    override def flatten: Seq[MosaicGeometry] = Seq(this)

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]] = Nil


}
