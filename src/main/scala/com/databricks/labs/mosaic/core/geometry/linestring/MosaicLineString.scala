package com.databricks.labs.mosaic.core.geometry.linestring

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint

trait MosaicLineString extends MosaicGeometry {

    def asSeq: Seq[MosaicPoint]

    override def getHoles: Seq[Seq[MosaicLineString]]

    override def getShells: Seq[MosaicLineString]

    override def flatten: Seq[MosaicGeometry]

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

}
