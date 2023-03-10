package com.databricks.labs.mosaic.core.geometry.multipolygon

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint

trait MosaicMultiPolygon extends MosaicGeometry {

    def asSeq: Seq[MosaicGeometry]

    override def flatten: Seq[MosaicGeometry]

    override def getShellPoints: Seq[Seq[MosaicPoint]]

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

}
