package com.databricks.labs.mosaic.core.geometry.geometrycollection

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint

trait MosaicGeometryCollection extends MosaicGeometry {

    def asSeq: Seq[MosaicGeometry]

    override def flatten: Seq[MosaicGeometry]

    override def getShellPoints: Seq[Seq[MosaicPoint]]

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

}
