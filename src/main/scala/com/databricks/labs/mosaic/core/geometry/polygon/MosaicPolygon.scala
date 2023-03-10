package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint

trait MosaicPolygon extends MosaicGeometry {

    override def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

    override def flatten: Seq[MosaicGeometry]

    override def getShellPoints: Seq[Seq[MosaicPoint]]

    def asSeq: Seq[MosaicLineString]

}
