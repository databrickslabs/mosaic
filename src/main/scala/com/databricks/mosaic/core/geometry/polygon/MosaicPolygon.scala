package com.databricks.mosaic.core.geometry.polygon

import com.databricks.mosaic.core.geometry.MosaicGeometry
import com.databricks.mosaic.core.geometry.point.MosaicPoint

trait MosaicPolygon extends MosaicGeometry {

    def getBoundaryPoints: Seq[MosaicPoint]

    def getHolePoints: Seq[Seq[MosaicPoint]]

}
