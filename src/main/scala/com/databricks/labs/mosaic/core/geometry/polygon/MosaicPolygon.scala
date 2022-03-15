package com.databricks.labs.mosaic.core.geometry.polygon

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint

trait MosaicPolygon extends MosaicGeometry {

    def getBoundaryPoints: Seq[MosaicPoint]

    def getHolePoints: Seq[Seq[MosaicPoint]]

}
