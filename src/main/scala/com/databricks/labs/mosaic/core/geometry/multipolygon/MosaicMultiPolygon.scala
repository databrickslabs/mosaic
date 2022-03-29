package com.databricks.labs.mosaic.core.geometry.multipolygon

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygon

trait MosaicMultiPolygon {

    def asSeq: Seq[MosaicGeometry]

    def getBoundaryPoints: Seq[MosaicPoint]

    def getHolePoints: Seq[Seq[MosaicPoint]]

    def getPolygons: Seq[MosaicPolygon]

}
