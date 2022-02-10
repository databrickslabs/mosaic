package com.databricks.mosaic.core.geometry.linestring

import com.databricks.mosaic.core.geometry.MosaicGeometry
import com.databricks.mosaic.core.geometry.point.MosaicPoint

trait MosaicLineString extends MosaicGeometry {

    def getBoundaryPoints: Seq[MosaicPoint]

    def getHolePoints: Seq[Seq[MosaicPoint]]

    def asSeq: Seq[MosaicPoint]

}
