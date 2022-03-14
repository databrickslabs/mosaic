package com.databricks.labs.mosaic.core.geometry.linestring

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint

trait MosaicLineString extends MosaicGeometry {

    def getBoundaryPoints: Seq[MosaicPoint]

    def getHolePoints: Seq[Seq[MosaicPoint]]

    def asSeq: Seq[MosaicPoint]

}
