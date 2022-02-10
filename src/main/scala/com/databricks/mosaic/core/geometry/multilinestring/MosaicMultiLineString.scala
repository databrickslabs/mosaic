package com.databricks.mosaic.core.geometry.multilinestring

import com.databricks.mosaic.core.geometry.MosaicGeometry
import com.databricks.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.mosaic.core.geometry.point.MosaicPoint

trait MosaicMultiLineString extends MosaicGeometry {

    def getBoundaryPoints: Seq[Seq[MosaicPoint]]

    def getHolePoints: Seq[Seq[Seq[MosaicPoint]]]

    def asSeq: Seq[MosaicLineString]

}
