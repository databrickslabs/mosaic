package com.databricks.mosaic.core.geometry.multipoint

import com.databricks.mosaic.core.geometry.MosaicGeometry
import com.databricks.mosaic.core.geometry.point.MosaicPoint
import com.databricks.mosaic.core.geometry.polygon.MosaicPolygon

trait MosaicMultiPoint extends MosaicGeometry {

    def asSeq: Seq[MosaicPoint]

    def convexHull: MosaicPolygon

}
