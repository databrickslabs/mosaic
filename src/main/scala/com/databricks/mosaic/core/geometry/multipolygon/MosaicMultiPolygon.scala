package com.databricks.mosaic.core.geometry.multipolygon

import com.databricks.mosaic.core.geometry.MosaicGeometry
import com.databricks.mosaic.core.geometry.point.MosaicPoint

trait MosaicMultiPolygon {

  def asSeq: Seq[MosaicGeometry]

  def getBoundaryPoints: Seq[MosaicPoint]

  def getHolePoints: Seq[Seq[MosaicPoint]]

}
