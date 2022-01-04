package com.databricks.mosaic.core.geometry.multipolygon

import com.databricks.mosaic.core.geometry.point.MosaicPoint

trait MosaicMultiPolygon {

  def getBoundaryPoints: Seq[MosaicPoint]

  def getHolePoints: Seq[Seq[MosaicPoint]]

}
