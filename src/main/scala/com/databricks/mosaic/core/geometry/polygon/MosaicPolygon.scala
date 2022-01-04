package com.databricks.mosaic.core.geometry.polygon

import com.databricks.mosaic.core.geometry.point.MosaicPoint

trait MosaicPolygon {

  def getBoundaryPoints: Seq[MosaicPoint]

  def getHolePoints: Seq[Seq[MosaicPoint]]

}
