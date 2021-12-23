package com.databricks.mosaic.core.geometry

trait MosaicMultiPolygon {

  def getBoundaryPoints: Seq[MosaicPoint]

  def getHolePoints: Seq[Seq[MosaicPoint]]

}
