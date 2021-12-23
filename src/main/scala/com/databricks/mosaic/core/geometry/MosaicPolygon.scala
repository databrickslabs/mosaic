package com.databricks.mosaic.core.geometry

trait MosaicPolygon {

  def getBoundaryPoints: Seq[MosaicPoint]

  def getHolePoints: Seq[Seq[MosaicPoint]]

}
