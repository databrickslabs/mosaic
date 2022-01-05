package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.core.geometry.point.MosaicPoint

trait MosaicGeometry extends GeometryWriter {

  def isValid: Boolean

  def getGeometryType: String

  def getArea: Double

  def getAPI: String

  def getCentroid: MosaicPoint

  def getCoordinates: Seq[MosaicPoint]

  def isEmpty: Boolean

  def getBoundary: Seq[MosaicPoint]

  def getHoles: Seq[Seq[MosaicPoint]]

  def boundary: MosaicGeometry

  def buffer(distance: Double): MosaicGeometry

  def simplify(tolerance: Double): MosaicGeometry

  def intersection(other: MosaicGeometry): MosaicGeometry

  def flatten: Seq[MosaicGeometry]

  def equals(other: MosaicGeometry): Boolean

  def equals(other: java.lang.Object): Boolean

  override def hashCode: Int

}
