package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.core.geometry.point.MosaicPoint

trait MosaicGeometry extends GeometryWriter with Serializable {

  def contains(geom2: MosaicGeometry): Boolean

  def getLength: Double

  def distance(geom2: MosaicGeometry): Double

  def isValid: Boolean

  def getGeometryType: String

  def getArea: Double

  def getAPI: String

  def getCentroid: MosaicPoint

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

  def hashCode: Int

}
