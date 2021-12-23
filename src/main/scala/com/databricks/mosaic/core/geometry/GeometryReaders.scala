package com.databricks.mosaic.core.geometry


trait GeometryReaders {
  def fromWKB(wkb: Array[Byte]): MosaicGeometry

  def fromWKT(wkt: String): MosaicGeometry

  def fromJSON(geoJson: String): MosaicGeometry

  def fromHEX(hex: String): MosaicGeometry

  def fromPoints(points: Seq[MosaicPoint]): MosaicGeometry
}
