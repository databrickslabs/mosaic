package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.core.geometry.point.MosaicPoint
import org.apache.spark.sql.catalyst.InternalRow

trait GeometryReader {

  def fromInternal(row: InternalRow): MosaicGeometry

  def fromWKB(wkb: Array[Byte]): MosaicGeometry

  def fromWKT(wkt: String): MosaicGeometry

  def fromJSON(geoJson: String): MosaicGeometry

  def fromHEX(hex: String): MosaicGeometry

  def fromPoints(points: Seq[MosaicPoint]): MosaicGeometry

}
