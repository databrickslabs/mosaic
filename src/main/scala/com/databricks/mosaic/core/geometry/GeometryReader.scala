package com.databricks.mosaic.core.geometry

import com.databricks.mosaic.core.geometry.point.MosaicPoint
import com.databricks.mosaic.core.types.model.GeometryTypeEnum
import org.apache.spark.sql.catalyst.InternalRow

trait GeometryReader {

  def fromInternal(row: InternalRow): MosaicGeometry

  def fromWKB(wkb: Array[Byte]): MosaicGeometry

  def fromWKT(wkt: String): MosaicGeometry

  def fromJSON(geoJson: String): MosaicGeometry

  def fromHEX(hex: String): MosaicGeometry

  def fromPoints(points: Seq[MosaicPoint], geomType: GeometryTypeEnum.Value): MosaicGeometry

}
