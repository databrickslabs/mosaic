package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.types.GeometryTypeEnum

/**
 * A trait that defines the methods for reading geometry data.
 * If a new format requires support, fromFormat method should be added to this trait.
 */
trait GeometryReader {

  val defaultSpatialReferenceId: Int = 4326

  def fromWKB(wkb: Array[Byte]): MosaicGeometry

  def fromWKT(wkt: String): MosaicGeometry

  def fromJSON(geoJson: String): MosaicGeometry

  def fromHEX(hex: String): MosaicGeometry

  def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value): MosaicGeometry

}
