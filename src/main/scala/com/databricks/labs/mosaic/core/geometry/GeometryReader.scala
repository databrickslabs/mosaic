package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum

import org.apache.spark.sql.catalyst.InternalRow

trait GeometryReader {

    def fromInternal(row: InternalRow): MosaicGeometry

    def fromWKB(wkb: Array[Byte]): MosaicGeometry

    def fromWKT(wkt: String): MosaicGeometry

    def fromJSON(geoJson: String): MosaicGeometry

    def fromHEX(hex: String): MosaicGeometry

    def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value): MosaicGeometry

    def fromKryo(row: InternalRow): MosaicGeometry

    val defaultSpatialReferenceId: Int = 4326

}
