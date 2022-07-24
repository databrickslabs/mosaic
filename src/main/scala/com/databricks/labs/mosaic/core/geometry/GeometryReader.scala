package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import org.apache.spark.sql.catalyst.InternalRow

trait GeometryReader {

    val defaultSpatialReferenceId: Int = 4326

    def fromInternal(row: InternalRow): MosaicGeometry

    def fromWKB(wkb: Array[Byte]): MosaicGeometry

    def fromWKT(wkt: String): MosaicGeometry

    def fromJSON(geoJson: String): MosaicGeometry

    def fromHEX(hex: String): MosaicGeometry

    def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value): MosaicGeometry

}
