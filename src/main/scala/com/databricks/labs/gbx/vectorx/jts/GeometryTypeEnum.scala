package com.databricks.labs.gbx.vectorx.jts

object GeometryTypeEnum extends Enumeration {

    val INVALID: GeometryTypeEnum.Value = Value(0, "INVALID")
    val POINT: GeometryTypeEnum.Value = Value(1, "POINT")
    val MULTIPOINT: GeometryTypeEnum.Value = Value(2, "MULTIPOINT")
    val LINESTRING: GeometryTypeEnum.Value = Value(3, "LINESTRING")
    val MULTILINESTRING: GeometryTypeEnum.Value = Value(4, "MULTILINESTRING")
    val POLYGON: GeometryTypeEnum.Value = Value(5, "POLYGON")
    val MULTIPOLYGON: GeometryTypeEnum.Value = Value(6, "MULTIPOLYGON")
    // coercion type JTS boundary returns LinearRing instead of LineString
    val LINEARRING: GeometryTypeEnum.Value = Value(7, "LINEARRING")
    val GEOMETRYCOLLECTION: GeometryTypeEnum.Value = Value(8, "GEOMETRYCOLLECTION")

    def apply(tpe: String): GeometryTypeEnum.Value = {
        GeometryTypeEnum.values
            .find(_.toString.equalsIgnoreCase(tpe))
            .getOrElse(INVALID)
    }

    def fromTypeId(typeId: Int): GeometryTypeEnum.Value = {
        GeometryTypeEnum.values
            .find(_.id == typeId)
            .getOrElse(INVALID)
    }

}
