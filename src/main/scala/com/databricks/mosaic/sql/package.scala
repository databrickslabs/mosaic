package com.databricks.mosaic

import java.util.Locale

package object sql {

    object MosaicJoinType extends Enumeration {

        val POINT_IN_POLYGON: MosaicJoinType.Value = Value(1, "PointInPolygon")
        val POLYGON_INTERSECTION: MosaicJoinType.Value = Value(2, "PolygonIntersection")

        def fromString(value: String): MosaicJoinType.Value =
            MosaicJoinType.values
                .find(_.toString == value.toUpperCase(Locale.ROOT))
                .getOrElse(
                  throw new IllegalArgumentException(
                    s"Invalid mode for geometry type: $value." +
                        s" Must be one of ${MosaicJoinType.values.mkString(",")}"
                  )
                )

        def fromId(id: Int): MosaicJoinType.Value =
            MosaicJoinType.values
                .find(_.id == id)
                .getOrElse(throw new IllegalArgumentException(s"Invalid value for mosaic join type id: $id."))

    }

}
