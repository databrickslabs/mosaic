package com.databricks.mosaic

import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

import com.databricks.mosaic.core.types._

package object sql {

    // noinspection ScalaStyle
    object constants {

        val geometryColumnEncodings: Map[DataType, String] =
            Map(
              StringType -> "WKT",
              BinaryType -> "WKB",
              HexType -> "HEX",
              InternalGeometryType -> "COORDS",
              JSONType -> "GEOJSON",
              KryoType -> "KRYO"
            )

        object ColMetaTags {

            val ROLE = "Role"
            val GEOMETRY_ID = "GeometryId"
            val FOCAL_GEOMETRY_FLAG = "FocalGeometry"
            val GEOMETRY_ENCODING = "GeometryEncoding"
            val GEOMETRY_TYPE_ID = "GeometryTypeId"
            val GEOMETRY_TYPE_DESCRIPTION = "GeometryTypeDescription"
            val INDEX_ID = "IndexId"
            val INDEX_SYSTEM = "IndexSystem"
            val INDEX_RESOLUTION = "IndexResolution"
            val PARENT_GEOMETRY_ID = "ParentGeometryId"

        }

        object ColRoles {

            val GEOMETRY = "Geometry"
            val INDEX = "Index"
            val CHIP = "Chip"
            val CHIP_FLAG = "ChipFlag"

            val AUXILIARIES = List(INDEX, CHIP, CHIP_FLAG)

        }

        object DefaultColNames {

            val defaultFlatGeometrySuffix = "flattened"
            val defaultPointIndexColumnName = "point_index"
            val defaultFillIndexColumnName = "fill_index"
            val defaultChipFlagColumnName = "is_core"
            val defaultChipColumnName = "chip_geometry"

        }

    }

}
