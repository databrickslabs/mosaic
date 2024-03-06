package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

/** Type definition for the raster tile. */
class RasterTileType(fields: Array[StructField]) extends StructType(fields) {

    def rasterType: DataType = fields.find(_.name == "raster").get.dataType

    override def simpleString: String = "RASTER_TILE"

    override def typeName: String = "struct"

}

object RasterTileType {

    /**
      * Creates a new instance of [[RasterTileType]].
      *
      * @param idType
      *   Type of the index ID. Can be one of [[LongType]], [[IntegerType]] or
      *   [[StringType]].
      * @param rasterType
      *   Type of the raster. Can be one of [[ByteType]] or [[StringType]]. Not
      *   to be confused with the data type of the raster. This is the type of
      *   the column that contains the raster.
      *
      * @return
      *   An instance of [[RasterTileType]].
      */
    def apply(idType: DataType, rasterType: DataType): DataType = {
        require(Seq(LongType, IntegerType, StringType).contains(idType))
        new RasterTileType(
          Array(
            StructField("index_id", idType),
            StructField("raster", rasterType),
            StructField("metadata", MapType(StringType, StringType))
          )
        )
    }

    /**
      * Creates a new instance of [[RasterTileType]].
      *
      * @param idType
      *   Type of the index ID. Can be one of [[LongType]], [[IntegerType]] or
      *   [[StringType]].
      * @param tileExpr
      *   Expression containing a tile. This is used to infer the raster type
      *   when chaining expressions.
      * @return
      */
    def apply(idType: DataType, tileExpr: Expression): DataType = {
        require(Seq(LongType, IntegerType, StringType).contains(idType))
        tileExpr.dataType match {
            case st @ StructType(_)                       => apply(idType, st.find(_.name == "raster").get.dataType)
            case _ @ArrayType(elementType: StructType, _) => apply(idType, elementType.find(_.name == "raster").get.dataType)
            case _                                        => throw new IllegalArgumentException("Unsupported raster type.")
        }
    }

    def apply(tileExpr: Expression): RasterTileType = {
        tileExpr.dataType match {
            case StructType(fields)                    => new RasterTileType(fields)
            case ArrayType(elementType: StructType, _) => new RasterTileType(elementType.fields)
            case _                                     => throw new IllegalArgumentException("Unsupported raster type.")
        }
    }

}
