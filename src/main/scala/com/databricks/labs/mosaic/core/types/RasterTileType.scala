package com.databricks.labs.mosaic.core.types

import com.databricks.labs.mosaic.core.types.RasterTileType.getRasterDataType
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

/** Type definition for the raster tile. */
class RasterTileType(fields: Array[StructField], useCheckpoint: Boolean) extends StructType(fields) {

    def rasterType: DataType = getRasterDataType(
        fields.find(_.name == "raster").get.dataType, useCheckpoint)

    override def simpleString: String = "RASTER_TILE"

    override def typeName: String = "struct"

}

object RasterTileType {

    /**
     * Change data type to [[StringType]] if using checkpointing.
     * @param dataType
     *    Data type to use if not checkpointing.
     * @param useCheckpoint
     *    Use to test for checkpointing enabled.
     * @return
     *    Returns [[DataType]] based on checkpointing enabled.
     */
    def getRasterDataType(dataType: DataType, useCheckpoint: Boolean): DataType = {
        // change to StringType if using checkpoint
        dataType match {
            case _: BinaryType if useCheckpoint => StringType
            case t => t
        }
    }

    /**
      * [APPLY-1] - Creates a new instance of [[RasterTileType]] with Array.
      *
      * @param idType
      *   Cellid type, can be one of [[LongType]], [[IntegerType]] or [[StringType]].
      * @param rasterType
      *   Type of the raster. Can be one of [[ByteType]] or [[StringType]]. Not
      *   to be confused with the data type of the raster. This is the type of
      *   the column that contains the raster.
      * @param useCheckpoint
      *    Use to test for checkpointing enabled.
      * @return
      *    Array RasterTileType [[DataType]].
      */
    def apply(idType: DataType, rasterType: DataType, useCheckpoint: Boolean): DataType = {
        require(Seq(LongType, IntegerType, StringType).contains(idType))
        new RasterTileType(
            Array(
                StructField("index_id", idType),
                StructField("raster", getRasterDataType(rasterType, useCheckpoint)),
                StructField("metadata", MapType(StringType, StringType))
            ),
            useCheckpoint
        )
    }

    /**
      * [APPLY-2] - Creates a new instance of [[RasterTileType]].
      *             Internally, calls [APPLY-1].
      *
      * @param idType
      *    Cellid type, can be one of [[LongType]], [[IntegerType]] or [[StringType]].
      * @param tileExpr
      *    Expression containing a tile. This is used to infer the raster type
      *    when chaining expressions; may be an array of tiles.
      * @param useCheckpoint
      *    Use to test for checkpointing enabled.
      * @return
      *    RasterTileType as [[DataType]]
      */
    def apply(idType: DataType, tileExpr: Expression, useCheckpoint: Boolean): DataType = {
        require(Seq(LongType, IntegerType, StringType).contains(idType))
        tileExpr.dataType match {
            case st @ StructType(_)                       =>
                apply(idType, st.find(_.name == "raster").get.dataType, useCheckpoint)
            case _ @ArrayType(elementType: StructType, _) =>
                apply(idType, elementType.find(_.name == "raster").get.dataType, useCheckpoint)
            case _                                        => throw new IllegalArgumentException("Unsupported raster type.")
        }
    }

    /**
      * [APPLY-3] - Creates a new instance of [[RasterTileType]].
      *             Internally, calls class constructor.
      *
      * @param tileExpr
      *   Expression containing a tile. This is used to infer the raster type
      *   when chaining expressions; may be an array of tiles.
      * @param useCheckpoint
      *    Use to test for checkpointing enabled.
      * @return
      * [[RasterTileType]]
      */
    def apply(tileExpr: Expression, useCheckpoint: Boolean): RasterTileType = {
        tileExpr.dataType match {
            case StructType(fields)                    => new RasterTileType(fields, useCheckpoint)
            case ArrayType(elementType: StructType, _) => new RasterTileType(elementType.fields, useCheckpoint)
            case _                                     => throw new IllegalArgumentException("Unsupported raster type.")
        }
    }

}
