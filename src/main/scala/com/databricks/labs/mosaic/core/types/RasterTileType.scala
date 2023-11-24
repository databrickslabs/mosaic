package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.types._

/** Type definition for the raster tile. */
class RasterTileType(fields: Array[StructField]) extends StructType(fields) {

    def rasterType: DataType = fields(1).dataType

    override def simpleString: String = "RASTER_TILE"

    override def typeName: String = "struct"

}

object RasterTileType {

    /**
      * Creates a new instance of [[RasterTileType]].
      *
      * @param idType
      *   Type of the index ID.
      * @return
      *   An instance of [[RasterTileType]].
      */
    def apply(idType: DataType): RasterTileType = {
        require(Seq(LongType, IntegerType, StringType).contains(idType))
        new RasterTileType(
          Array(
            StructField("index_id", idType),
            StructField("raster", BinaryType),
            StructField("parentPath", StringType),
            StructField("driver", StringType),
            StructField("seqNo", IntegerType)
          )
        )
    }

}
