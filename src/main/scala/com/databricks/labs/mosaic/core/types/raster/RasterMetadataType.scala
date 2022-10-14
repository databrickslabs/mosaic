package com.databricks.labs.mosaic.core.types.raster

import com.databricks.labs.mosaic.core.types._
import org.apache.spark.sql.types._

class RasterMetadataType
    extends StructType(
        Array(
            StructField("xSize", IntegerType),
            StructField("ySize", IntegerType),
            StructField("extent", RasterExtentType),
            StructField("numBands", IntegerType),
            StructField("crs", StringType),
            StructField("bands", ArrayType(RasterBandType))
        )
    ) {

    override def simpleString: String = "RasterMetadata"

    override def typeName: String = "raster_metadata"

}



