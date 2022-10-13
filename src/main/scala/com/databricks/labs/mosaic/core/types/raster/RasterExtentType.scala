package com.databricks.labs.mosaic.core.types.raster

import org.apache.spark.sql.types._

class RasterExtentType
    extends StructType(
        Array(
            StructField("minX", DoubleType),
            StructField("minY", DoubleType),
            StructField("maxX", DoubleType),
            StructField("maxY", DoubleType)
        )
    ) {

    override def simpleString: String = "RasterExtent"

    override def typeName: String = "raster_extent"

}