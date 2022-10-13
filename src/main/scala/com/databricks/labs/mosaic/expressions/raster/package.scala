package com.databricks.labs.mosaic.expressions

import org.apache.spark.sql.catalyst.util.{ArrayBasedMapBuilder, ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

package object raster {

    def buildMap(metaData: Map[String, String]): ArrayBasedMapData = {
        val keys = ArrayData.toArrayData(metaData.keys.toArray[String].map(UTF8String.fromString))
        val values = ArrayData.toArrayData(metaData.values.toArray[String].map(UTF8String.fromString))
        val mapBuilder = new ArrayBasedMapBuilder(StringType, StringType)
        mapBuilder.putAll(keys, values)
        mapBuilder.build()
    }
}
