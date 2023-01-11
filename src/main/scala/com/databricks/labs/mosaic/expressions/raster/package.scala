package com.databricks.labs.mosaic.expressions

import org.apache.spark.sql.catalyst.util.{ArrayBasedMapBuilder, ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Utility methods for raster expressions. */
package object raster {

    /** Datatype representing pixels in a raster. */
    val PixelCoordsType: DataType = StructType(Seq(StructField("x", IntegerType), StructField("y", IntegerType)))

    /** Datatype representing pixels in a raster. */
    val WorldCoordsType: DataType = StructType(Seq(StructField("x", DoubleType), StructField("y", DoubleType)))

    /**
      * Datatype representing a raster projected to a grid.
      * @param cellIDType
      *   The cell ID type of the index system.
      * @param measureType
      *   The measure type of the resulting pixel value.
      *
      * @return
      *   The datatype to be used for serialization of the result of
      *   [[RasterToGridExpression]].
      */
    def RasterToGridType(cellIDType: DataType, measureType: DataType): DataType = {
        ArrayType(
          ArrayType(
            StructType(
              Seq(StructField("cellID", cellIDType), StructField("measure", measureType))
            )
          )
        )
    }

    /**
      * Builds a spark map from a scala Map[String, String].
      * @param metaData
      *   The metadata to be used.
      * @return
      *   Serialized map.
      */
    def buildMapString(metaData: Map[String, String]): ArrayBasedMapData = {
        val keys = ArrayData.toArrayData(metaData.keys.toArray[String].map(UTF8String.fromString))
        val values = ArrayData.toArrayData(metaData.values.toArray[String].map(UTF8String.fromString))
        val mapBuilder = new ArrayBasedMapBuilder(StringType, StringType)
        mapBuilder.putAll(keys, values)
        mapBuilder.build()
    }

    /**
      * Builds a spark map from a scala Map[String, Double].
      * @param metaData
      *   The metadata to be used.
      * @return
      *   Serialized map.
      */
    def buildMapDouble(metaData: Map[String, Double]): ArrayBasedMapData = {
        val keys = ArrayData.toArrayData(metaData.keys.toArray[String].map(UTF8String.fromString))
        val values = ArrayData.toArrayData(metaData.values.toArray[Double])
        val mapBuilder = new ArrayBasedMapBuilder(StringType, DoubleType)
        mapBuilder.putAll(keys, values)
        mapBuilder.build()
    }

}
