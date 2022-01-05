package com.databricks.mosaic.core.constructors

import collection.JavaConversions._
import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import com.databricks.mosaic.core.geometry.api.GeometryAPI.JTS
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.test.SparkTest

class TestConstructors extends FunSuite with SparkTest with Matchers {
  import testImplicits._

  val mosaicContext: MosaicContext = MosaicContext(H3IndexSystem, JTS)
  import mosaicContext.functions._

  test("ST_Point should create a new point InternalGeometryType from two doubles.") {
    mosaicContext.register(spark)

    val xVals = Array(30.0, 40.0, 20.0, 10.0, 30.0)
    val yVals = Array(10.0, 40.0, 40.0, 20.0, 10.0)
    val rows = xVals.zip(yVals).map({ case (x: Double, y: Double) => Row(x, y)}).toList
    val schema = StructType(
        List(
          StructField("X", DoubleType),
          StructField("Y", DoubleType)
        )
      )

    val left = 
      spark.createDataFrame(rows, schema)
      .withColumn("geom", st_point($"X", $"Y"))
      .select(st_astext($"geom").alias("wkt"))
      .as[String]
      .collect()

    val right = List(
      "POINT (30 10)",
      "POINT (40 40)",
      "POINT (20 40)",
      "POINT (10 20)",
      "POINT (30 10)"
    )

    left should contain allElementsOf right

  }
}
