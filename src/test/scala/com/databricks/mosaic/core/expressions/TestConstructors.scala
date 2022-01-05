package com.databricks.mosaic.core.constructors

import collection.JavaConversions._
import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}

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

    val xVals = Array(30.0, 40.0, -20.1, 10.0, 30.3)
    val yVals = Array(10.0, 40.0, 40.0, 20.5, -10.2)
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
      "POINT (-20.1 40)",
      "POINT (10 20.5)",
      "POINT (30.3 -10.2)"
    )

    left should contain allElementsOf right

  }
  test("ST_Polygon should create a new polygon InternalGeometryType from a sequence of double pairs.") {
    mosaicContext.register(spark)

    val polygon = Array((30.0, 10.0), (40.0, 40.0), (20.0, 40.0), (10.0, 20.0), (30.0, 10.0))
    val rows = List(Row(polygon.map({case (x: Double, y: Double) => Array(x, y)})))
    val schema = StructType(
        List(
          StructField("Boundary", ArrayType(ArrayType(DoubleType)))
        )
      )

    val left = 
      spark.createDataFrame(rows, schema)
      .withColumn("geom", st_polygon($"Boundary"))
      .select(st_astext($"geom").alias("wkt"))
      .as[String]
      .collect()


    val right = List("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")

    left should contain allElementsOf right

  }
}
