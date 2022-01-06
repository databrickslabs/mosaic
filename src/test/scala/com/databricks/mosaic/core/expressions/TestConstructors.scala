package com.databricks.mosaic.core.constructors

import collection.JavaConversions._
import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{collect_list, explode}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}

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
  test("ST_MakeLine should create a new LineString geometry from an array of Point geometries.") {
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
      .withColumn("points", st_point($"X", $"Y"))
      .groupBy()
      .agg(collect_list($"points").alias("linePoints"))
      .withColumn("lineString", st_makeline($"linePoints"))
      .select(st_astext($"lineString").alias("wkt"))
      .as[String]
      .collect
      .head

    val right = "LINESTRING (30 10, 40 40, -20.1 40, 10 20.5, 30.3 -10.2)"

    left shouldBe right

  }

  test("ST_MakeLine should create a new LineString geometry from a mixed array of geometries including Point, MultiPoint, LineString and MultiLineString.") {
    mosaicContext.register(spark)

    val geometries = List(
      "POINT (30 10)",
      "MULTIPOINT (10 40, 40 30, 20 20, 30 10)",
      "LINESTRING (30 10, 10 30, 40 40)",
      "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"
    )

    val rows = geometries.map(s => Row(s)).toList
    val schema = StructType(List(StructField("wkt", StringType)))

    val left = 
      spark.createDataFrame(rows, schema)
      .withColumn("geom", st_geomfromwkt($"wkt"))
      .groupBy()
      .agg(collect_list($"geom").alias("geoms"))
      .withColumn("lineString", st_makeline($"geoms"))
      .select(st_astext($"lineString").alias("wkt"))
      .as[String]
      .collect
      .head

    val right = "LINESTRING (30 10, 10 40, 40 30, 20 20, 30 10, 30 10, 10 30, 40 40, 10 10, 20 20, 10 40, 40 40, 30 30, 40 20, 30 10)"

    left shouldBe right

  }

  test("ST_MakePolygon should create a new Polygon from a closed LineString geometry.") {
    mosaicContext.register(spark)

    val lineStrings = List(
      "LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)",
      "LINESTRING (35 10, 45 45, 15 40, 10 20, 35 10)"
    )

    val rows = lineStrings.map(s => Row(s)).toList
    val schema = StructType(List(StructField("wkt", StringType)))

    val left = 
      spark.createDataFrame(rows, schema)
      .withColumn("geom", st_geomfromwkt($"wkt"))
      .withColumn("polygon", st_makepolygon($"geom"))
      .select(st_astext($"polygon").alias("wkt"))
      .as[String]
      .collect

    val right = List(
      "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
      "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10))"
    )

    left should contain allElementsOf right

  }

  test("ST_MakePolygon should create a new Polygon from a closed LineString geometry and an array of LineStrings representing holes.") {
    mosaicContext.register(spark)

    val lineStrings = List(
      ("LINESTRING (35 10, 45 45, 15 40, 10 20, 35 10)", List("LINESTRING (20 30, 35 35, 30 20, 20 30)")),
      ("LINESTRING (20 35, 10 30, 10 10, 30 5, 45 20, 20 35)", List("LINESTRING (30 20, 20 15, 20 25, 30 20)", "LINESTRING (35 20, 32 20, 32 18, 35 20)"))
    )

    val rows = lineStrings.map({ case (b: String, h: List[String]) => Row(b, h) }).toList
    val schema = StructType(List(
      StructField("boundaryWkt", StringType),
      StructField("holesWkt", ArrayType(StringType))
    ))

    val left = 
      spark.createDataFrame(rows, schema)
      .withColumn("boundaryGeom", st_geomfromwkt($"boundaryWkt"))
      .withColumn("holeWkt", explode($"holesWkt"))
      .withColumn("holeGeom", st_geomfromwkt($"holeWkt"))
      .groupBy($"boundaryGeom")
      .agg(collect_list($"holeGeom").alias("holeGeoms"))
      .withColumn("polygon", st_makepolygon($"boundaryGeom", $"holeGeoms"))
      .select(st_astext($"polygon").alias("wkt"))
      .as[String]
      .collect
    
    val right = List(
      "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))",
      "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20), (35 20, 32 20, 32 18, 35 20))"
    )

    left should contain allElementsOf right

  }


}
