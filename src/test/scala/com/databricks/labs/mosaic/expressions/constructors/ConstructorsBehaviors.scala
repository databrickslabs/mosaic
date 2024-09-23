package com.databricks.labs.mosaic.expressions.constructors

import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import scala.collection.JavaConverters._

trait ConstructorsBehaviors { this: AnyFlatSpec =>

    def createST_Point(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val xVals = Array(30.0, 40.0, -20.1, 10.0, 30.3)
        val yVals = Array(10.0, 40.0, 40.0, 20.5, -10.2)
        val rows = xVals.zip(yVals).map({ case (x: Double, y: Double) => Row(x, y) }).toList
        val schema = StructType(
          List(
            StructField("X", DoubleType),
            StructField("Y", DoubleType)
          )
        )

        val left = spark
            .createDataFrame(rows.asJava, schema)
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

    def createST_MakeLineSimple(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val xVals = Array(30.0, 40.0, -20.1, 10.0, 30.3)
        val yVals = Array(10.0, 40.0, 40.0, 20.5, -10.2)
        val rows = xVals.zip(yVals).map({ case (x: Double, y: Double) => Row(x, y) }).toList
        val schema = StructType(
          List(
            StructField("X", DoubleType),
            StructField("Y", DoubleType)
          )
        )

        val left = spark
            .createDataFrame(rows.asJava, schema)
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

    def createST_MakeLineComplex(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val geometries = List(
          "POINT (30 10)",
          "MULTIPOINT (10 40, 40 30, 20 20, 30 10)",
          "LINESTRING (30 10, 10 30, 40 40)",
          "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"
        )

        val rows = geometries.map(s => Row(s))
        val schema = StructType(List(StructField("wkt", StringType)))

        val left = spark
            .createDataFrame(rows.asJava, schema)
            .groupBy()
            .agg(collect_list($"wkt").alias("geoms"))
            .withColumn("lineString", st_makeline($"geoms"))
            .select(col("lineString"))
            .as[String]
            .collect
            .head

        val right = "LINESTRING (30 10, 10 40, 40 30, 20 20, 30 10, 30 10, 10 30, 40 40, 10 10, 20 20, 10 40, 40 40, 30 30, 40 20, 30 10)"

        left shouldBe right
    }

    def createST_MakeLineAnyNull(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val geometries = List(
            "POINT (30 10)",
            "MULTIPOINT (10 40, 40 30, 20 20, 30 10)",
            "LINESTRING (30 10, 10 30, 40 40)",
            "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"
        )

        val rows = geometries.map(s => Row(s))
        val schema = StructType(List(StructField("wkt", StringType)))

        val left = spark
            .createDataFrame(rows.asJava, schema)
            .withColumn("geom", st_geomfromwkt($"wkt"))
            .groupBy()
            .agg(collect_list($"geom").alias("geoms"))
            .withColumn("lineString", st_makeline(array_union($"geoms", array(lit(null)))))
            .select(st_astext($"lineString").alias("wkt"))
            .as[String]
            .collect
            .head

        left shouldBe null
    }

    def createST_MakePolygonNoHoles(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val lineStrings = List(
          "LINESTRING (30 10, 40 40, 20 40, 10 20, 30 10)",
          "LINESTRING (35 10, 45 45, 15 40, 10 20, 35 10)"
        )

        val rows = lineStrings.map(s => Row(s))
        val schema = StructType(List(StructField("wkt", StringType)))

        val left = spark
            .createDataFrame(rows.asJava, schema)
            .withColumn("geom", st_geomfromwkt($"wkt"))
            .withColumn("polygon", st_makepolygon($"geom"))
            .select(st_astext($"polygon").alias("wkt"))
            .as[String]
            .collect
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val right = List(
          "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
          "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10))"
        ).map(mc.getGeometryAPI.geometry(_, "WKT"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def createST_MakePolygonWithHoles(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val lineStrings = List(
          ("LINESTRING (35 10, 45 45, 15 40, 10 20, 35 10)", List("LINESTRING (20 30, 35 35, 30 20, 20 30)")),
          (
            "LINESTRING (20 35, 10 30, 10 10, 30 5, 45 20, 20 35)",
            List("LINESTRING (30 20, 20 15, 20 25, 30 20)", "LINESTRING (35 20, 32 20, 32 18, 35 20)")
          )
        )

        val rows = lineStrings.map({ case (b: String, h: List[String]) => Row(b, h) })
        val schema = StructType(
          List(
            StructField("boundaryWkt", StringType),
            StructField("holesWkt", ArrayType(StringType))
          )
        )

        val left = spark
            .createDataFrame(rows.asJava, schema)
            .withColumn("boundaryGeom", st_geomfromwkt($"boundaryWkt"))
            .withColumn("holeWkt", explode($"holesWkt"))
            .withColumn("holeGeom", st_geomfromwkt($"holeWkt"))
            .groupBy($"boundaryGeom")
            .agg(collect_list($"holeGeom").alias("holeGeoms"))
            .withColumn("polygon", st_makepolygon($"boundaryGeom", $"holeGeoms"))
            .select(st_astext($"polygon").alias("wkt"))
            .as[String]
            .collect
            .map(mc.getGeometryAPI.geometry(_, "WKT"))
            .sortBy(_.getArea)

        val right = List(
          "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))",
          "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20), (35 20, 32 20, 32 18, 35 20))"
        ).map(mc.getGeometryAPI.geometry(_, "WKT")).sortBy(_.getArea)

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

}
