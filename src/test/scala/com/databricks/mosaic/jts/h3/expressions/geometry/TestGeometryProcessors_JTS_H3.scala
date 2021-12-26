package com.databricks.mosaic.expressions.geometry
import com.databricks.mosaic.core.geometry.GeometryAPI.JTS
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import org.scalatest._
import com.databricks.mosaic.test.SparkTest
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import com.databricks.mosaic.mocks
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType}

class TestGeometryProcessors_JTS_H3 extends FunSuite with Matchers with SparkTest {

  val mosaicContext: MosaicContext = MosaicContext(H3IndexSystem, JTS)
  import mosaicContext.functions._
  import testImplicits._

  val wktReader = new WKTReader()
  val wktWriter = new WKTWriter()
  val geomFactory = new GeometryFactory()
  val referenceGeoms = mocks.wkt_rows.map(g => wktReader.read(g.head))

  test("Test length (or perimeter) calculation") {
    mosaicContext.register(spark)
    // TODO break into two for line segment vs. polygons

    val expected = referenceGeoms.map(_.getLength)
    val result = mocks.getWKTRowsDf
      .select(st_length($"wkt"))
      .as[Double]
      .collect()
    
    result should contain theSameElementsAs expected

    val result2 = mocks.getWKTRowsDf
      .select(st_perimeter($"wkt"))
      .as[Double]
      .collect()
    
    result2 should contain theSameElementsAs expected

    mocks.getWKTRowsDf.createOrReplaceTempView("source")

    val sqlResult = spark.sql("select st_length(wkt) from source")
      .as[Double]
      .collect()

    sqlResult should contain theSameElementsAs expected

    val sqlResult2 = spark.sql("select st_perimeter(wkt) from source")
      .as[Double]
      .collect()

    sqlResult2 should contain theSameElementsAs expected
  }

  test("Test area calculation") {
    mosaicContext.register(spark)

    val expected = referenceGeoms.map(_.getArea)
    val result = mocks.getWKTRowsDf
      .select(st_area($"wkt"))
      .as[Double]
      .collect()

    result should contain theSameElementsAs expected

    mocks.getWKTRowsDf.createOrReplaceTempView("source")

    val sqlResult = spark.sql("select st_area(wkt) from source")
      .as[Double]
      .collect()

    sqlResult should contain theSameElementsAs expected

  }

  test("Test centroid calculation (2-dimensional)") {
    mosaicContext.register(spark)

    val expected = referenceGeoms.map(_.getCentroid.getCoordinate).map(c => (c.x, c.y))
    val result = mocks.getWKTRowsDf
      .select(st_centroid2D($"wkt").alias("coord"))
      .selectExpr("coord.*")
      .as[Tuple2[Double, Double]]
      .collect()

    result should contain theSameElementsAs expected

    mocks.getWKTRowsDf.createOrReplaceTempView("source")

    val sqlResult = spark
      .sql("""with subquery (
        | select st_centroid2D(wkt) as coord from source
        |) select coord.* from subquery""".stripMargin)
      .as[Tuple2[Double, Double]]
      .collect()

    sqlResult should contain theSameElementsAs expected
  }

  test("Test distance calculation") {
    mosaicContext.register(spark)

    val coords = referenceGeoms.head.getCoordinates()
    val pointsWKT = coords.map(geomFactory.createPoint).map(wktWriter.write)
    val pointWKTCompared = pointsWKT.zip(pointsWKT.tail).toSeq
    val expected = coords.zip(coords.tail).map({ case (a: Coordinate, b: Coordinate) => a.distance(b)})

    val df = pointWKTCompared.toDF("leftGeom", "rightGeom")

    val result = df.select(st_distance($"leftGeom", $"rightGeom")).as[Double].collect()
    
    result should contain allElementsOf expected

    df.createOrReplaceTempView("source")
    val sqlResult = spark.sql("select st_distance(leftGeom, rightGeom) from source").as[Double].collect()

    sqlResult should contain allElementsOf expected

  }
  
}
