package com.databricks.mosaic.ogc.h3.expressions.geometry

import org.scalatest._


import com.databricks.mosaic.test.SparkTest
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.core.geometry.api.GeometryAPI.OGC
import com.databricks.mosaic.core.index.H3IndexSystem


class TestGeometryProcessors_OGC_H3 extends FunSuite with Matchers with SparkTest {
  val mosaicContext: MosaicContext = MosaicContext(H3IndexSystem, OGC)

  import mosaicContext.functions._
  import testImplicits._

  test("Test polygon contains point") {
    mosaicContext.register(spark)
    
    val poly = 
      """POLYGON ((10 10, 110 10, 110 110, 10 110, 10 10),
        | (20 20, 20 30, 30 30, 30 20, 20 20),
        | (40 20, 40 30, 50 30, 50 20, 40 20))""".stripMargin.filter(_ >= ' ')

    val rows = List(
      (poly, "POINT (35 25)", true), 
      (poly, "POINT (25 25)", false)
      )

    val results = 
      rows.toDF("leftGeom", "rightGeom", "expected")
      .withColumn("result", st_contains($"leftGeom", $"rightGeom"))
      .where($"expected" === $"result")

    results.count shouldBe 2

  }

    test("Test convex hull generation") {
    mosaicContext.register(spark)
    val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)")
    val expected = List("POLYGON ((-70 35, -80 35, -80 45, -70 45, -70 35))")

    val results = 
      multiPoint.toDF("multiPoint")
      .withColumn("result", st_convexhull($"multiPoint"))
      .select(st_astext($"result"))
      .as[String]
      .collect()     
    
    results should contain allElementsOf expected
  }
}
