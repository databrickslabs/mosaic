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
