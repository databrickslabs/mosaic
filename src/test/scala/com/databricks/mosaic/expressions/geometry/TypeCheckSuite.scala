package com.databricks.mosaic.expressions.geometry

import org.scalatest.{FunSuite, Matchers}
import com.databricks.mosaic.test.SparkTest
import com.databricks.mosaic.mocks.{getWKTRowsDf, getHexRowsDf}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import com.databricks.mosaic.functions.{as_hex, st_geometrytype}

class TypeCheckSuite extends FunSuite with Matchers with SparkTest {
  test(
    "ST_GeometryType returns the correct geometry type string for WKT geometries"
  ) {
    val df = getWKTRowsDf.withColumn("result", st_geometrytype(col("wkt")))
    val results = df.collect().map(_.getString(1)).toList
    val expected = List("POLYGON", "POLYGON", "MULTIPOLYGON", "MULTIPOLYGON")
    results should contain theSameElementsAs expected
  }
  test(
    "ST_GeometryType returns the correct geometry type string for hex-encoded WKB geometries"
  ) {
    val df =
      getHexRowsDf.withColumn("result", st_geometrytype(as_hex(col("hex"))))
    val results = df.collect().map(_.getString(1)).toList
    val expected = List("POLYGON", "MULTIPOLYGON")
    results should contain theSameElementsAs expected
  }
}
