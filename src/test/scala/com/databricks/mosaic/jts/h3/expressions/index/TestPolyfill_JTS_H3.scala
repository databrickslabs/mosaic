package com.databricks.mosaic.jts.h3.expressions.index

import com.databricks.mosaic.core.geometry.GeometryAPI.JTS
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.getBoroughs
import com.databricks.mosaic.test.SparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.{FunSuite, Matchers}

case class TestPolyfill_JTS_H3() extends FunSuite with SparkTest with Matchers {

  val mosaicContext: MosaicContext = MosaicContext(H3IndexSystem, JTS)

  import mosaicContext.functions._

  test("Polyfill of a WKT polygon") {
    mosaicContext.register(spark)

    val boroughs: DataFrame = getBoroughs

    val mosaics = boroughs.select(
      polyfill(col("wkt"), 11)
    ).collect()

    boroughs.collect().length shouldEqual mosaics.length

    boroughs.createOrReplaceTempView("boroughs")

    val mosaics2 = spark.sql(
      """
        |select polyfill(wkt, 11) from boroughs
        |""".stripMargin).collect()

    boroughs.collect().length shouldEqual mosaics2.length
  }

  test("Polyfill  of a WKB polygon") {
    mosaicContext.register(spark)

    val boroughs: DataFrame = getBoroughs

    val mosaics = boroughs.select(
      polyfill(convert_to(col("wkt"), "wkb"), 11)
    ).collect()

    boroughs.collect().length shouldEqual mosaics.length

    boroughs.createOrReplaceTempView("boroughs")

    val mosaics2 = spark.sql(
      """
        |select polyfill(convert_to_wkb(wkt), 11) from boroughs
        |""".stripMargin).collect()

    boroughs.collect().length shouldEqual mosaics2.length
  }

  test("Polyfill  of a HEX polygon") {
    mosaicContext.register(spark)

    val boroughs: DataFrame = getBoroughs

    val mosaics = boroughs.select(
      polyfill(convert_to(col("wkt"), "hex"), 11)
    ).collect()

    boroughs.collect().length shouldEqual mosaics.length

    boroughs.createOrReplaceTempView("boroughs")

    val mosaics2 = spark.sql(
      """
        |select polyfill(convert_to_hex(wkt), 11) from boroughs
        |""".stripMargin).collect()

    boroughs.collect().length shouldEqual mosaics2.length
  }

  test("Polyfill of a COORDS polygon") {
    mosaicContext.register(spark)

    val boroughs: DataFrame = getBoroughs

    val mosaics = boroughs.select(
      polyfill(convert_to(col("wkt"), "coords"), 11)
    ).collect()

    boroughs.collect().length shouldEqual mosaics.length

    boroughs.createOrReplaceTempView("boroughs")

    val mosaics2 = spark.sql(
      """
        |select polyfill(convert_to_coords(wkt), 11) from boroughs
        |""".stripMargin).collect()

    boroughs.collect().length shouldEqual mosaics2.length
  }

}
