package com.databricks.mosaic.index

import com.databricks.mosaic.functions._
import com.databricks.mosaic.mocks.getBoroughs
import com.databricks.mosaic.test.SparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.{FunSuite, Matchers}

case class TestH3_Polyfill() extends FunSuite with SparkTest with Matchers {

  test("H3 Polyfill of a WKT polygon") {
    val boroughs: DataFrame = getBoroughs
    register(spark)

    val mosaics = boroughs.select(
      h3_polyfill(col("wkt"), 11)
    ).collect()

    boroughs.collect().length shouldEqual mosaics.length

    boroughs.createOrReplaceTempView("boroughs")

    val mosaics2 = spark.sql(
      """
        |select h3_polyfill(wkt, 11) from boroughs
        |""".stripMargin).collect()

    boroughs.collect().length shouldEqual mosaics2.length
  }

  test("H3 Polyfill  of a WKB polygon") {
    val boroughs: DataFrame = getBoroughs
    register(spark)

    val mosaics = boroughs.select(
      h3_polyfill(convert_to(col("wkt"), "wkb"), 11)
    ).collect()

    boroughs.collect().length shouldEqual mosaics.length

    boroughs.createOrReplaceTempView("boroughs")

    val mosaics2 = spark.sql(
      """
        |select h3_polyfill(convert_to_wkb(wkt), 11) from boroughs
        |""".stripMargin).collect()

    boroughs.collect().length shouldEqual mosaics2.length
  }

  test("H3 Polyfill  of a HEX polygon") {
    val boroughs: DataFrame = getBoroughs
    register(spark)

    val mosaics = boroughs.select(
      h3_polyfill(convert_to(col("wkt"), "hex"), 11)
    ).collect()

    boroughs.collect().length shouldEqual mosaics.length

    boroughs.createOrReplaceTempView("boroughs")

    val mosaics2 = spark.sql(
      """
        |select h3_polyfill(convert_to_hex(wkt), 11) from boroughs
        |""".stripMargin).collect()

    boroughs.collect().length shouldEqual mosaics2.length
  }

  test("H3 Polyfill of a COORDS polygon") {
    val boroughs: DataFrame = getBoroughs
    register(spark)

    val mosaics = boroughs.select(
      h3_polyfill(convert_to(col("wkt"), "coords"), 11)
    ).collect()

    boroughs.collect().length shouldEqual mosaics.length

    boroughs.createOrReplaceTempView("boroughs")

    val mosaics2 = spark.sql(
      """
        |select h3_polyfill(convert_to_coords(wkt), 11) from boroughs
        |""".stripMargin).collect()

    boroughs.collect().length shouldEqual mosaics2.length
  }

}
