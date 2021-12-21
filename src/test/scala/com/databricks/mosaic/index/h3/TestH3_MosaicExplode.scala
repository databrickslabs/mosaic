package com.databricks.mosaic.index.h3

import com.databricks.mosaic.functions._
import com.databricks.mosaic.mocks.getBoroughs
import com.databricks.mosaic.test.SparkTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.{FunSuite, Matchers}

case class TestH3_MosaicExplode() extends FunSuite with SparkTest with Matchers {

  test("Decomposition of a WKT polygon to h3 mosaics") {
    val boroughs: DataFrame = getBoroughs
    register(spark)

    val mosaics = boroughs.select(
      h3_mosaic_explode(col("wkt"), 11)
    ).collect()

    boroughs.collect().length should be < mosaics.length

    boroughs.createOrReplaceTempView("boroughs")

    val mosaics2 = spark.sql(
      """
        |select h3_mosaic_explode(wkt, 11) from boroughs
        |""".stripMargin).collect()

    boroughs.collect().length should be < mosaics2.length
  }

  test("Decomposition of a WKB polygon to h3 mosaics") {
    val boroughs: DataFrame = getBoroughs
    register(spark)

    val mosaics = boroughs.select(
      h3_mosaic_explode(convert_to(col("wkt"), "wkb"), 11)
    ).collect()

    boroughs.collect().length should be < mosaics.length

    boroughs.createOrReplaceTempView("boroughs")

    val mosaics2 = spark.sql(
      """
        |select h3_mosaic_explode(convert_to_wkb(wkt), 11) from boroughs
        |""".stripMargin).collect()

    boroughs.collect().length should be < mosaics2.length
  }

  test("Decomposition of a HEX polygon to h3 mosaics") {
    val boroughs: DataFrame = getBoroughs
    register(spark)

    val mosaics = boroughs.select(
      h3_mosaic_explode(convert_to(col("wkt"), "hex"), 11)
    ).collect()

    boroughs.collect().length should be < mosaics.length

    boroughs.createOrReplaceTempView("boroughs")

    val mosaics2 = spark.sql(
      """
        |select h3_mosaic_explode(convert_to_hex(wkt), 11) from boroughs
        |""".stripMargin).collect()

    boroughs.collect().length should be < mosaics2.length
  }

  test("Decomposition of a COORDS polygon to h3 mosaics") {
    val boroughs: DataFrame = getBoroughs
    register(spark)

    val mosaics = boroughs.select(
      h3_mosaic_explode(convert_to(col("wkt"), "coords"), 11)
    ).collect()

    boroughs.collect().length should be < mosaics.length

    boroughs.createOrReplaceTempView("boroughs")

    val mosaics2 = spark.sql(
      """
        |select h3_mosaic_explode(convert_to_coords(wkt), 11) from boroughs
        |""".stripMargin).collect()

    boroughs.collect().length should be < mosaics2.length
  }

}
