package com.databricks.mosaic.jts.h3.expressions.index

import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.databricks.mosaic.core.geometry.api.GeometryAPI.JTS
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.getBoroughs
import com.databricks.mosaic.test.SparkFlatSpec

case class TestPolyfill_JTS_H3() extends SparkFlatSpec with Matchers {

    val mosaicContext: MosaicContext = MosaicContext.build(H3IndexSystem, JTS)

    import mosaicContext.functions._

    it should "Polyfill of a WKT polygon" in {
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              polyfill(col("wkt"), 11)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select polyfill(wkt, 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    it should "Polyfill of a WKB polygon" in {
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              polyfill(convert_to(col("wkt"), "wkb"), 11)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select polyfill(convert_to_wkb(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    it should "Polyfill of a HEX polygon" in {
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              polyfill(convert_to(col("wkt"), "hex"), 11)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select polyfill(convert_to_hex(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    it should "Polyfill of a COORDS polygon" in {
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              polyfill(convert_to(col("wkt"), "coords"), 11)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select polyfill(convert_to_coords(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

}
