package com.databricks.mosaic.jts.h3.expressions.index

import org.scalatest.Matchers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.databricks.mosaic.core.geometry.api.GeometryAPI.JTS
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.getBoroughs
import com.databricks.mosaic.test.SparkFunSuite

case class TestMosaicExplode_JTS_H3() extends SparkFunSuite with Matchers {

    val mosaicContext: MosaicContext = MosaicContext.build(H3IndexSystem, JTS)

    import mosaicContext.functions._

    test("Decomposition of a WKT polygon to mosaics") {
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaic_explode(col("wkt"), 11)
            )
            .collect()

        boroughs.collect().length should be < mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaic_explode(wkt, 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length should be < mosaics2.length
    }

    test("Decomposition of a WKB polygon to mosaics") {
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaic_explode(convert_to(col("wkt"), "wkb"), 11)
            )
            .collect()

        boroughs.collect().length should be < mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaic_explode(convert_to_wkb(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length should be < mosaics2.length
    }

    test("Decomposition of a HEX polygon to mosaics") {
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaic_explode(convert_to(col("wkt"), "hex"), 11)
            )
            .collect()

        boroughs.collect().length should be < mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaic_explode(convert_to_hex(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length should be < mosaics2.length
    }

    test("Decomposition of a COORDS polygon to mosaics") {
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaic_explode(convert_to(col("wkt"), "coords"), 11)
            )
            .collect()

        boroughs.collect().length should be < mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaic_explode(convert_to_coords(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length should be < mosaics2.length
    }

}
