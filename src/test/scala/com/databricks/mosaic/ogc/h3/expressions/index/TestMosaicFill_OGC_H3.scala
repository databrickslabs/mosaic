package com.databricks.mosaic.ogc.h3.expressions.index

import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.databricks.mosaic.core.geometry.api.GeometryAPI.OGC
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.getBoroughs
import com.databricks.mosaic.test.SparkFlatSpec

case class TestMosaicFill_OGC_H3() extends SparkFlatSpec with Matchers {

    val mosaicContext: MosaicContext = MosaicContext.build(H3IndexSystem, OGC)

    import mosaicContext.functions._

    it should "MosaicFill of a WKT polygon" in {
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaicfill(col("wkt"), 11)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaicfill(wkt, 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    it should "MosaicFill of a WKB polygon" in {
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaicfill(convert_to(col("wkt"), "wkb"), 11)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaicfill(convert_to_wkb(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    it should "MosaicFill of a HEX polygon" in {
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaicfill(convert_to(col("wkt"), "hex"), 11)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaicfill(convert_to_hex(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    it should "MosaicFill of a COORDS polygon" in {
        mosaicContext.register(spark)

        val boroughs: DataFrame = getBoroughs

        val mosaics = boroughs
            .select(
              mosaicfill(convert_to(col("wkt"), "coords"), 11)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql("""
                   |select mosaicfill(convert_to_coords(wkt), 11) from boroughs
                   |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

}
