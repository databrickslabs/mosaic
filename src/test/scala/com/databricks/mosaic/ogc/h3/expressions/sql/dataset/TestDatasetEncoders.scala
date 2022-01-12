package com.databricks.mosaic.ogc.h3.expressions.sql.dataset

import com.stephenn.scalatest.jsonassert.JsonMatchers
import org.scalatest.Matchers

import org.apache.spark.sql.DataFrame

import com.databricks.mosaic.core.geometry.MosaicGeometryOGC
import com.databricks.mosaic.core.geometry.api.GeometryAPI.OGC
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.{getHexRowsDf, getWKTRowsDf}
import com.databricks.mosaic.test.SparkFunSuite

class TestDatasetEncoders extends SparkFunSuite with Matchers with JsonMatchers {

    val mosaicContext: MosaicContext = MosaicContext.build(H3IndexSystem, OGC)
    import mosaicContext.functions._
    import testImplicits._

    test("Conversion from WKB to WKT") {
        mosaicContext.register(spark)

        val hexDf: DataFrame = getHexRowsDf
            .withColumn("wkb", convert_to(as_hex($"hex"), "WKB"))
        val wktDf: DataFrame = getWKTRowsDf

        // MosaicFrame(hexDf).toDataset

        val left = hexDf
            .orderBy("id")
            .select(convert_to($"wkb", "WKT").alias("wkt"))
            .as[String]
            .collect()
            .map(MosaicGeometryOGC.fromWKT)

        val right = wktDf
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(MosaicGeometryOGC.fromWKT)

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

        hexDf.createOrReplaceTempView("format_testing_left")
        wktDf.createOrReplaceTempView("format_testing_right")

        val left2 = spark
            .sql("select convert_to_wkt(wkb) as wkt from format_testing_left order by id")
            .as[String]
            .collect()
            .map(MosaicGeometryOGC.fromWKT)

        val right2 = spark
            .sql("select wkt from format_testing_right order by id")
            .as[String]
            .collect()
            .map(MosaicGeometryOGC.fromWKT)

        right2.zip(left2).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val left3 = spark
            .sql("select st_aswkt(wkb) as wkt from format_testing_left order by id")
            .as[String]
            .collect()
            .map(MosaicGeometryOGC.fromWKT)

        right.zip(left3).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val left4 = spark
            .sql("select st_astext(wkb) as wkt from format_testing_left order by id")
            .as[String]
            .collect()
            .map(MosaicGeometryOGC.fromWKT)

        right.zip(left4).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

}
