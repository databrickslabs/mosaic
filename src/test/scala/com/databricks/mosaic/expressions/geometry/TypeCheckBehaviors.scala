package com.databricks.mosaic.expressions.geometry

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.SparkSession

import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.{getHexRowsDf, getWKTRowsDf}

trait TypeCheckBehaviors {
    this: AnyFlatSpec =>

    def wktTypes(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf

        val results = df
            .select(st_geometrytype($"wkt").alias("result"))
            .as[String]
            .collect()
            .toList
            .sorted
        val expected = List("LINESTRING", "MULTILINESTRING", "MULTIPOINT", "MULTIPOLYGON", "MULTIPOLYGON", "POINT", "POLYGON", "POLYGON")

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_geometrytype(wkt) from source")
            .as[String]
            .collect
            .toList
            .sorted

        sqlResults.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def hexTypes(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getHexRowsDf.select(as_hex($"hex").alias("hex"))

        val results = df
            .select(st_geometrytype($"hex").alias("result"))
            .orderBy("result")
            .as[String]
            .collect()
            .toList
            .sorted

        val expected = List("LINESTRING", "MULTILINESTRING", "MULTIPOINT", "MULTIPOLYGON", "MULTIPOLYGON", "POINT", "POLYGON", "POLYGON")

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_geometrytype(hex) from source")
            .as[String]
            .collect
            .toList
            .sorted

        sqlResults.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

}
