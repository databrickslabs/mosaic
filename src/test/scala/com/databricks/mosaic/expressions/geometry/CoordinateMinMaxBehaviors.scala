package com.databricks.mosaic.expressions.geometry

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.mocks.getWKTRowsDf

trait CoordinateMinMaxBehaviors { this: AnyFlatSpec =>

    def xMin(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf.orderBy("id")
        val results = df
            .select(st_xmin(col("wkt")))
            .as[Double]
            .collect()

        val expected = List(10.0, 0.0, 10.0, 10.0, -75.78033, 10.0, 10.0, 10.0)

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_xmin(wkt) from source")
            .as[Double]
            .collect()

        sqlResults.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def xMax(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf.orderBy("id")
        val results = df.select(st_xmax(col("wkt"))).as[Double].collect()
        val expected = List(40.0, 2.0, 110.0, 45.0, -75.78033, 40.0, 40.0, 40.0)

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_xmax(wkt) from source")
            .as[Double]
            .collect()

        sqlResults.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def yMin(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf.orderBy("id")
        val results = df.select(st_ymin(col("wkt"))).as[Double].collect()
        val expected = List(10.0, 0.0, 10.0, 5.0, 35.18937, 10.0, 10.0, 10.0)

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_ymin(wkt) from source")
            .as[Double]
            .collect()

        sqlResults.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def yMax(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val df = getWKTRowsDf.orderBy("id")
        val results = df.select(st_ymax(col("wkt"))).as[Double].collect()
        val expected = List(40.0, 2.0, 110.0, 60.0, 35.18937, 40.0, 40.0, 40.0)

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResults = spark
            .sql("select st_ymax(wkt) from source")
            .as[Double]
            .collect()

        sqlResults.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

}
