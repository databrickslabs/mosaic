package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

trait ConversionBehaviors { this: AnyFlatSpec =>

    def conversion_expressions(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        mosaicContext.register(spark)

        val wkt = """POINT (0 0)"""

        val df = Seq(
          Row(wkt)
        )

        val rdd = spark.sparkContext.parallelize(df)

        val testSchema = StructType(
          Seq(
            StructField("wkt", StringType)
          )
        )

        val sourceDf = spark.createDataFrame(rdd, testSchema)

        val result = sourceDf
            .withColumn("geom", expr("st_geomfromwkt(wkt)")) // WKT -> Geom
            .withColumn("wkb", expr("st_aswkb(geom)")) // Geom -> WKB
            .withColumn("geom", expr("st_geomfromwkb(wkb)")) // WKB -> Geom
            .withColumn("geojson", expr("st_asgeojson(geom)")) // Geom -> GeoJSON
            .withColumn("geom", expr("st_geomfromgeojson(geojson)")) // GeoJSON -> Geom
            .withColumn("new_wkt", expr("st_aswkt(geom)")) // Geom -> WKT
            .select("new_wkt")
            .collect()

        result.toSet shouldEqual Set(Row(wkt))
    }

    def conversion_functions(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val wkt = """POINT (0 0)"""

        val df = Seq(
          Row(wkt)
        )

        val rdd = spark.sparkContext.parallelize(df)

        val testSchema = StructType(
          Seq(
            StructField("wkt", StringType)
          )
        )

        val sourceDf = spark.createDataFrame(rdd, testSchema)

        val result = sourceDf
            .withColumn("geom", st_geomfromwkt(col("wkt"))) // WKT -> Geom
            .withColumn("wkb", st_aswkb(col("geom"))) // Geom -> WKB
            .withColumn("geom", st_geomfromwkb(col("wkb"))) // WKB -> Geom
            .withColumn("geojson", st_asgeojson(col("geom"))) // Geom -> GeoJSON
            .withColumn("geom", st_geomfromgeojson(col("geojson"))) // GeoJSON -> Geom
            .withColumn("new_wkt", st_aswkt(col("geom"))) // Geom -> WKT
            .select("new_wkt")
            .collect()

        result.toSet shouldEqual Set(Row(wkt))
    }

}
