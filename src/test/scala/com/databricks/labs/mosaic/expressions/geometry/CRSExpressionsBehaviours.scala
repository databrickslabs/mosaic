package com.databricks.labs.mosaic.expressions.geometry

import scala.collection.JavaConverters._

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

trait CRSExpressionsBehaviours { this: AnyFlatSpec =>

    val wktReader = new WKTReader()
    val wktWriter = new WKTWriter()
    val geomFactory = new GeometryFactory()

    def extractSRID(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val refSrid = 27700

        val referenceGeoms = mocks.geoJSON_rows
            .map(_(1).asInstanceOf[String])
            .map(mc.getGeometryAPI.geometry(_, "GEOJSON"))

        referenceGeoms
            .foreach(_.setSpatialReference(refSrid))

        val referenceRows = referenceGeoms
            .map(g => Row(g.toJSON))
            .asJava
        val schema = StructType(List(StructField("json", StringType)))

        val sourceDf = spark
            .createDataFrame(referenceRows, schema)
            .select(as_json($"json").alias("json"))
            .where(!st_geometrytype($"json").isin("MultiLineString", "MultiPolygon"))

        val result = sourceDf // ESRI GeoJSON issue
            .select(st_srid($"json"))
            .as[Int]
            .collect()

        result should contain only refSrid

        sourceDf.createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("select st_srid(json) from source")
            .as[Int]
            .collect()

        sqlResult should contain only refSrid

    }

    def assignSRID(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val refSrid = 27700

        val sourceDf = mocks.getWKTRowsDf(mc)
            .withColumn("internal", convert_to($"wkt", "COORDS"))

        val result = sourceDf
            .select(st_setsrid($"internal", lit(refSrid)).alias("internal"))
            .select(st_srid($"internal").alias("srid"))
            .as[Int]
            .collect()

        result should contain only refSrid

        val resultJson = sourceDf
            .select(convert_to($"wkt", "GEOJSON").alias("json"))
            .where(!st_geometrytype($"json").isin("MultiLineString", "MultiPolygon"))
            .select(st_setsrid($"json", lit(refSrid)).alias("json"))
            .select(st_srid($"json").alias("srid"))
            .as[Int]
            .collect()

        resultJson should contain only refSrid

        sourceDf.createOrReplaceTempView("source")

        val sqlResult = spark
            .sql(s"select st_srid(st_setsrid(internal, $refSrid)) from source")
            .as[Int]
            .collect()

        sqlResult should contain only refSrid

    }

    def reprojectGeometries(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val fromSrid = 4326
        val toSrid = 3857

        // Comparison vs. PostGIS
        val testDataWKT = List(
          (
            "POLYGON((30 10,40 40,20 40,10 20,30 10))",
            "POLYGON((3339584.723798207 1118889.9748579594,4452779.631730943 4865942.279503175,2226389.8158654715 4865942.279503175,1113194.9079327357 2273030.926987689,3339584.723798207 1118889.9748579594))"
          ),
          (
            "MULTIPOLYGON(((0 0,0 1,2 2,0 0)))",
            "MULTIPOLYGON(((0 0,0 111325.14286638508,222638.98158654713 222684.20850554403,0 0)))"
          ),
          (
            "MULTIPOLYGON(((40 60,20 45,45 30,40 60)), ((20 35,10 30,10 10,30 5,45 20,20 35), (30 20,20 15,20 25,30 20)))",
            "MULTIPOLYGON(((4452779.631730943 8399737.889818357,2226389.8158654715 5621521.486192066,5009377.085697311 3503549.8435043744,4452779.631730943 8399737.889818357)), ((2226389.8158654715 4163881.144064293,1113194.9079327357 3503549.8435043744,1113194.9079327357 1118889.9748579594,3339584.723798207 557305.2572745767,5009377.085697311 2273030.926987689,2226389.8158654715 4163881.144064293), (3339584.723798207 2273030.926987689,2226389.8158654715 1689200.1396078935,2226389.8158654715 2875744.6243522433,3339584.723798207 2273030.926987689)))"
          ),
          ("POINT(-75.78033 35.18937)", "POINT(-8435827.747746235 4189645.642183593)"),
          (
            "MULTIPOINT(10 40,40 30,20 20,30 10)",
            "MULTIPOINT(1113194.9079327357 4865942.279503175,4452779.631730943 3503549.8435043744,2226389.8158654715 2273030.926987689,3339584.723798207 1118889.9748579594)"
          ),
          (
            "LINESTRING(30 10,10 30,40 40)",
            "LINESTRING(3339584.723798207 1118889.9748579594,1113194.9079327357 3503549.8435043744,4452779.631730943 4865942.279503175)"
          )
        ).map({ case (f: String, t: String) => Row(f, t) })
        val testSchema = StructType(
          Seq(
            StructField("fromWKT", StringType),
            StructField("toWKTRef", StringType)
          )
        )

        val sourceDf = spark
            .createDataFrame(testDataWKT.asJava, testSchema)
            .withColumn("fromInternal", st_setsrid(convert_to($"fromWKT", "COORDS"), lit(fromSrid)))
            .withColumn("toInternalRef", st_setsrid(convert_to($"toWKTRef", "COORDS"), lit(toSrid)))

        val result = sourceDf
            .withColumn("toInternalTest", st_transform($"fromInternal", lit(toSrid)))
            .select(
              st_distance($"toInternalRef", $"toInternalTest"),
              when(st_geometrytype($"toInternalRef") === "POINT", 0)
                  .otherwise(st_area(st_intersection($"toInternalRef", $"toInternalTest"))),
              st_area($"toInternalRef")
            )
            .as[(Double, Double, Double)]
            .collect()

        result.foreach({ case (d, a1, a2) =>
            d should be < 1e-9
            a1 shouldBe a2 +- 1
        })

        sourceDf.createOrReplaceTempView("source")

        val sqlResult = spark
            .sql(s"""
                    |with transformed as (
                    |    select 
                    |        toInternalRef
                    |        , st_transform(fromInternal, $toSrid) as toInternalTest
                    |    from source)
                    |select
                    |    st_distance(toInternalRef, toInternalTest)
                    |    , case st_geometrytype(toInternalRef)
                    |       when "POINT" then 0
                    |       else st_area(st_intersection(toInternalRef, toInternalTest))
                    |       end
                    |   , st_area(toInternalRef)
                    |from transformed
                    |""".stripMargin)
            .as[(Double, Double, Double)]
            .collect()

        sqlResult.foreach({ case (d, a1, a2) =>
            d should be < 1e-9
            a1 shouldBe a2 +- 1
        })

    }

}
