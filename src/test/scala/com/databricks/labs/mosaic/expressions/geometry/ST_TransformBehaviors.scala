package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper, noException}

import scala.collection.JavaConverters._

trait ST_TransformBehaviors extends QueryTest {

    val wktReader = new WKTReader()
    val wktWriter = new WKTWriter()
    val geomFactory = new GeometryFactory()

    def reprojectGeometries(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val fromSrid = 4326
        val toSrid = 3857

        val sourceDf = testData(spark)
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

    def testData(spark: SparkSession): DataFrame = {
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
        sourceDf
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stTransform = ST_Transform(lit("POINT (1 1)").expr, lit(4326).expr, mc.expressionConfig)

        stTransform.left shouldEqual lit("POINT (1 1)").expr
        stTransform.right shouldEqual lit(4326).expr
        stTransform.dataType shouldEqual lit("POINT (1 1)").expr.dataType
        noException should be thrownBy stTransform.makeCopy(stTransform.children.toArray)
    }

}
