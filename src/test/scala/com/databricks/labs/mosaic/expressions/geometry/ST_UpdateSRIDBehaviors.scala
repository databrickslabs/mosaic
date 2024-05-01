package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper, noException}

trait ST_UpdateSRIDBehaviors extends MosaicSpatialQueryTest {

    def updateSRIDBehaviour(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        val mc = mosaicContext
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val refSrid = 27700

        // Internal format
        val sourceDf = mocks
            .getWKTRowsDf()
            .withColumn("internal", convert_to($"wkt", "COORDS"))

        val result = sourceDf
            .select(st_setsrid($"internal", lit(refSrid)).alias("internal"))
            .select(st_srid($"internal").alias("srid"))
            .as[Int]
            .collect()

        result should contain only refSrid

        // GoeJSON
        val resultJson = sourceDf
            .where(!upper(st_geometrytype($"wkt")).isin("MULTILINESTRING", "MULTIPOLYGON"))
            .select(st_geomfromwkt($"wkt").alias("geom"))
            .select(st_updatesrid($"geom", lit(refSrid), lit(4326)).alias("geom"))

            // Convert to and from geoJSON should maintain the SRID
            .select(st_asgeojson($"geom").alias("json"))
            .select(st_geomfromgeojson($"json").alias("geom"))

            // Extract the SRID
            .select(st_srid($"geom").alias("srid"))
            .as[Int]
            .collect()

        resultJson should contain only 4326

        sourceDf.createOrReplaceTempView("source")

        // SQL
        val sqlResult = spark
            .sql(s"select st_srid(st_updatesrid(internal, $refSrid, 4326)) from source")
            .as[Int]
            .collect()

        sqlResult should contain only 4326
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        val stUpdateSRID = ST_UpdateSRID(lit("POINT (1 1)").expr, lit(4326).expr, lit(27700).expr, mc.expressionConfig)

        stUpdateSRID.first shouldEqual lit("POINT (1 1)").expr
        stUpdateSRID.second shouldEqual lit(4326).expr
        stUpdateSRID.third shouldEqual lit(27700).expr
        stUpdateSRID.dataType shouldEqual lit("POINT (1 1)").expr.dataType
        noException should be thrownBy stUpdateSRID.makeCopy(stUpdateSRID.children.toArray)

    }

}
