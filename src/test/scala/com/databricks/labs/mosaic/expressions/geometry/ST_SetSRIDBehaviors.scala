package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper, noException}

trait ST_SetSRIDBehaviors extends QueryTest {

    def setSRIDBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
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
            .select(st_setsrid($"geom", lit(refSrid)).alias("geom"))

            // Convert to and from geoJSON should maintain the SRID
            .select(st_asgeojson($"geom").alias("json"))
            .select(st_geomfromgeojson($"json").alias("geom"))

            // Extract the SRID
            .select(st_srid($"geom").alias("srid"))
            .as[Int]
            .collect()

        resultJson should contain only refSrid

        sourceDf.createOrReplaceTempView("source")

        // SQL
        val sqlResult = spark
            .sql(s"select st_srid(st_setsrid(internal, $refSrid)) from source")
            .as[Int]
            .collect()

        sqlResult should contain only refSrid
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stSetSRID = ST_SetSRID(lit("POINT (1 1)").expr, lit(4326).expr, mc.expressionConfig)

        stSetSRID.left shouldEqual lit("POINT (1 1)").expr
        stSetSRID.right shouldEqual lit(4326).expr
        stSetSRID.dataType shouldEqual lit("POINT (1 1)").expr.dataType
        noException should be thrownBy stSetSRID.makeCopy(stSetSRID.children.toArray)

    }

}
