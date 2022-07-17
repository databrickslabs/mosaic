package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions._
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper, noException}

trait ST_SetSRIDBehaviors extends QueryTest {

    def setSRIDBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val sc = spark
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val refSrid = 27700

        val sourceDf = mocks
            .getWKTRowsDf(mc)
            .withColumn("internal", convert_to($"wkt", "COORDS"))

        val result = sourceDf
            .select(st_setsrid($"internal", lit(refSrid)).alias("internal"))
            .select(st_srid($"internal").alias("srid"))
            .as[Int]
            .collect()

        result should contain only refSrid

        val resultJson = sourceDf
            .select(convert_to($"wkt", "GEOJSON").alias("json"))
            .where(!upper(st_geometrytype($"json")).isin("MULTILINESTRING", "MULTIPOLYGON"))
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

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stSetSRID = ST_SetSRID(lit("POINT (1 1)").expr, lit(4326).expr, "illegalAPI")

        stSetSRID.left shouldEqual lit("POINT (1 1)").expr
        stSetSRID.right shouldEqual lit(4326).expr
        stSetSRID.dataType shouldEqual lit("POINT (1 1)").expr.dataType
        noException should be thrownBy stSetSRID.makeCopy(stSetSRID.children.toArray)

    }

}