package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{mocks, MosaicSpatialQueryTest}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper, noException}

import scala.collection.JavaConverters._


trait ST_SRIDBehaviors extends MosaicSpatialQueryTest {

    def SRIDBehaviour(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        val mc = mosaicContext
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

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
            .where(!upper(st_geometrytype($"json")).isin("MULTILINESTRING", "MULTIPOLYGON"))

        val result = sourceDf // GeoJSON issue
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

    def SRIDCodegen(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        val mc = mosaicContext
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

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
            .where(!upper(st_geometrytype($"json")).isin("MULTILINESTRING", "MULTIPOLYGON"))

        val results = sourceDf // GeoJSON issue
            .select(st_srid($"json"))
            .as[Int]

        val queryExecution = results.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)

        val stSRID = ST_SRID(lit("POINT (1 1)").expr, mc.expressionConfig)

        stSRID.child shouldEqual lit("POINT (1 1)").expr
        stSRID.dataType shouldEqual IntegerType
        noException should be thrownBy stSRID.makeCopy(stSRID.children.toArray)

    }

}
