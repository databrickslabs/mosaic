package com.databricks.labs.mosaic.codegen

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.col

trait ConvertToCodegenBehaviors { this: AnyFlatSpec =>

    def codegenWKBtoWKT(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val hexDf: DataFrame = getHexRowsDf
            .orderBy("id")
            .withColumn("wkb", convert_to(as_hex(col("hex")), "WKB"))
            .select(
              convert_to(col("wkb"), "WKT").alias("wkt")
            )

        val queryExecution = hexDf.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = hexDf
            .as[String]
            .collect()
            .map(mosaicContext.getGeometryAPI.geometry(_, "WKT"))

        val right = getWKTRowsDf
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(mosaicContext.getGeometryAPI.geometry(_, "WKT"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def codegenWKBtoHEX(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val wktDf = getWKTRowsDf
            .orderBy("id")
            .withColumn("wkb", convert_to($"wkt", "wkb"))
            .select(
              convert_to($"wkb", "hex").getItem("hex").alias("hex")
            )

        val queryExecution = wktDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = wktDf
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "HEX"))

        val right = getHexRowsDf
            .orderBy("id")
            .select("hex")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "HEX"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def codegenWKBtoCOORDS(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val wktDf = getWKTRowsDf
            .withColumn("wkb", convert_to($"wkt", "wkb"))
            .select(
              convert_to($"wkb", "coords").alias("coords")
            )

        val queryExecution = wktDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = wktDf
            .collect()
            .map(_.toSeq.head)

        val right = getHexRowsDf
            .withColumn("test", as_hex($"hex"))
            .withColumn("coords", convert_to(as_hex($"hex"), "coords"))
            .select("coords")
            .collect()
            .map(_.toSeq.head)

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def codegenWKBtoGEOJSON(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val wkbDf: DataFrame = getHexRowsDf
            .orderBy("id")
            .select(convert_to(as_hex($"hex"), "WKB").alias("wkb"))
            .select(
              convert_to($"wkb", "geojson").getItem("json").alias("geojson")
            )

        val queryExecution = wkbDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = wkbDf
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "GEOJSON"))

        val right = getGeoJSONDf
            .orderBy("id")
            .select(as_json($"geojson").getItem("json").alias("geojson"))
            .select("geojson")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "GEOJSON"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def codegenWKTtoWKB(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val wktDf: DataFrame = getWKTRowsDf
            .orderBy("id")
            .select(
              convert_to($"wkt", "WKB").alias("wkb")
            )

        val queryExecution = wktDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = wktDf
            .as[Array[Byte]]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKB"))

        val right = getHexRowsDf
            .orderBy("id")
            .withColumn("wkb", convert_to(as_hex($"hex"), "WKB"))
            .select("wkb")
            .as[Array[Byte]]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKB"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenWKTtoHEX(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val wktDf = getWKTRowsDf
            .orderBy("id")
            .select(
              convert_to($"wkt", "hex").getItem("hex").alias("hex")
            )

        val queryExecution = wktDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = wktDf
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "HEX"))

        val right = getHexRowsDf
            .orderBy("id")
            .select("hex")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "HEX"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenWKTtoCOORDS(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val wktDf = getWKTRowsDf
            .orderBy("id")
            .select(
              convert_to($"wkt", "coords").alias("coords")
            )

        val queryExecution = wktDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = wktDf
            .collect()
            .map(_.toSeq.head)

        val right = getHexRowsDf
            .orderBy("id")
            .withColumn("coords", convert_to(as_hex($"hex"), "coords"))
            .select("coords")
            .collect()
            .map(_.toSeq.head)

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenWKTtoGEOJSON(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val wktDf: DataFrame = getWKTRowsDf
            .orderBy("id")
            .select(
              convert_to($"wkt", "geojson").getItem("json").alias("geojson")
            )

        val queryExecution = wktDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = wktDf
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "GEOJSON"))

        val right = getGeoJSONDf
            .orderBy("id")
            .select(as_json($"geojson").getItem("json").alias("geojson"))
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "GEOJSON"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenHEXtoWKB(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val hexDf: DataFrame = getHexRowsDf
            .orderBy("id")
            .withColumn("hex", as_hex($"hex"))
            .select(
              convert_to($"hex", "WKB").alias("wkb")
            )

        val queryExecution = hexDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = hexDf
            .as[Array[Byte]]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKB"))

        val right = getWKTRowsDf
            .orderBy("id")
            .withColumn("wkb", convert_to($"wkt", "WKB"))
            .select("wkb")
            .as[Array[Byte]]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKB"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenHEXtoWKT(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val hexDf: DataFrame = getHexRowsDf
            .orderBy("id")
            .withColumn("hex", as_hex($"hex"))
            .select(
              convert_to($"hex", "wkt").alias("wkt").cast("string")
            )

        val queryExecution = hexDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = hexDf
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val right = getWKTRowsDf
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenHEXtoCOORDS(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val hexDf: DataFrame = getHexRowsDf
            .orderBy("id")
            .withColumn("hex", as_hex($"hex"))
            .select(
              convert_to($"hex", "coords").alias("coords")
            )

        val queryExecution = hexDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = hexDf
            .collect()
            .map(_.toSeq.head)

        val right = getWKTRowsDf
            .orderBy("id")
            .withColumn("coords", convert_to($"wkt", "coords"))
            .select("coords")
            .collect()
            .map(_.toSeq.head)

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenHEXtoGEOJSON(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val hexDf: DataFrame = getHexRowsDf
            .orderBy("id")
            .withColumn("hex", as_hex($"hex"))
            .select(
              convert_to($"hex", "geojson").getItem("json").alias("geojson")
            )

        val queryExecution = hexDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = hexDf
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "GEOJSON"))

        val right = getGeoJSONDf
            .orderBy("id")
            .select("geojson")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "GEOJSON"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenCOORDStoWKB(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val hexDf: DataFrame = getHexRowsDf
            .orderBy("id")
            .withColumn("coords", convert_to(as_hex($"hex"), "coords"))
            .select(
              convert_to($"coords", "WKB").alias("wkb")
            )

        val queryExecution = hexDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = hexDf
            .as[Array[Byte]]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKB"))

        val right = getWKTRowsDf
            .orderBy("id")
            .withColumn("wkb", convert_to($"wkt", "WKB"))
            .select("wkb")
            .as[Array[Byte]]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKB"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenCOORDStoWKT(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val hexDf: DataFrame = getHexRowsDf
            .orderBy("id")
            .withColumn("coords", convert_to(as_hex($"hex"), "coords"))
            .select(
              convert_to($"coords", "WKT").alias("wkt")
            )

        val queryExecution = hexDf.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = hexDf
            .as[String]
            .collect()
            .map(mosaicContext.getGeometryAPI.geometry(_, "WKT"))

        val right = getWKTRowsDf
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(mosaicContext.getGeometryAPI.geometry(_, "WKT"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def codegenCOORDStoHEX(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val wktDf = getWKTRowsDf
            .orderBy("id")
            .withColumn("coords", convert_to($"wkt", "coords"))
            .select(
              convert_to($"coords", "hex").getItem("hex").alias("hex")
            )

        val queryExecution = wktDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = wktDf
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "HEX"))

        val right = getHexRowsDf
            .orderBy("id")
            .select("hex")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "HEX"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenCOORDStoGEOJSON(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val wkbDf: DataFrame = getHexRowsDf
            .orderBy("id")
            .withColumn("coords", convert_to(as_hex($"hex"), "coords"))
            .select(
              convert_to($"coords", "geojson").getItem("json").alias("geojson")
            )

        val queryExecution = wkbDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = wkbDf
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "GEOJSON"))

        val right = getGeoJSONDf
            .orderBy("id")
            .select(as_json($"geojson").getItem("json").alias("geojson"))
            .select("geojson")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "GEOJSON"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def codegenGEOJSONtoWKB(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val hexDf: DataFrame = getHexRowsDf
            .orderBy("id")
            .withColumn("geojson", convert_to(as_hex($"hex"), "geojson"))
            .select(
              convert_to($"geojson", "WKB").alias("wkb")
            )

        val queryExecution = hexDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = hexDf
            .as[Array[Byte]]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKB"))

        val right = getWKTRowsDf
            .orderBy("id")
            .withColumn("wkb", convert_to($"wkt", "WKB"))
            .select("wkb")
            .as[Array[Byte]]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKB"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenGEOJSONtoWKT(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val hexDf: DataFrame = getHexRowsDf
            .orderBy("id")
            .withColumn("geojson", convert_to(as_hex($"hex"), "geojson"))
            .select(
              convert_to($"geojson", "wkt").alias("wkt").cast("string")
            )

        val queryExecution = hexDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = hexDf
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val right = getWKTRowsDf
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenGEOJSONtoHEX(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val hexDf = getHexRowsDf
            .orderBy("id")
            .withColumn("geojson", convert_to(as_hex($"hex"), "geojson"))
            .select(
              convert_to($"geojson", "hex").getItem("hex").alias("hex")
            )

        val queryExecution = hexDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = hexDf
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "HEX"))

        val right = getHexRowsDf
            .orderBy("id")
            .select("hex")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "HEX"))

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def codegenGEOJSONtoCOORDS(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._

        val hexDf: DataFrame = getHexRowsDf
            .orderBy("id")
            .where(!st_geometrytype(as_hex($"hex")).isin("MultiLineString", "MultiPolygon"))
            .withColumn("geojson", convert_to(as_hex($"hex"), "geojson"))
            .select(
              convert_to($"geojson", "coords").alias("coords")
            )

        val queryExecution = hexDf.queryExecution
        val plan = queryExecution.executedPlan

        plan.find(_.isInstanceOf[WholeStageCodegenExec]).isDefined shouldBe true

        val codeGenStage = plan.find(_.isInstanceOf[WholeStageCodegenExec]).get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val left = hexDf
            .collect()
            .map(_.toSeq.head)

        val right = getWKTRowsDf
            .orderBy("id")
            .where(!st_geometrytype($"wkt").isin("MultiLineString", "MultiPolygon"))
            .withColumn("coords", convert_to($"wkt", "coords"))
            .select("coords")
            .collect()
            .map(_.toSeq.head)

        right.zip(left).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

}
