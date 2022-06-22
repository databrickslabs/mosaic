package com.databricks.labs.mosaic.expressions.geometry

import scala.collection.immutable
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.{col, lit}

trait GeometryProcessorsBehaviors { this: AnyFlatSpec =>

    val wktReader = new WKTReader()
    val wktWriter = new WKTWriter()
    val geomFactory = new GeometryFactory()

    def lengthCalculation(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        // TODO break into two for line segment vs. polygons

        val expected = mocks
            .getWKTRowsDf(mc)
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(wkt => mc.getGeometryAPI.geometry(wkt, "WKT").getLength)

        val result = mocks
            .getWKTRowsDf(mc)
            .orderBy("id")
            .select(st_length($"wkt"))
            .as[Double]
            .collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val result2 = mocks
            .getWKTRowsDf(mc)
            .select(st_perimeter($"wkt"))
            .as[Double]
            .collect()

        result2.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        mocks.getWKTRowsDf(mc).createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("select st_length(wkt) from source")
            .as[Double]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        val sqlResult2 = spark
            .sql("select st_perimeter(wkt) from source")
            .as[Double]
            .collect()

        sqlResult2.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def lengthCalculationCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        // TODO break into two for line segment vs. polygons

        val result = mocks
            .getWKTRowsDf(mc)
            .select(st_length($"wkt"))
            .as[Double]

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def areaCalculation(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val expected = mocks
            .getWKTRowsDf(mc)
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(wkt => mc.getGeometryAPI.geometry(wkt, "WKT").getArea)

        val result = mocks
            .getWKTRowsDf(mc)
            .select(st_area($"wkt"))
            .as[Double]
            .collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        mocks.getWKTRowsDf(mc).createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("select st_area(wkt) from source")
            .as[Double]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def areaCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val result = mocks
            .getWKTRowsDf(mc)
            .select(st_area($"wkt"))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def centroid2DCalculation(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val expected = mocks
            .getWKTRowsDf(mc)
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(wkt => mc.getGeometryAPI.geometry(wkt, "WKT").getCentroid)
            .map(c => (c.getX, c.getY))

        val result = mocks
            .getWKTRowsDf(mc)
            .select(st_centroid2D($"wkt").alias("coord"))
            .selectExpr("coord.*")
            .as[(Double, Double)]
            .collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        mocks.getWKTRowsDf(mc).createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("""with subquery (
                   | select st_centroid2D(wkt) as coord from source
                   |) select coord.* from subquery""".stripMargin)
            .as[(Double, Double)]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def centroid2DCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val result = mocks
            .getWKTRowsDf(mc)
            .select(st_centroid2D($"wkt").alias("coord"))
            .selectExpr("coord.*")

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def distanceCalculation(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val referenceGeoms = mocks
            .getWKTRowsDf(mc)
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(wkt => mc.getGeometryAPI.geometry(wkt, "WKT"))

        val coords = referenceGeoms.head.getShells
        val pointsWKT = coords.map(_.toWKT)
        val pointWKTCompared = pointsWKT.zip(pointsWKT.tail)
        val expected = coords.zip(coords.tail).map({ case (a, b) => a.distance(b) })

        val df = pointWKTCompared.toDF("leftGeom", "rightGeom")

        val result = df.select(st_distance($"leftGeom", $"rightGeom")).as[Double].collect()

        result.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

        df.createOrReplaceTempView("source")
        val sqlResult = spark.sql("select st_distance(leftGeom, rightGeom) from source").as[Double].collect()

        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

    def distanceCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val referenceGeoms: immutable.Seq[MosaicGeometry] =
            mocks.wkt_rows_epsg4326.map(_(1).asInstanceOf[String]).map(mc.getGeometryAPI.geometry(_, "WKT"))

        val coords = referenceGeoms.head.getShells
        val df = coords.map(_.toWKT).toDF("point")

        val result = df
            .withColumnRenamed("point", "pointLeft")
            .crossJoin(df.withColumnRenamed("point", "pointRight"))
            .select(st_distance($"pointLeft", st_asbinary($"pointRight")))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def polygonContains(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val poly = """POLYGON ((10 10, 110 10, 110 110, 10 110, 10 10),
                     | (20 20, 20 30, 30 30, 30 20, 20 20),
                     | (40 20, 40 30, 50 30, 50 20, 40 20))""".stripMargin.filter(_ >= ' ')

        val rows = List(
          (poly, "POINT (35 25)", true),
          (poly, "POINT (25 25)", false)
        )

        val results = rows
            .toDF("leftGeom", "rightGeom", "expected")
            .withColumn("result", st_contains($"leftGeom", $"rightGeom"))
            .where($"expected" === $"result")

        results.count shouldBe 2

    }

    def containsCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val poly = """POLYGON ((10 10, 110 10, 110 110, 10 110, 10 10),
                     | (20 20, 20 30, 30 30, 30 20, 20 20),
                     | (40 20, 40 30, 50 30, 50 20, 40 20))""".stripMargin.filter(_ >= ' ')

        val rows = List(
          ("POINT (35 25)", true),
          ("POINT (25 25)", false)
        )

        val polygons = List(poly).toDF("leftGeom")
        val points = rows.toDF("rightGeom", "expected")

        val result = polygons
            .crossJoin(points)
            .withColumn("result", st_contains($"leftGeom", $"rightGeom"))
            .where($"expected" === $"result")

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def convexHullGeneration(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)")
        val expected = List("POLYGON ((-80 35, -80 45, -70 45, -70 35, -80 35))")
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val results = multiPoint
            .toDF("multiPoint")
            .crossJoin(multiPoint.toDF("other"))
            .withColumn("result", st_convexhull($"multiPoint"))
            .select($"result")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        results.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }

    }

    def convexHullCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)").toDF("multiPoint")
        val expected = List("POLYGON ((-80 35, -80 45, -70 45, -70 35, -80 35))").toDF("polygon")

        val result = multiPoint
            .crossJoin(expected)
            .withColumn("result", st_convexhull($"multiPoint"))
            .select(st_asbinary($"result"))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def transformationsCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val multiPoint = List("MULTIPOINT (-70 35, -80 45, -70 45, -80 35)")

        val result = multiPoint
            .toDF("multiPoint")
            .crossJoin(multiPoint.toDF("other"))
            .withColumn("result", st_convexhull($"multiPoint"))
            .select($"result")
            .select(
              st_translate(
                st_scale(
                  st_rotate($"result", lit(1.1)),
                  lit(1.1),
                  lit(1.2)
                ),
                lit(1.2),
                lit(1.3)
              )
            )
            .as[String]

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        result.collect().length > 0 shouldBe true
    }

    def bufferCalculation(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        noException should be thrownBy {
            spark.createDataFrame(Seq((1, "POINT (1 1)")))
                .withColumn("buffered", st_buffer(col("_2"), 1))
                .count()
        }

        val referenceGeoms = mocks.getWKTRowsDf(mc)
            .orderBy("id")
            .select("wkt")
            .as[String]
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        val expected = referenceGeoms.map(_.buffer(1).getLength)
        val result = mocks
            .getWKTRowsDf(mc)
            .orderBy("id")
            .select(st_length(st_buffer($"wkt", lit(1))))
            .as[Double]
            .collect()

        result.zip(expected).foreach { case (l, r) => math.abs(l - r) should be < 1e-8 }

        mocks.getWKTRowsDf(mc).createOrReplaceTempView("source")

        val sqlResult = spark
            .sql("select id, st_length(st_buffer(wkt, 1)) from source")
            .orderBy("id")
            .drop("id")
            .as[Double]
            .collect()

        sqlResult.zip(expected).foreach { case (l, r) => math.abs(l - r) should be < 1e-8 }
    }

    def bufferCodegen(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val result = mocks
            .getWKTRowsDf(mc)
            .select(st_length(st_buffer($"wkt", lit(1))))

        val queryExecution = result.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)
    }

    def auxiliaryMethods(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val df =  mocks
            .getWKTRowsDf(mc)
            .withColumn("geom", st_geomfromwkt(col("wkt")))
            .withColumn("other", st_buffer(col("wkt"), lit(1)))

        val geometryAPIName = mosaicContext.getGeometryAPI.name

        val stLength = ST_Length(df.col("geom").expr, geometryAPIName)
        val stArea = ST_Area(df.col("geom").expr, geometryAPIName)
        val stCentroid2D = ST_Centroid(df.col("geom").expr, geometryAPIName)
        val stDistance = ST_Distance(df.col("geom").expr, df.col("other").expr, geometryAPIName)
        val stContains = ST_Contains(df.col("geom").expr, df.col("other").expr, geometryAPIName)
        val stConvexHull = ST_ConvexHull(df.col("geom").expr, geometryAPIName)
        val stTranslate = ST_Translate(df.col("geom").expr, lit(1).expr, lit(2).expr, geometryAPIName)
        val stRotate = ST_Rotate(df.col("geom").expr, lit(1).expr, geometryAPIName)
        val stScale = ST_Scale(df.col("geom").expr, lit(1).expr, lit(2).expr, geometryAPIName)
        val stBuffer = ST_Buffer(df.col("geom").expr, lit(1).expr, geometryAPIName)

        stLength.child shouldEqual df.col("geom").expr
        stArea.child shouldEqual df.col("geom").expr
        stCentroid2D.child shouldEqual df.col("geom").expr
        stDistance.left shouldEqual df.col("geom").expr
        stDistance.right shouldEqual df.col("other").expr
        stContains.left shouldEqual df.col("geom").expr
        stContains.right shouldEqual df.col("other").expr
        stConvexHull.child shouldEqual df.col("geom").expr
        stTranslate.first shouldEqual df.col("geom").expr
        stTranslate.second shouldEqual lit(1).expr
        stTranslate.third shouldEqual lit(2).expr
        stRotate.left shouldEqual df.col("geom").expr
        stRotate.right shouldEqual lit(1).expr
        stScale.first shouldEqual df.col("geom").expr
        stScale.second shouldEqual lit(1).expr
        stScale.third shouldEqual lit(2).expr
        stBuffer.left shouldEqual df.col("geom").expr
        stBuffer.right shouldEqual lit(1).expr

        noException should be thrownBy stLength.makeCopy(Array(df.col("geom").expr))
        noException should be thrownBy stArea.makeCopy(Array(df.col("geom").expr))
        noException should be thrownBy stCentroid2D.makeCopy(Array(df.col("geom").expr))
        noException should be thrownBy stDistance.makeCopy(Array(df.col("geom").expr, df.col("other").expr))
        noException should be thrownBy stContains.makeCopy(Array(df.col("geom").expr, df.col("other").expr))
        noException should be thrownBy stConvexHull.makeCopy(Array(df.col("geom").expr))
        noException should be thrownBy stTranslate.makeCopy(Array(df.col("geom").expr, lit(1).expr, lit(2).expr))
        noException should be thrownBy stRotate.makeCopy(Array(df.col("geom").expr, lit(1).expr))
        noException should be thrownBy stScale.makeCopy(Array(df.col("geom").expr, lit(1).expr, lit(2).expr))
        noException should be thrownBy stBuffer.makeCopy(Array(df.col("geom").expr, lit(1).expr))

        noException should be thrownBy stLength.withNewChildrenInternal(Seq(df.col("geom").expr).toIndexedSeq)
        noException should be thrownBy stArea.withNewChildrenInternal(Seq(df.col("geom").expr).toIndexedSeq)
        noException should be thrownBy stCentroid2D.withNewChildrenInternal(Seq(df.col("geom").expr).toIndexedSeq)
        noException should be thrownBy stDistance.withNewChildrenInternal(Seq(df.col("geom").expr, df.col("other").expr).toIndexedSeq)
        noException should be thrownBy stContains.withNewChildrenInternal(Seq(df.col("geom").expr, df.col("other").expr).toIndexedSeq)
        noException should be thrownBy stConvexHull.withNewChildrenInternal(Seq(df.col("geom").expr).toIndexedSeq)
        noException should be thrownBy stTranslate.withNewChildrenInternal(Seq(df.col("geom").expr, lit(1).expr, lit(2).expr).toIndexedSeq)
        noException should be thrownBy stRotate.withNewChildrenInternal(Seq(df.col("geom").expr, lit(1).expr).toIndexedSeq)
        noException should be thrownBy stScale.withNewChildrenInternal(Seq(df.col("geom").expr, lit(1).expr, lit(2).expr).toIndexedSeq)
        noException should be thrownBy stBuffer.withNewChildrenInternal(Seq(df.col("geom").expr, lit(1).expr).toIndexedSeq)

    }

}
