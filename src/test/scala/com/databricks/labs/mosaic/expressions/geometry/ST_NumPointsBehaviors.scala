package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{be, contain, convertToAnyShouldWrapper}

trait ST_NumPointsBehaviors extends QueryTest {

    def numPointsBehaviour(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val poly = """POLYGON ((10 10, 110 10, 110 110, 10 110, 10 10),
                     | (20 20, 20 30, 30 30, 30 20, 20 20),
                     | (40 20, 40 30, 50 30, 50 20, 40 20))""".stripMargin.filter(_ >= ' ')

        val line = """LINESTRING (10 10, 10 20, 20 11, 11 30)"""

        val rows = List(
          poly,
          line
        )

        val results = rows
            .toDF("geometry")
            .withColumn("num_points", st_numpoints($"geometry"))
            .select("num_points")
            .as[Int]
            .collect()

        results should contain theSameElementsAs Seq(15, 4)
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)

        val stNumPoints = ST_NumPoints(lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr, geometryAPI.name)

        stNumPoints.child shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr
        stNumPoints.dataType shouldEqual IntegerType
        noException should be thrownBy stNumPoints.makeCopy(stNumPoints.children.toArray)

    }

}
