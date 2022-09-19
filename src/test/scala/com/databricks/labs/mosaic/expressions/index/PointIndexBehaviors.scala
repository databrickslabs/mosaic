package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

trait PointIndexBehaviors extends QueryTest {

    def wktPointIndex(indexSystem: IndexSystem, geometryAPI: GeometryAPI, resolution: Int): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .withColumn("centroid", st_centroid2D(col("wkt")))
            .select(
              point_index_geom(st_point(col("centroid.x"), col("centroid.y")), resolution),
              point_index_lonlat(col("centroid.x"), col("centroid.y"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.withColumn("centroid", st_centroid2D(col("wkt"))).createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select point_index_geom(st_point(centroid.x, centroid.y), $resolution),
                    |point_index_lonlat(centroid.x, centroid.y, $resolution)
                    |from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)
        import mc.functions._

        indexSystem match {
            case BNGIndexSystem =>
                val lonLatIndex = PointIndexLonLat(lit(10000.0).expr, lit(10000.0).expr, lit("100m").expr, lit(false).expr, indexSystem.name)
                val pointIndex =
                    PointIndexGeom(st_point(lit(10000.0), lit(10000.0)).expr, lit(5).expr, lit(false).expr, indexSystem.name, geometryAPI.name)
                lonLatIndex.inputTypes should contain theSameElementsAs Seq(DoubleType, DoubleType, StringType, BooleanType)
                lonLatIndex.dataType shouldEqual StringType
                lonLatIndex
                    .makeCopy(Array(lit(10001.0).expr, lit(10000.0).expr, lit(5).expr, lit(false).expr))
                    .asInstanceOf[PointIndexLonLat]
                    .first shouldEqual lit(10001.0).expr
                pointIndex.prettyName shouldEqual "grid_pointascellid"
                pointIndex
                    .makeCopy(Array(st_point(lit(10001.0), lit(10000.0)).expr, lit(5).expr, lit(true).expr))
                    .asInstanceOf[PointIndexGeom]
                    .first shouldEqual st_point(lit(10001.0), lit(10000.0)).expr
            case H3IndexSystem  =>
                val lonLatIndex = PointIndexLonLat(lit(10.0).expr, lit(10.0).expr, lit(10).expr, lit(true).expr, indexSystem.name)
                val pointIndex =
                    PointIndexGeom(st_point(lit(10.0), lit(10.0)).expr, lit(10).expr, lit(true).expr, indexSystem.name, geometryAPI.name)
                lonLatIndex.inputTypes should contain theSameElementsAs Seq(DoubleType, DoubleType, IntegerType, BooleanType)
                lonLatIndex.dataType shouldEqual LongType
                lonLatIndex
                    .makeCopy(Array(lit(11.0).expr, lit(10.0).expr, lit(10).expr, lit(false).expr))
                    .asInstanceOf[PointIndexLonLat]
                    .first shouldEqual lit(11.0).expr
                pointIndex.prettyName shouldEqual "grid_pointascellid"
                pointIndex
                    .makeCopy(Array(st_point(lit(10001), lit(10000)).expr, lit(5).expr, lit(true).expr))
                    .asInstanceOf[PointIndexGeom]
                    .first shouldEqual st_point(lit(10001), lit(10000)).expr
        }

        val badExprLonLat = PointIndexLonLat(lit(true).expr, lit(10000.0).expr, lit(5).expr, lit(5).expr, indexSystem.name)
        val badExprPoint =
            PointIndexGeom(lit("POLYGON EMPTY").expr, lit(5).expr, lit(true).expr, indexSystem.name, geometryAPI.name)
        an[Error] should be thrownBy badExprLonLat.inputTypes
        an[Error] should be thrownBy badExprLonLat.dataType
        an[Error] should be thrownBy badExprPoint.makeCopy(
          Array(lit("POLYGON EMPTY").expr, lit(5).expr, lit(5).expr)
        ).dataType
        an[Exception] should be thrownBy badExprPoint.nullSafeEval(UTF8String.fromString("POLYGON EMPTY"), 5, true)

        //legacy API def tests
        noException should be thrownBy mc.functions.point_index_geom(lit(""), lit(5))
        noException should be thrownBy mc.functions.point_index_geom(lit(""), 5)
        noException should be thrownBy mc.functions.point_index_lonlat(lit(1), lit(1), lit(5))
        noException should be thrownBy mc.functions.point_index_lonlat(lit(1), lit(1), 5)
    }

}
