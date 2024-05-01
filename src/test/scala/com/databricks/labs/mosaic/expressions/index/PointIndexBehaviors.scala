package com.databricks.labs.mosaic.expressions.index

import java.nio.file.Files

import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaDeprecation
trait PointIndexBehaviors extends MosaicSpatialQueryTest {

    def behaviorInt(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = 5

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .withColumn("centroid", st_centroid(col("wkt")))
            .select(
              point_index_geom(col("centroid"), resolution),
              point_index_lonlat(st_x(col("centroid")), st_y(col("centroid")), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.withColumn("centroid", st_centroid(col("wkt"))).createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select point_index_geom(centroid, $resolution),
                    |point_index_lonlat(st_x(centroid), st_y(centroid), $resolution)
                    |from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def behaviorString(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => "5"
            case BNGIndexSystem => "100m"
            case _              => "5"
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .withColumn("centroid", st_centroid(col("wkt")))
            .select(
              grid_pointascellid(col("centroid"), lit(resolution)),
              grid_longlatascellid(st_x(col("centroid")), st_y(col("centroid")), lit(resolution))
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.withColumn("centroid", st_centroid(col("wkt"))).createOrReplaceTempView("boroughs")

        val resolution2 = mc.getIndexSystem match {
            case H3IndexSystem  => "5"
            case BNGIndexSystem => "'100m'"
            case _              => "1"
        }

        val mosaics2 = spark
            .sql(s"""
                    |select point_index_geom(centroid, $resolution2),
                    |point_index_lonlat(st_x(centroid), st_y(centroid), $resolution2)
                    |from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)
        import mc.functions._

        val indexSystem = mc.getIndexSystem
        val geometryAPI = mc.getGeometryAPI

        indexSystem match {
            case BNGIndexSystem =>
                val lonLatIndex = PointIndexLonLat(lit(10000.0).expr, lit(10000.0).expr, lit("100m").expr, indexSystem)
                val pointIndex = PointIndexGeom(st_point(lit(10000.0), lit(10000.0)).expr, lit(5).expr, indexSystem, geometryAPI.name)
                lonLatIndex.inputTypes should contain theSameElementsAs Seq(DoubleType, DoubleType, StringType, BooleanType)
                lonLatIndex.dataType shouldEqual StringType
                lonLatIndex
                    .makeCopy(Array(lit(10001.0).expr, lit(10000.0).expr, lit(5).expr))
                    .asInstanceOf[PointIndexLonLat]
                    .first shouldEqual lit(10001.0).expr
                pointIndex.prettyName shouldEqual "grid_pointascellid"
                pointIndex
                    .makeCopy(Array(st_point(lit(10001.0), lit(10000.0)).expr, lit(5).expr, lit(true).expr))
                    .asInstanceOf[PointIndexGeom]
                    .left shouldEqual st_point(lit(10001.0), lit(10000.0)).expr
            case _              =>
                val lonLatIndex = PointIndexLonLat(lit(10.0).expr, lit(10.0).expr, lit(10).expr, indexSystem)
                val pointIndex = PointIndexGeom(st_point(lit(10.0), lit(10.0)).expr, lit(10).expr, indexSystem, geometryAPI.name)
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
                    .left shouldEqual st_point(lit(10001), lit(10000)).expr
        }

        val badExprLonLat = PointIndexLonLat(lit(true).expr, lit(10000.0).expr, lit(5).expr, indexSystem)
        val badExprPoint = PointIndexGeom(lit("POLYGON EMPTY").expr, lit(5).expr, indexSystem, geometryAPI.name)
        an[Error] should be thrownBy badExprLonLat.inputTypes
        an[Exception] should be thrownBy badExprPoint.nullSafeEval(UTF8String.fromString("POLYGON EMPTY"), 5)

        // legacy API def tests
        noException should be thrownBy mc.functions.point_index_geom(lit(""), lit(5))
        noException should be thrownBy mc.functions.point_index_geom(lit(""), 5)
        noException should be thrownBy mc.functions.point_index_lonlat(lit(1), lit(1), lit(5))
        noException should be thrownBy mc.functions.point_index_lonlat(lit(1), lit(1), 5)
    }

    def issue_383(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        mc.register(spark)
        import mc.functions._

        val resolution = 5
        val name = "issue_383"
        val boroughs: DataFrame = getBoroughs(mc)

        val df = boroughs
            .withColumn("geom", st_geomfromwkt(col("wkt")))

        val dbDir = Files.createTempDirectory(name)
        spark.sql(s"DROP DATABASE IF EXISTS ${name} CASCADE")
        spark.sql(s"CREATE DATABASE IF NOT EXISTS ${name} LOCATION '${dbDir}'")
        df.write.saveAsTable(s"${name}.${name}")

        val df2 = spark
            .sql(s"SELECT * FROM ${name}.${name}")
            .select(grid_pointascellid(col("geom"), lit(resolution)))

        df.collect().length shouldEqual df2.collect().length
    }

}
