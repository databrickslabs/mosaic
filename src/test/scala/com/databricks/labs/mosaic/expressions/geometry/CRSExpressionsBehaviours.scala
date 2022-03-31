package com.databricks.labs.mosaic.expressions.geometry

import scala.collection.JavaConverters._
import scala.collection.immutable

import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}

trait CRSExpressionsBehaviours { this: AnyFlatSpec =>

    val wktReader = new WKTReader()
    val wktWriter = new WKTWriter()
    val geomFactory = new GeometryFactory()

    def extractSRID(mosaicContext: => MosaicContext, spark: => SparkSession): Unit = {
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

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
            .where(!st_geometrytype($"json").isin("MultiLineString", "MultiPolygon"))

        val result = sourceDf // ESRI GeoJSON issue
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
//
//        sqlResult.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
//
//        val sqlResult2 = spark
//            .sql("select st_perimeter(wkt) from source")
//            .as[Double]
//            .collect()
//
//        sqlResult2.zip(expected).foreach { case (l, r) => l.equals(r) shouldEqual true }
    }

}
