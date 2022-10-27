package com.databricks.labs.mosaic.test

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.CodegenInterpretedPlanTest
import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem, IndexSystem}
import org.apache.spark.sql.functions.col
import org.scalatest.Tag

abstract class MosaicSpatialQueryTest extends CodegenInterpretedPlanTest with MosaicHelper {

    private val geometryApis = Seq(JTS, ESRI)
    private val indexSystems = Seq(H3IndexSystem, BNGIndexSystem)

    protected def spark: SparkSession

    protected def testAllGeometriesAllIndexSystems(testName: String, testTags: Tag*)(testFun: MosaicContext => Unit): Unit = {
        for (geom <- geometryApis) {
            for (is <- indexSystems) {
                super.test(testName + s" (${geom.name}, ${is.name})", testTags: _*)(
                  withMosaicConf(geom, is) { testFun }
                )
            }
        }
    }

    protected def checkGeometryTopo(
        mc: MosaicContext,
        actualAnswer: DataFrame,
        expectedAnswer: DataFrame,
        geometryFieldName: String
    ): Unit = {
        import mc.functions.st_aswkt

        val actualGeoms = actualAnswer
            .withColumn("answer_wkt", st_aswkt(col(geometryFieldName)))
            .select("answer_wkt")
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))
        val expectedGeoms = expectedAnswer
            .withColumn("answer_wkt", st_aswkt(col(geometryFieldName)))
            .select("answer_wkt")
            .collect()
            .map(mc.getGeometryAPI.geometry(_, "WKT"))

        actualGeoms.zip(expectedGeoms).foreach { case (actualGeom, expectedGeom) =>
            assert(actualGeom.equals(expectedGeom), s"$actualGeom did not topologically equal $expectedGeom")
        }
    }

}

trait MosaicHelper {
    protected def withMosaicConf(geometry: GeometryAPI, indexSystem: IndexSystem)(f: MosaicContext => Unit): Unit = {
        val mc: MosaicContext = MosaicContext.build(indexSystem, geometry)
        f(mc)
    }
}
