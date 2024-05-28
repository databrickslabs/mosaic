package com.databricks.labs.mosaic.expressions.format

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

trait ConvertToBehaviors extends QueryTest {

    def checkInputTypeBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)

        val wkts = getWKTRowsDf().select("wkt")
        val wkbs = getWKTRowsDf().select(st_aswkb(col("wkt")).alias("wkb"))
        val hexes = getHexRowsDf(mc).select("hex")
        val geojsons = getGeoJSONDf(mc).select("geojson")
        val coords = getWKTRowsDf().select(st_geomfromwkt(col("wkt")).alias("coords"))

        val wkbExpr = wkbs.col("wkb").expr
        wkbExpr.checkInputDataTypes() shouldEqual TypeCheckSuccess

        ConvertTo(wkbs.col("wkb").expr, "WKT", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(wkbs.col("wkb").expr, "WKB", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(wkbs.col("wkb").expr, "COORDS", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(wkbs.col("wkb").expr, "HEX", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(wkbs.col("wkb").expr, "GEOJSON", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(wkts.col("wkt").expr, "WKT", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(wkts.col("wkt").expr, "WKB", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(wkts.col("wkt").expr, "COORDS", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(wkts.col("wkt").expr, "HEX", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(wkts.col("wkt").expr, "GEOJSON", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(hexes.col("hex").expr, "WKT", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(hexes.col("hex").expr, "WKB", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(hexes.col("hex").expr, "COORDS", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(hexes.col("hex").expr, "HEX", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(hexes.col("hex").expr, "GEOJSON", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(geojsons.col("geojson").expr, "WKT", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(geojsons.col("geojson").expr, "WKB", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(geojsons.col("geojson").expr, "COORDS", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(geojsons.col("geojson").expr, "HEX", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(geojsons.col("geojson").expr, "GEOJSON", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(coords.col("coords").expr, "WKT", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(coords.col("coords").expr, "WKB", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(coords.col("coords").expr, "COORDS", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(coords.col("coords").expr, "HEX", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(coords.col("coords").expr, "GEOJSON", geometryAPI.name).checkInputDataTypes() shouldEqual TypeCheckSuccess
        ConvertTo(lit(1).expr, "GEOJSON", geometryAPI.name).checkInputDataTypes().isFailure shouldEqual true
        an[Error] should be thrownBy ConvertTo(coords.col("coords").expr, "ERROR", geometryAPI.name)
            .checkInputDataTypes()

    }

    def passthroughBehavior(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        import mc.functions._
        mc.register(spark)
        val wkts = getWKTRowsDf().select("wkt").withColumn("new_wkt", st_astext(col("wkt"))).where("new_wkt == wkt")
        wkts.count() should be > 0L
    }

    def auxiliaryMethods(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        mc.register(spark)
        val wkts = getWKTRowsDf().select("wkt")
        val expr = ConvertTo(wkts.col("wkt").expr, "WKT", geometryAPI.name)
        expr.makeCopy(expr.children.toArray) shouldEqual expr
    }

}
