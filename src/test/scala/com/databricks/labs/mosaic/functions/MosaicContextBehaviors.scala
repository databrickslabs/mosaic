package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic._
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.test._
import org.apache.spark.sql.adapters.Column
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

import java.net.URI

trait MosaicContextBehaviors extends MosaicSpatialQueryTest {

    import MosaicContextBehaviors._

    // noinspection EmptyParenMethodAccessedAsParameterless
    def creationOfContext(mosaicContext: MosaicContext): Unit = {
        val indexSystem = mosaicContext.getIndexSystem
        val geometryAPI = mosaicContext.getGeometryAPI
        spark.sparkContext.setLogLevel("FATAL")
        MosaicContext.reset()
        an[Error] should be thrownBy MosaicContext.context
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        MosaicContext.indexSystem shouldEqual indexSystem
        MosaicContext.geometryAPI shouldEqual geometryAPI
        MosaicContext.indexSystem match {
            case BNGIndexSystem => mc.getIndexSystem.getCellIdDataType shouldEqual StringType
            case H3IndexSystem  => mc.getIndexSystem.getCellIdDataType shouldEqual LongType
            case _              => mc.getIndexSystem.getCellIdDataType shouldEqual LongType
        }
        an[Error] should be thrownBy mc.setCellIdDataType("binary")
    }

    def sqlRegistration(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        val indexSystem = mc.getIndexSystem
        import mc.functions._
        mc.register()

        val registry = spark.sessionState.functionRegistry
        def getFunc(name: String): registry.FunctionBuilder = registry.lookupFunctionBuilder(FunctionIdentifier(name)).get

        val pointWkt = Literal("POINT (1 1 1)")
        val pointsWkt = array(lit("POINT (1 1)"), lit("POINT (1 2)"), lit("POINT (2 3)")).expr
        val polygonPointsWkt = array(lit("POINT (1 1)"), lit("POINT (1 2)"), lit("POINT (2 3)"), lit("POINT (1 1)")).expr
        val holePointsWkt = array(lit("POINT (1.1 1.1)"), lit("POINT (1.1 1.9)"), lit("POINT (1.1 1.1)")).expr
        val multiPolygon = lit("MULTIPOLYGON ( (((1 1) (1 2) (2 1) (1 1))) (((11 1) (11 2) (2 1) (11 1))) (((21 1) (21 2) (2 1) (21 1))))")
        val xLit = Literal(1.0)
        val yLit = Literal(1.0)
        val gridCellLong = indexSystem match {
            case BNGIndexSystem => lit(1050138790).expr
            case H3IndexSystem  => lit(623060282076758015L).expr
            case _              => lit(0L).expr
        }
        val gridCellStr = indexSystem match {
            case BNGIndexSystem => lit("TQ388791").expr
            case H3IndexSystem  => lit("8a58e0682d6ffff").expr
            case _              => lit("0").expr
        }

        noException should be thrownBy getFunc("as_hex").apply(Seq(pointWkt))
        noException should be thrownBy getFunc("as_json").apply(Seq(pointWkt))
        noException should be thrownBy getFunc("st_point").apply(Seq(xLit, yLit))
        noException should be thrownBy getFunc("st_makeline").apply(Seq(pointsWkt))
        noException should be thrownBy getFunc("st_polygon").apply(Seq(polygonPointsWkt))
        noException should be thrownBy getFunc("st_polygon").apply(Seq(polygonPointsWkt, holePointsWkt))
        an[Error] should be thrownBy getFunc("st_polygon").apply(Seq())
        noException should be thrownBy getFunc("grid_boundaryaswkb").apply(Seq(gridCellLong))
        noException should be thrownBy getFunc("grid_boundaryaswkb").apply(Seq(gridCellStr))
        noException should be thrownBy getFunc("grid_boundary").apply(Seq(gridCellLong, lit("WKT").expr))
        noException should be thrownBy getFunc("grid_boundary").apply(Seq(gridCellStr, lit("WKT").expr))
        noException should be thrownBy getFunc("flatten_polygons").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_xmax").apply(Seq(pointWkt))
        noException should be thrownBy getFunc("st_xmin").apply(Seq(pointWkt))
        noException should be thrownBy getFunc("st_ymax").apply(Seq(pointWkt))
        noException should be thrownBy getFunc("st_ymin").apply(Seq(pointWkt))
        noException should be thrownBy getFunc("st_zmax").apply(Seq(pointWkt))
        noException should be thrownBy getFunc("st_zmin").apply(Seq(pointWkt))
        noException should be thrownBy getFunc("st_isvalid").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_geometrytype").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_area").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_centroid").apply(Seq(pointWkt))
        noException should be thrownBy getFunc("st_geomfromwkt").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_geomfromwkb").apply(Seq(st_aswkb(multiPolygon).expr))
        noException should be thrownBy getFunc("st_geomfromgeojson").apply(Seq(st_asgeojson(multiPolygon).expr))
        noException should be thrownBy getFunc("convert_to_hex").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("convert_to_wkt").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("convert_to_wkb").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("convert_to_coords").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("convert_to_geojson").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_aswkt").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_astext").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_aswkb").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_asbinary").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_asgeojson").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_length").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_perimeter").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_distance").apply(Seq(multiPolygon.expr, pointWkt))
        noException should be thrownBy getFunc("st_contains").apply(Seq(multiPolygon.expr, pointWkt))
        noException should be thrownBy getFunc("st_within").apply(Seq(pointWkt, multiPolygon.expr))
        noException should be thrownBy getFunc("st_translate").apply(Seq(multiPolygon.expr, xLit, yLit))
        noException should be thrownBy getFunc("st_scale").apply(Seq(multiPolygon.expr, xLit, yLit))
        noException should be thrownBy getFunc("st_rotate").apply(Seq(multiPolygon.expr, xLit, yLit))
        noException should be thrownBy getFunc("st_convexhull").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_difference").apply(Seq(multiPolygon.expr, multiPolygon.expr))
        noException should be thrownBy getFunc("st_union").apply(Seq(multiPolygon.expr, multiPolygon.expr))
        noException should be thrownBy getFunc("st_unaryunion").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_simplify").apply(Seq(multiPolygon.expr, xLit))
        noException should be thrownBy getFunc("st_envelope").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_buffer").apply(Seq(multiPolygon.expr, xLit))
        noException should be thrownBy getFunc("st_bufferloop").apply(Seq(multiPolygon.expr, xLit, yLit))
        noException should be thrownBy getFunc("st_numpoints").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_intersects").apply(Seq(multiPolygon.expr, pointsWkt))
        noException should be thrownBy getFunc("st_intersection").apply(Seq(multiPolygon.expr, multiPolygon.expr))
        noException should be thrownBy getFunc("st_srid").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("st_setsrid").apply(Seq(multiPolygon.expr, lit("EPSG:4326").expr))
        noException should be thrownBy getFunc("st_transform").apply(Seq(multiPolygon.expr, lit("EPSG:4326").expr))
        noException should be thrownBy getFunc("st_hasvalidcoordinates").apply(
          Seq(multiPolygon.expr, lit("EPSG:4326").expr, lit("boundary").expr)
        )
        noException should be thrownBy getFunc("st_intersection_aggregate").apply(
          Seq(
            grid_tessellateexplode(multiPolygon, lit(5)).expr,
            grid_tessellateexplode(multiPolygon, lit(5)).expr
          )
        )
        noException should be thrownBy getFunc("st_intersects_aggregate").apply(
          Seq(
            grid_tessellateexplode(multiPolygon, lit(5)).expr,
            grid_tessellateexplode(multiPolygon, lit(5)).expr
          )
        )
        noException should be thrownBy getFunc("st_union_agg").apply(Seq(holePointsWkt))
        noException should be thrownBy getFunc("grid_tessellateexplode").apply(Seq(multiPolygon.expr, lit(5).expr))
        noException should be thrownBy getFunc("grid_tessellateexplode").apply(Seq(multiPolygon.expr, lit(5).expr, lit(false).expr))
        noException should be thrownBy getFunc("grid_tessellateexplode").apply(
          Seq(multiPolygon.expr, lit(5).expr, lit(false).expr, lit(false).expr)
        )
        noException should be thrownBy getFunc("grid_tessellateaslong").apply(Seq(multiPolygon.expr, lit(5).expr))
        noException should be thrownBy getFunc("grid_tessellate").apply(Seq(multiPolygon.expr, lit(5).expr))
        noException should be thrownBy getFunc("grid_tessellate").apply(Seq(multiPolygon.expr, lit(5).expr, lit(false).expr))
        noException should be thrownBy getFunc("grid_tessellate").apply(
          Seq(multiPolygon.expr, lit(5).expr, lit(false).expr, lit(false).expr)
        )
        noException should be thrownBy getFunc("grid_longlatascellid").apply(Seq(xLit, yLit, lit(5).expr, lit(false).expr))
        noException should be thrownBy getFunc("grid_pointascellid").apply(Seq(pointsWkt, lit(5).expr, lit(false).expr))
        noException should be thrownBy getFunc("grid_polyfill").apply(Seq(multiPolygon.expr, lit(5).expr, lit(false).expr))
        noException should be thrownBy getFunc("grid_boundaryaswkb").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("grid_boundary").apply(Seq(multiPolygon.expr, lit("wkt").expr))
        noException should be thrownBy getFunc("grid_cellkring").apply(Seq(multiPolygon.expr, lit(5).expr))
        noException should be thrownBy getFunc("grid_cellkringexplode").apply(Seq(multiPolygon.expr, lit(5).expr))
        noException should be thrownBy getFunc("grid_cellkloop").apply(Seq(multiPolygon.expr, lit(5).expr))
        noException should be thrownBy getFunc("grid_cellkloopexplode").apply(Seq(multiPolygon.expr, lit(5).expr))
        noException should be thrownBy getFunc("grid_geometrykring").apply(Seq(multiPolygon.expr, lit(5).expr, lit(5).expr))
        noException should be thrownBy getFunc("grid_geometrykringexplode").apply(Seq(multiPolygon.expr, lit(5).expr, lit(5).expr))
        noException should be thrownBy getFunc("grid_geometrykloop").apply(Seq(multiPolygon.expr, lit(5).expr, lit(5).expr))
        noException should be thrownBy getFunc("grid_geometrykloopexplode").apply(Seq(multiPolygon.expr, lit(5).expr, lit(5).expr))

        noException should be thrownBy getFunc("st_dump").apply(Seq(multiPolygon.expr))
        noException should be thrownBy getFunc("try_sql").apply(Seq(st_area(multiPolygon).expr))

        noException should be thrownBy getFunc("index_geometry").apply(Seq(multiPolygon.expr, lit(5).expr))
        noException should be thrownBy getFunc("mosaic_explode").apply(Seq(multiPolygon.expr, lit(5).expr))
        noException should be thrownBy getFunc("mosaic_explode").apply(Seq(multiPolygon.expr, lit(5).expr, lit(false).expr))
        noException should be thrownBy getFunc("mosaicfill").apply(Seq(multiPolygon.expr, lit(5).expr))
        noException should be thrownBy getFunc("mosaicfill").apply(Seq(multiPolygon.expr, lit(5).expr, lit(false).expr))
        noException should be thrownBy getFunc("point_index_geom").apply(Seq(pointsWkt, lit(5).expr, lit(false).expr))
        noException should be thrownBy getFunc("point_index_lonlat").apply(Seq(xLit, yLit, lit(5).expr, lit(false).expr))
        noException should be thrownBy getFunc("polyfill").apply(Seq(multiPolygon.expr, lit(5).expr, lit(false).expr))

        spark.sessionState.catalog.createDatabase(CatalogDatabase("test", "", new URI("loc"), Map.empty), ignoreIfExists = true)
        noException should be thrownBy mc.register("test")
    }

    def productH3Detection(): Unit = {
        val mc = h3MosaicContext
        mc.shouldUseDatabricksH3() shouldBe false
        spark.conf.set(SPARK_DATABRICKS_GEO_H3_ENABLED, "false")
        mc.shouldUseDatabricksH3() shouldBe false
        spark.conf.set(SPARK_DATABRICKS_GEO_H3_ENABLED, "true")
        mc.shouldUseDatabricksH3() shouldBe true
    }

    def sqlFunctionLookup(): Unit = {
        val functionBuilder = stub[FunctionBuilder]
        val mc = h3MosaicContext
        val registry = spark.sessionState.functionRegistry

        spark.conf.set(SPARK_DATABRICKS_GEO_H3_ENABLED, "true")

        // Register mock product functions
        registry.registerFunction(
          FunctionIdentifier("h3_longlatash3", None),
          new ExpressionInfo("product", "h3_longlatash3"),
          (exprs: Seq[Expression]) => Column(exprs.head).expr
        )
        registry.registerFunction(
          FunctionIdentifier("h3_polyfillash3", None),
          new ExpressionInfo("product", "h3_polyfillash3"),
          functionBuilder
        )
        registry.registerFunction(
          FunctionIdentifier("h3_boundaryaswkb", None),
          new ExpressionInfo("product", "h3_boundaryaswkb"),
          functionBuilder
        )
        registry.registerFunction(
          FunctionIdentifier("h3_distance", None),
          new ExpressionInfo("product", "h3_distance"),
          functionBuilder
        )

        mc.register(spark)

        registry.lookupFunction(FunctionIdentifier("grid_longlatascellid", None)).get.getName shouldBe "h3_longlatash3"
        registry.lookupFunction(FunctionIdentifier("grid_polyfill", None)).get.getName shouldBe "h3_polyfillash3"
        registry.lookupFunction(FunctionIdentifier("grid_boundaryaswkb", None)).get.getName shouldBe "h3_boundaryaswkb"
    }

    def callDatabricksH3(): Unit = {
        val mc = h3MosaicContext
        spark.conf.set(SPARK_DATABRICKS_GEO_H3_ENABLED, "true")
        import mc.functions._

        val result = spark
            .range(10)
            .withColumn("grid_longlatascellid", grid_longlatascellid(col("id"), col("id"), col("id")))
            .withColumn("grid_polyfill", grid_polyfill(col("id"), col("id")))
            .withColumn("grid_boundaryaswkb", grid_boundaryaswkb(col("id")))
            .collect()

        result.length shouldBe 10
        result.forall(_.getAs[String]("grid_longlatascellid") == "dummy_h3_longlatascellid")
        result.forall(_.getAs[String]("grid_polyfill") == "dummy_h3_polyfill")
        result.forall(_.getAs[String]("grid_boundaryaswkb") == "dummy_h3_boundaryaswkb")
    }

    def reflectedMethods(): Unit = {
        val mc = h3MosaicContext
        val method = mc.getProductMethod("sample_increment")
        method.apply(1).asInstanceOf[Int] shouldBe 2
    }

    def throwErrors(): Unit = {
        spark.conf.set("spark.databricks.clusterUsageTags.sparkVersion", "13-x")
        an[Exception] should be thrownBy MosaicContext.checkDBR(spark)

        spark.conf.set("spark.databricks.clusterUsageTags.sparkVersion", "14-x")
        an[Exception] should be thrownBy MosaicContext.checkDBR(spark)

        spark.conf.set("spark.databricks.clusterUsageTags.sparkVersion", "14-photon-x")
        an[Exception] should be thrownBy MosaicContext.checkDBR(spark)

        spark.conf.set("spark.databricks.clusterUsageTags.sparkVersion", "12-x")
        an[Exception] should be thrownBy MosaicContext.checkDBR(spark)

        spark.conf.set("spark.databricks.clusterUsageTags.sparkVersion", "12-photon-x")
        an[Exception] should be thrownBy MosaicContext.checkDBR(spark)
    }

     def noErrors(): Unit = {
        spark.conf.set("spark.databricks.clusterUsageTags.sparkVersion", "13-ml-x")
        noException should be thrownBy MosaicContext.checkDBR(spark)

        spark.conf.set("spark.databricks.clusterUsageTags.sparkVersion", "13-photon-x")
        noException should be thrownBy MosaicContext.checkDBR(spark)
    }

}

object MosaicContextBehaviors extends MockFactory {

    def h3MosaicContext: MosaicContext = {
        val ix = stub[IndexSystem]
        ix.getCellIdDataType _ when () returns LongType
        ix.name _ when () returns H3.name
        MosaicContext.build(H3IndexSystem, JTS)
    }

}
