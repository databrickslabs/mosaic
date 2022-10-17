package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.functions.auxiliary.BadIndexSystem
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.{array, lit}
import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.matchers.must.Matchers.{be, noException}
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

import java.net.URI

trait MosaicContextBehaviors extends QueryTest {

    //noinspection EmptyParenMethodAccessedAsParameterless
    def creationOfContext(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        MosaicContext.reset()
        an[Error] should be thrownBy MosaicContext.context
        val mc = MosaicContext.build(indexSystem, geometryAPI)
        MosaicContext.indexSystem shouldEqual indexSystem
        MosaicContext.geometryAPI shouldEqual geometryAPI
        MosaicContext.indexSystem match {
            case BNGIndexSystem => mc.getIndexSystem.getCellIdDataType shouldEqual StringType
            case H3IndexSystem  => mc.getIndexSystem.getCellIdDataType shouldEqual LongType
        }
        an[Error] should be thrownBy MosaicContext.build(BadIndexSystem, geometryAPI)
    }

    def sqlRegistration(indexSystem: IndexSystem, geometryAPI: GeometryAPI): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = MosaicContext.build(indexSystem, geometryAPI)
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
        }
        val gridCellStr = indexSystem match {
            case BNGIndexSystem => lit("TQ388791").expr
            case H3IndexSystem  => lit("8a58e0682d6ffff").expr
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
        noException should be thrownBy getFunc("st_centroid2D").apply(Seq(pointWkt))
        noException should be thrownBy getFunc("st_centroid3D").apply(Seq(pointWkt))
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
        noException should be thrownBy getFunc("st_translate").apply(Seq(multiPolygon.expr, xLit, yLit))
        noException should be thrownBy getFunc("st_scale").apply(Seq(multiPolygon.expr, xLit, yLit))
        noException should be thrownBy getFunc("st_rotate").apply(Seq(multiPolygon.expr, xLit, yLit))
        noException should be thrownBy getFunc("st_convexhull").apply(Seq(multiPolygon.expr))
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
        noException should be thrownBy getFunc("grid_tessellateaslong").apply(Seq(multiPolygon.expr, lit(5).expr))
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

        spark.sessionState.catalog.createDatabase(
            CatalogDatabase("test", "", new URI("loc"), Map.empty),
            ignoreIfExists = true)
        noException should be thrownBy mc.register("test")
    }

}
