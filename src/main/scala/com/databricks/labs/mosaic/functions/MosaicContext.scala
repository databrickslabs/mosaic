package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic.core.crs.CRSBoundsProvider
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.expressions.constructors._
import com.databricks.labs.mosaic.expressions.format._
import com.databricks.labs.mosaic.expressions.geometry._
import com.databricks.labs.mosaic.expressions.helper.TrySql
import com.databricks.labs.mosaic.expressions.index._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}

//noinspection DuplicatedCode
class MosaicContext(indexSystem: IndexSystem, geometryAPI: GeometryAPI) extends Serializable {

    import org.apache.spark.sql.adapters.{Column => ColumnAdapter}

    val crsBoundsProvider: CRSBoundsProvider = CRSBoundsProvider(geometryAPI)

    val idAsLongDefaultExpr: Expression =
        indexSystem.defaultDataTypeID match {
            case LongType   => lit(true).expr
            case StringType => lit(false).expr
            case _          => throw new Error("Id can either be long or string.")
        }

    def register(): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        register(spark)
    }

    def register(database: String): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        spark.sql(s"create database if not exists $database")
        register(spark, Some(database))
    }

    /**
      * Registers required parsers for SQL for Mosaic functionality.
      *
      * @param spark
      *   SparkSession to which the parsers are registered to.
      * @param database
      *   A database to which functions are added to. By default none is passed
      *   resulting in functions being registered in default database.
      */
    // noinspection ZeroIndexToHead
    // scalastyle:off line.size.limit
    def register(
        spark: SparkSession,
        database: Option[String] = None
    ): Unit = {
        val registry = spark.sessionState.functionRegistry

        /** IndexSystem and GeometryAPI Agnostic methods */
        registry.registerFunction(
          FunctionIdentifier("as_hex", database),
          AsHex.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => AsHex(exprs(0))
        )
        registry.registerFunction(
          FunctionIdentifier("as_json", database),
          AsJSON.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => AsJSON(exprs(0))
        )
        registry.registerFunction(
          FunctionIdentifier("st_point", database),
          ST_Point.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_Point(exprs(0), exprs(1))
        )
        registry.registerFunction(
          FunctionIdentifier("st_makeline", database),
          ST_MakeLine.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_MakeLine(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_polygon", database),
          ST_MakePolygon.registryExpressionInfo(database),
          (exprs: Seq[Expression]) =>
              exprs match {
                  case e if e.length == 1 => ST_MakePolygon(e.head, array().expr)
                  case e if e.length == 2 => ST_MakePolygon(e.head, e.last)
                  case _                  => throw new Error("Wrong number of arguments.")
              }
        )
        registry.registerFunction(
          FunctionIdentifier("grid_boundaryaswkb", database),
          IndexGeometry.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => IndexGeometry(exprs(0), Literal("WKB"), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("grid_boundary", database),
          IndexGeometry.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => IndexGeometry(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
        )

        /** GeometryAPI Specific */
        registry.registerFunction(
          FunctionIdentifier("flatten_polygons", database),
          FlattenPolygons.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => FlattenPolygons(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_xmax", database),
          ST_MinMaxXYZ.registryExpressionInfo(database, "st_xmax"),
          (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "X", "MAX")
        )
        registry.registerFunction(
          FunctionIdentifier("st_xmin", database),
          ST_MinMaxXYZ.registryExpressionInfo(database, "st_xmin"),
          (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "X", "MIN")
        )
        registry.registerFunction(
          FunctionIdentifier("st_ymax", database),
          ST_MinMaxXYZ.registryExpressionInfo(database, "st_ymax"),
          (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "Y", "MAX")
        )
        registry.registerFunction(
          FunctionIdentifier("st_ymin", database),
          ST_MinMaxXYZ.registryExpressionInfo(database, "st_ymin"),
          (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "Y", "MIN")
        )
        registry.registerFunction(
          FunctionIdentifier("st_zmax", database),
          ST_MinMaxXYZ.registryExpressionInfo(database, "st_zmax"),
          (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "Z", "MAX")
        )
        registry.registerFunction(
          FunctionIdentifier("st_zmin", database),
          ST_MinMaxXYZ.registryExpressionInfo(database, "st_zmin"),
          (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "Z", "MIN")
        )
        registry.registerFunction(
          FunctionIdentifier("st_isvalid", database),
          ST_IsValid.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_IsValid(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_geometrytype", database),
          ST_GeometryType.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_GeometryType(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_area", database),
          ST_Area.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_Area(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_centroid2D", database),
          ST_Centroid.registryExpressionInfo(database, "st_centroid2D"),
          (exprs: Seq[Expression]) => ST_Centroid(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_centroid3D", database),
          ST_Centroid.registryExpressionInfo(database, "st_centroid3D"),
          (exprs: Seq[Expression]) => ST_Centroid(exprs(0), geometryAPI.name, 3)
        )
        registry.registerFunction(
          FunctionIdentifier("st_geomfromwkt", database),
          ConvertTo.registryExpressionInfo(database, "st_geomfromwkt"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_geomfromwkb", database),
          ConvertTo.registryExpressionInfo(database, "st_geomfromwkb"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_geomfromgeojson", database),
          ConvertTo.registryExpressionInfo(database, "st_geomfromgeojson"),
          (exprs: Seq[Expression]) => ConvertTo(AsJSON(exprs(0)), "coords", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("convert_to_hex", database),
          ConvertTo.registryExpressionInfo(database, "convert_to_hex"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "hex", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("convert_to_wkt", database),
          ConvertTo.registryExpressionInfo(database, "convert_to_wkt"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("convert_to_wkb", database),
          ConvertTo.registryExpressionInfo(database, "convert_to_wkb"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("convert_to_coords", database),
          ConvertTo.registryExpressionInfo(database, "convert_to_coords"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("convert_to_geojson", database),
          ConvertTo.registryExpressionInfo(database, "convert_to_geojson"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "geojson", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_aswkt", database),
          ConvertTo.registryExpressionInfo(database, "st_aswkt"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_astext", database),
          ConvertTo.registryExpressionInfo(database, "st_astext"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_aswkb", database),
          ConvertTo.registryExpressionInfo(database, "st_aswkb"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_asbinary", database),
          ConvertTo.registryExpressionInfo(database, "st_asbinary"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_asgeojson", database),
          ConvertTo.registryExpressionInfo(database, "st_asgeojson"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "geojson", geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_length", database),
          ST_Length.registryExpressionInfo(database, "st_length"),
          (exprs: Seq[Expression]) => ST_Length(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_perimeter", database),
          ST_Length.registryExpressionInfo(database, "st_perimeter"),
          (exprs: Seq[Expression]) => ST_Length(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_distance", database),
          ST_Distance.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_Distance(exprs(0), exprs(1), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_contains", database),
          ST_Contains.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_Contains(exprs(0), exprs(1), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_translate", database),
          ST_Translate.registryExpressionInfo(database, "st_translate"),
          (exprs: Seq[Expression]) => ST_Translate(exprs(0), exprs(1), exprs(2), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_scale", database),
          ST_Scale.registryExpressionInfo(database, "st_scale"),
          (exprs: Seq[Expression]) => ST_Scale(exprs(0), exprs(1), exprs(2), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_rotate", database),
          ST_Rotate.registryExpressionInfo(database, "st_rotate"),
          (exprs: Seq[Expression]) => ST_Rotate(exprs(0), exprs(1), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_convexhull", database),
          ST_ConvexHull.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_ConvexHull(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_unaryunion", database),
          ST_UnaryUnion.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_UnaryUnion(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_buffer", database),
          ST_Buffer.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_Buffer(exprs(0), ColumnAdapter(exprs(1)).cast("double").expr, geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_numpoints", database),
          ST_NumPoints.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_NumPoints(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_intersects", database),
          ST_Intersects.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_Intersects(exprs(0), exprs(1), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_intersection", database),
          ST_Intersection.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_Intersection(exprs(0), exprs(1), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_srid", database),
          ST_SRID.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_SRID(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_setsrid", database),
          ST_SetSRID.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_SetSRID(exprs(0), exprs(1), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_transform", database),
          ST_Transform.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_Transform(exprs(0), exprs(1), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_hasvalidcoordinates", database),
          ST_HasValidCoordinates.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_HasValidCoordinates(exprs(0), exprs(1), exprs(2), geometryAPI.name)
        )

        /** Aggregators */
        registry.registerFunction(
          FunctionIdentifier("st_intersection_aggregate", database),
          ST_IntersectionAggregate.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_IntersectionAggregate(exprs(0), exprs(1), geometryAPI.name, indexSystem.name, 0, 0)
        )
        registry.registerFunction(
          FunctionIdentifier("st_intersects_aggregate", database),
          ST_IntersectsAggregate.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_IntersectsAggregate(exprs(0), exprs(1), geometryAPI.name)
        )

        /** IndexSystem and GeometryAPI Specific methods */
        registry.registerFunction(
          FunctionIdentifier("grid_tessellateexplode", database),
          MosaicExplode.registryExpressionInfo(database),
          (exprs: Seq[Expression]) =>
              exprs match {
                  case e if e.length == 2 => MosaicExplode(e(0), e(1), lit(true).expr, lit(true).expr, indexSystem.name, geometryAPI.name)
                  case e if e.length == 3 =>
                      MosaicExplode(
                        e(0),
                        e(1),
                        e(2),
                        idAsLongDefaultExpr,
                        indexSystem.name,
                        geometryAPI.name
                      )
                  case _                  => MosaicExplode(
                        exprs(0),
                        exprs(1),
                        exprs(2),
                        exprs(3),
                        indexSystem.name,
                        geometryAPI.name
                      )
              }
        )
        registry.registerFunction(
          FunctionIdentifier("grid_tessellateaslong", database),
          MosaicFill.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => MosaicFill(exprs(0), exprs(1), lit(true).expr, lit(true).expr, indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("grid_tessellate", database),
          MosaicFill.registryExpressionInfo(database),
          (exprs: Seq[Expression]) =>
              exprs match {
                  case e if e.length == 2 => MosaicFill(e(0), e(1), lit(true).expr, idAsLongDefaultExpr, indexSystem.name, geometryAPI.name)
                  case e if e.length == 3 => MosaicFill(e(0), e(1), e(2), idAsLongDefaultExpr, indexSystem.name, geometryAPI.name)
                  case e                  => MosaicFill(e(0), e(1), e(2), e(3), indexSystem.name, geometryAPI.name)
              }
        )
        registry.registerFunction(
          FunctionIdentifier("grid_longlatascellid", database),
          PointIndexLonLat.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => PointIndexLonLat(exprs(0), exprs(1), exprs(2), exprs(3), indexSystem.name)
        )
        registry.registerFunction(
          FunctionIdentifier("grid_pointascellid", database),
          PointIndexGeom.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => PointIndexGeom(exprs(0), exprs(1), exprs(2), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("grid_polyfill", database),
          Polyfill.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => Polyfill(exprs(0), exprs(1), exprs(2), indexSystem.name, geometryAPI.name)
        )

        // DataType keywords are needed at checkInput execution time.
        // They cant be passed as Expressions to ConvertTo Expression.
        // Instead they are passed as String instances and for SQL
        // parser purposes separate method names are defined.

        registry.registerFunction(
          FunctionIdentifier("st_dump", database),
          FlattenPolygons.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => FlattenPolygons(exprs(0), geometryAPI.name)
        )

        // Not specific to Mosaic
        registry.registerFunction(
          FunctionIdentifier("try_sql", database),
          TrySql.registryExpressionInfo(database, "try_sql"),
          (exprs: Seq[Expression]) => TrySql(exprs(0))
        )

        /** Legacy API Specific */
        registry.registerFunction(
          FunctionIdentifier("index_geometry", database),
          IndexGeometry.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => IndexGeometry(exprs(0), Literal("WKB"), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("mosaic_explode", database),
          MosaicExplode.registryExpressionInfo(database),
          (exprs: Seq[Expression]) =>
              exprs match {
                  case e if e.length == 2 =>
                      MosaicExplode(
                        e(0),
                        e(1),
                        lit(true).expr,
                        idAsLongDefaultExpr,
                        indexSystem.name,
                        geometryAPI.name
                      )
                  case e if e.length == 3 =>
                      MosaicExplode(
                        e(0),
                        e(1),
                        e(2),
                        idAsLongDefaultExpr,
                        indexSystem.name,
                        geometryAPI.name
                      )
                  case _                  => throw new Error("Wrong number of arguments.")
              }
        )
        registry.registerFunction(
          FunctionIdentifier("mosaicfill", database),
          MosaicFill.registryExpressionInfo(database),
          (exprs: Seq[Expression]) =>
              exprs match {
                  case e if e.length == 2 => MosaicFill(e(0), e(1), lit(true).expr, idAsLongDefaultExpr, indexSystem.name, geometryAPI.name)
                  case e if e.length == 3 => MosaicFill(e(0), e(1), e(2), idAsLongDefaultExpr, indexSystem.name, geometryAPI.name)
                  case _                  => throw new Error("Wrong number of arguments.")
              }
        )
        registry.registerFunction(
          FunctionIdentifier("point_index_geom", database),
          PointIndexGeom.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => PointIndexGeom(exprs(0), exprs(1), idAsLongDefaultExpr, indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("point_index_lonlat", database),
          PointIndexLonLat.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => PointIndexLonLat(exprs(0), exprs(1), exprs(2), idAsLongDefaultExpr, indexSystem.name)
        )
        registry.registerFunction(
          FunctionIdentifier("polyfill", database),
          Polyfill.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => Polyfill(exprs(0), exprs(1), idAsLongDefaultExpr, indexSystem.name, geometryAPI.name)
        )

    }

    def getGeometryAPI: GeometryAPI = this.geometryAPI

    def getIndexSystem: IndexSystem = this.indexSystem

    // scalastyle:off object.name
    object functions extends Serializable {

        /** IndexSystem and GeometryAPI Agnostic methods */
        def as_hex(inGeom: Column): Column = ColumnAdapter(AsHex(inGeom.expr))
        def as_json(inGeom: Column): Column = ColumnAdapter(AsJSON(inGeom.expr))

        /** GeometryAPI Specific */

        /** Spatial functions */
        def flatten_polygons(geom: Column): Column = ColumnAdapter(FlattenPolygons(geom.expr, geometryAPI.name))
        def st_area(geom: Column): Column = ColumnAdapter(ST_Area(geom.expr, geometryAPI.name))
        def st_buffer(geom: Column, radius: Column): Column =
            ColumnAdapter(ST_Buffer(geom.expr, radius.cast("double").expr, geometryAPI.name))
        def st_buffer(geom: Column, radius: Double): Column =
            ColumnAdapter(ST_Buffer(geom.expr, lit(radius).cast("double").expr, geometryAPI.name))
        def st_centroid2D(geom: Column): Column = ColumnAdapter(ST_Centroid(geom.expr, geometryAPI.name))
        def st_centroid3D(geom: Column): Column = ColumnAdapter(ST_Centroid(geom.expr, geometryAPI.name, 3))
        def st_convexhull(geom: Column): Column = ColumnAdapter(ST_ConvexHull(geom.expr, geometryAPI.name))
        def st_distance(geom1: Column, geom2: Column): Column = ColumnAdapter(ST_Distance(geom1.expr, geom2.expr, geometryAPI.name))
        def st_dump(geom: Column): Column = ColumnAdapter(FlattenPolygons(geom.expr, geometryAPI.name))
        def st_geometrytype(geom: Column): Column = ColumnAdapter(ST_GeometryType(geom.expr, geometryAPI.name))
        def st_hasvalidcoordinates(geom: Column, crsCode: Column, which: Column): Column =
            ColumnAdapter(ST_HasValidCoordinates(geom.expr, crsCode.expr, which.expr, geometryAPI.name))
        def st_intersection(left: Column, right: Column): Column = ColumnAdapter(ST_Intersection(left.expr, right.expr, geometryAPI.name))
        def st_isvalid(geom: Column): Column = ColumnAdapter(ST_IsValid(geom.expr, geometryAPI.name))
        def st_length(geom: Column): Column = ColumnAdapter(ST_Length(geom.expr, geometryAPI.name))
        def st_numpoints(geom: Column): Column = ColumnAdapter(ST_NumPoints(geom.expr, geometryAPI.name))
        def st_perimeter(geom: Column): Column = ColumnAdapter(ST_Length(geom.expr, geometryAPI.name))
        def st_rotate(geom1: Column, td: Column): Column = ColumnAdapter(ST_Rotate(geom1.expr, td.expr, geometryAPI.name))
        def st_scale(geom1: Column, xd: Column, yd: Column): Column =
            ColumnAdapter(ST_Scale(geom1.expr, xd.expr, yd.expr, geometryAPI.name))
        def st_setsrid(geom: Column, srid: Column): Column = ColumnAdapter(ST_SetSRID(geom.expr, srid.expr, geometryAPI.name))
        def st_srid(geom: Column): Column = ColumnAdapter(ST_SRID(geom.expr, geometryAPI.name))
        def st_transform(geom: Column, srid: Column): Column = ColumnAdapter(ST_Transform(geom.expr, srid.expr, geometryAPI.name))
        def st_translate(geom1: Column, xd: Column, yd: Column): Column =
            ColumnAdapter(ST_Translate(geom1.expr, xd.expr, yd.expr, geometryAPI.name))
        def st_xmax(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "X", "MAX"))
        def st_xmin(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "X", "MIN"))
        def st_ymax(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "Y", "MAX"))
        def st_ymin(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "Y", "MIN"))
        def st_zmax(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "Z", "MAX"))
        def st_zmin(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "Z", "MIN"))
        def st_unaryunion(geom: Column): Column = ColumnAdapter(ST_UnaryUnion(geom.expr, geometryAPI.name))

        /** Undocumented helper */
        def convert_to(inGeom: Column, outDataType: String): Column = ColumnAdapter(ConvertTo(inGeom.expr, outDataType, geometryAPI.name))

        /** Geometry constructors */
        def st_point(xVal: Column, yVal: Column): Column = ColumnAdapter(ST_Point(xVal.expr, yVal.expr))
        def st_geomfromwkt(inGeom: Column): Column = ColumnAdapter(ConvertTo(inGeom.expr, "coords", geometryAPI.name))
        def st_geomfromwkb(inGeom: Column): Column = ColumnAdapter(ConvertTo(inGeom.expr, "coords", geometryAPI.name))
        def st_geomfromgeojson(inGeom: Column): Column = ColumnAdapter(ConvertTo(AsJSON(inGeom.expr), "coords", geometryAPI.name))
        def st_makeline(points: Column): Column = ColumnAdapter(ST_MakeLine(points.expr, geometryAPI.name))
        def st_makepolygon(boundaryRing: Column): Column = ColumnAdapter(ST_MakePolygon(boundaryRing.expr, array().expr))
        def st_makepolygon(boundaryRing: Column, holeRingArray: Column): Column =
            ColumnAdapter(ST_MakePolygon(boundaryRing.expr, holeRingArray.expr))

        /** Geometry accessors */
        def st_asbinary(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkb", geometryAPI.name))
        def st_asgeojson(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "geojson", geometryAPI.name))
        def st_astext(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkt", geometryAPI.name))
        def st_aswkb(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkb", geometryAPI.name))
        def st_aswkt(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkt", geometryAPI.name))

        /** Spatial predicates */
        def st_contains(geom1: Column, geom2: Column): Column = ColumnAdapter(ST_Contains(geom1.expr, geom2.expr, geometryAPI.name))
        def st_intersects(left: Column, right: Column): Column = ColumnAdapter(ST_Intersects(left.expr, right.expr, geometryAPI.name))

        /** Aggregators */
        def st_intersects_aggregate(leftIndex: Column, rightIndex: Column): Column =
            ColumnAdapter(
              ST_IntersectsAggregate(leftIndex.expr, rightIndex.expr, geometryAPI.name).toAggregateExpression(isDistinct = false)
            )
        def st_intersection_aggregate(leftIndex: Column, rightIndex: Column): Column =
            ColumnAdapter(
              ST_IntersectionAggregate(leftIndex.expr, rightIndex.expr, geometryAPI.name, indexSystem.name, 0, 0)
                  .toAggregateExpression(isDistinct = false)
            )

        /** IndexSystem Specific */

        /** IndexSystem and GeometryAPI Specific methods */
        def grid_tessellateexplode(geom: Column, resolution: Column): Column =
            grid_tessellateexplode(geom, resolution, lit(true), ColumnAdapter(idAsLongDefaultExpr))
        def grid_tessellateexplode(geom: Column, resolution: Column, keepCoreGeometries: Column): Column =
            grid_tessellateexplode(geom, resolution, keepCoreGeometries, ColumnAdapter(idAsLongDefaultExpr))
        def grid_tessellateexplode(geom: Column, resolution: Column, keepCoreGeometries: Column, idAsLong: Column): Column =
            ColumnAdapter(
                MosaicExplode(geom.expr, resolution.expr, keepCoreGeometries.expr, idAsLong.expr, indexSystem.name, geometryAPI.name)
            )
        def grid_tessellateexplode(geom: Column, resolution: Int): Column =
            grid_tessellateexplode(geom, lit(resolution), lit(true), ColumnAdapter(idAsLongDefaultExpr))
        def grid_tessellateexplode(geom: Column, resolution: Int, keepCoreGeometries: Boolean): Column =
            grid_tessellateexplode(geom, lit(resolution), lit(keepCoreGeometries), ColumnAdapter(idAsLongDefaultExpr))
        def grid_tessellateexplode(geom: Column, resolution: Int, keepCoreGeometries: Boolean, idAsLong: Boolean): Column = {
            val resExpr = lit(resolution).expr
            val keepCoreExpr = lit(keepCoreGeometries).expr
            val idAsLongExpr = lit(idAsLong).expr
            ColumnAdapter(MosaicExplode(geom.expr, resExpr, keepCoreExpr, idAsLongExpr, indexSystem.name, geometryAPI.name))
        }
        def grid_tessellate(geom: Column, resolution: Column): Column =
            grid_tessellate(geom, resolution, lit(true), ColumnAdapter(idAsLongDefaultExpr))
        def grid_tessellate(geom: Column, resolution: Column, keepCoreGeometries: Column): Column =
            grid_tessellate(geom, resolution, keepCoreGeometries, ColumnAdapter(idAsLongDefaultExpr))
        def grid_tessellate(geom: Column, resolution: Column, keepCoreGeometries: Column, idAsLong: Column): Column =
            ColumnAdapter(
                MosaicFill(geom.expr, resolution.expr, keepCoreGeometries.expr, idAsLong.expr, indexSystem.name, geometryAPI.name)
            )
        def grid_tessellate(geom: Column, resolution: Int, keepCoreGeometries: Boolean): Column =
            grid_tessellate(geom, lit(resolution), lit(keepCoreGeometries), ColumnAdapter(idAsLongDefaultExpr))
        def grid_tessellate(geom: Column, resolution: Int, keepCoreGeometries: Boolean, idAsLong: Boolean): Column =
            grid_tessellate(geom, lit(resolution), lit(keepCoreGeometries), lit(idAsLong))
        def grid_pointascellid(point: Column, resolution: Column): Column =
            ColumnAdapter(PointIndexGeom(point.expr, resolution.expr, idAsLongDefaultExpr, indexSystem.name, geometryAPI.name))
        def grid_pointascellid(point: Column, resolution: Int): Column =
            ColumnAdapter(PointIndexGeom(point.expr, lit(resolution).expr, idAsLongDefaultExpr, indexSystem.name, geometryAPI.name))
        def grid_longlatascellid(lon: Column, lat: Column, resolution: Column): Column =
            ColumnAdapter(PointIndexLonLat(lon.expr, lat.expr, resolution.expr, idAsLongDefaultExpr, indexSystem.name))
        def grid_longlatascellid(lon: Column, lat: Column, resolution: Int): Column =
            ColumnAdapter(PointIndexLonLat(lon.expr, lat.expr, lit(resolution).expr, idAsLongDefaultExpr, indexSystem.name))
        def grid_polyfill(geom: Column, resolution: Column): Column =
            ColumnAdapter(Polyfill(geom.expr, resolution.expr, idAsLongDefaultExpr, indexSystem.name, geometryAPI.name))
        def grid_polyfill(geom: Column, resolution: Int): Column =
            ColumnAdapter(Polyfill(geom.expr, lit(resolution).expr, idAsLongDefaultExpr, indexSystem.name, geometryAPI.name))
        def grid_boundaryaswkb(indexID: Column): Column =
            ColumnAdapter(IndexGeometry(indexID.expr, lit("WKB").expr, indexSystem.name, geometryAPI.name))
        def grid_boundary(indexID: Column, format: Column): Column =
            ColumnAdapter(IndexGeometry(indexID.expr, format.expr, indexSystem.name, geometryAPI.name))
        def grid_boundary(indexID: Column, format: String): Column =
            ColumnAdapter(IndexGeometry(indexID.expr, lit(format).expr, indexSystem.name, geometryAPI.name))

        // Not specific to Mosaic
        def try_sql(inCol: Column): Column = ColumnAdapter(TrySql(inCol.expr))

        // Legacy API
        @deprecated("Please use 'grid_boundaryaswkb' or 'grid_boundary(..., format_name)' expressions instead.")
        def index_geometry(indexID: Column): Column = grid_boundaryaswkb(indexID)
        @deprecated("Please use 'grid_tassellateexplode' expression instead.")
        def mosaic_explode(geom: Column, resolution: Column): Column = grid_tessellateexplode(geom, resolution)
        @deprecated("Please use 'grid_tassellateexplode' expression instead.")
        def mosaic_explode(geom: Column, resolution: Column, keepCoreGeometries: Boolean): Column =
            grid_tessellateexplode(geom, resolution, lit(keepCoreGeometries))
        @deprecated("Please use 'grid_tassellateexplode' expression instead.")
        def mosaic_explode(geom: Column, resolution: Column, keepCoreGeometries: Column): Column =
            grid_tessellateexplode(geom, resolution, keepCoreGeometries)
        @deprecated("Please use 'grid_tassellateexplode' expression instead.")
        def mosaic_explode(geom: Column, resolution: Int): Column = grid_tessellateexplode(geom, resolution)
        @deprecated("Please use 'grid_tassellateexplode' expression instead.")
        def mosaic_explode(geom: Column, resolution: Int, keepCoreGeometries: Boolean): Column =
            grid_tessellateexplode(geom, resolution, keepCoreGeometries)
        @deprecated("Please use 'grid_tassellateexplode' expression instead.")
        def mosaic_explode(geom: Column, resolution: Int, keepCoreGeometries: Column): Column =
            grid_tessellateexplode(geom, lit(resolution), keepCoreGeometries)
        @deprecated("Please use 'grid_tessellate' expression instead.")
        def mosaicfill(geom: Column, resolution: Column): Column = grid_tessellate(geom, resolution)
        @deprecated("Please use 'grid_tessellate' expression instead.")
        def mosaicfill(geom: Column, resolution: Int): Column = grid_tessellate(geom, lit(resolution))
        @deprecated("Please use 'grid_tessellate' expression instead.")
        def mosaicfill(geom: Column, resolution: Column, keepCoreGeometries: Boolean): Column =
            grid_tessellate(geom, resolution, lit(keepCoreGeometries))
        @deprecated("Please use 'grid_tessellate' expression instead.")
        def mosaicfill(geom: Column, resolution: Int, keepCoreGeometries: Boolean): Column =
            grid_tessellate(geom, resolution, keepCoreGeometries)
        @deprecated("Please use 'grid_tessellate' expression instead.")
        def mosaicfill(geom: Column, resolution: Column, keepCoreGeometries: Column): Column =
            grid_tessellate(geom, resolution, keepCoreGeometries)
        @deprecated("Please use 'grid_tessellate' expression instead.")
        def mosaicfill(geom: Column, resolution: Int, keepCoreGeometries: Column): Column =
            grid_tessellate(geom, lit(resolution), keepCoreGeometries)
        @deprecated("Please use 'grid_pointascellid' expressions instead.")
        def point_index_geom(point: Column, resolution: Column): Column = grid_pointascellid(point, resolution)
        @deprecated("Please use 'grid_pointascellid' expressions instead.")
        def point_index_geom(point: Column, resolution: Int): Column = grid_pointascellid(point, resolution)
        @deprecated("Please use 'grid_longlatascellid' expressions instead.")
        def point_index_lonlat(lon: Column, lat: Column, resolution: Column): Column =
            grid_longlatascellid(lon, lat, resolution)
        @deprecated("Please use 'grid_longlatascellid' expressions instead.")
        def point_index_lonlat(lon: Column, lat: Column, resolution: Int): Column =
            grid_longlatascellid(lon, lat, resolution)
        @deprecated("Please use 'grid_polyfill' expressions instead.")
        def polyfill(geom: Column, resolution: Column): Column =
            grid_polyfill(geom, resolution)
        @deprecated("Please use 'grid_polyfill' expressions instead.")
        def polyfill(geom: Column, resolution: Int): Column =
            grid_polyfill(geom, resolution)

    }

}
// scalastyle:on object.name
// scalastyle:on line.size.limit

object MosaicContext {

    private var instance: Option[MosaicContext] = None

    def build(indexSystem: IndexSystem, geometryAPI: GeometryAPI): MosaicContext = {
        instance = Some(new MosaicContext(indexSystem, geometryAPI))
        context
    }

    def context: MosaicContext =
        instance match {
            case Some(context) => context
            case None          => throw new Error("MosaicContext was not built.")
        }

    def reset(): Unit = instance = None

    def geometryAPI: GeometryAPI = context.getGeometryAPI

    def indexSystem: IndexSystem = context.getIndexSystem

}
