package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic.{DATABRICKS_SQL_FUNCTIONS_MODULE, H3, SPARK_DATABRICKS_GEO_H3_ENABLED}
import com.databricks.labs.mosaic.core.crs.CRSBoundsProvider
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.types.ChipType
import com.databricks.labs.mosaic.expressions.constructors._
import com.databricks.labs.mosaic.expressions.format._
import com.databricks.labs.mosaic.expressions.geometry._
import com.databricks.labs.mosaic.expressions.util.TrySql
import com.databricks.labs.mosaic.expressions.index._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.types.{LongType, StringType}

import scala.reflect.runtime.universe

//noinspection DuplicatedCode
class MosaicContext(indexSystem: IndexSystem, geometryAPI: GeometryAPI) extends Serializable {

    import org.apache.spark.sql.adapters.{Column => ColumnAdapter}

    val crsBoundsProvider: CRSBoundsProvider = CRSBoundsProvider(geometryAPI)

    val mirror: universe.Mirror = universe.runtimeMirror(getClass.getClassLoader)

    def setCellIdDataType(dataType: String): Unit = if (dataType == "string") {
        indexSystem.setCellIdDataType(StringType)
    } else if (dataType == "long") {
        indexSystem.setCellIdDataType(LongType)
    } else {
        throw new Error(s"Unsupported data type: $dataType")
    }

    def registerProductH3(registry: FunctionRegistry, dbName: Option[String]): Unit = {
        aliasFunction(registry, "grid_longlatascellid", dbName, "h3_longlatash3", None)
        aliasFunction(registry, "grid_polyfill", dbName, "h3_polyfillash3", None)
        aliasFunction(registry, "grid_boundaryaswkb", dbName, "h3_boundaryaswkb", None)
    }

    def aliasFunction( 
        registry: FunctionRegistry,
        alias: String,
        aliasDbName: Option[String],
        functionName: String,
        functionDbName: Option[String]
    ): Unit = {
        registry.registerFunction(
          FunctionIdentifier(alias, aliasDbName),
          registry.lookupFunction(FunctionIdentifier(functionName, functionDbName)).get,
          registry.lookupFunctionBuilder(FunctionIdentifier(functionName, functionDbName)).get
        )
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
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords", geometryAPI.name, Some("st_geomfromwkt"))
        )
        registry.registerFunction(
          FunctionIdentifier("st_geomfromwkb", database),
          ConvertTo.registryExpressionInfo(database, "st_geomfromwkb"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords", geometryAPI.name, Some("st_geomfromwkb"))
        )
        registry.registerFunction(
          FunctionIdentifier("st_geomfromgeojson", database),
          ConvertTo.registryExpressionInfo(database, "st_geomfromgeojson"),
          (exprs: Seq[Expression]) => ConvertTo(AsJSON(exprs(0)), "coords", geometryAPI.name, Some("st_geomfromgeojson"))
        )
        registry.registerFunction(
          FunctionIdentifier("convert_to_hex", database),
          ConvertTo.registryExpressionInfo(database, "convert_to_hex"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "hex", geometryAPI.name, Some("convert_to_hex"))
        )
        registry.registerFunction(
          FunctionIdentifier("convert_to_wkt", database),
          ConvertTo.registryExpressionInfo(database, "convert_to_wkt"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt", geometryAPI.name, Some("convert_to_wkt"))
        )
        registry.registerFunction(
          FunctionIdentifier("convert_to_wkb", database),
          ConvertTo.registryExpressionInfo(database, "convert_to_wkb"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb", geometryAPI.name, Some("convert_to_wkb"))
        )
        registry.registerFunction(
          FunctionIdentifier("convert_to_coords", database),
          ConvertTo.registryExpressionInfo(database, "convert_to_coords"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords", geometryAPI.name, Some("convert_to_coords"))
        )
        registry.registerFunction(
          FunctionIdentifier("convert_to_geojson", database),
          ConvertTo.registryExpressionInfo(database, "convert_to_geojson"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "geojson", geometryAPI.name, Some("convert_to_geojson"))
        )
        registry.registerFunction(
          FunctionIdentifier("st_aswkt", database),
          ConvertTo.registryExpressionInfo(database, "st_aswkt"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt", geometryAPI.name, Some("st_aswkt"))
        )
        registry.registerFunction(
          FunctionIdentifier("st_astext", database),
          ConvertTo.registryExpressionInfo(database, "st_astext"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt", geometryAPI.name, Some("st_astext"))
        )
        registry.registerFunction(
          FunctionIdentifier("st_aswkb", database),
          ConvertTo.registryExpressionInfo(database, "st_aswkb"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb", geometryAPI.name, Some("st_aswkb"))
        )
        registry.registerFunction(
          FunctionIdentifier("st_asbinary", database),
          ConvertTo.registryExpressionInfo(database, "st_asbinary"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb", geometryAPI.name, Some("st_asbinary"))
        )
        registry.registerFunction(
          FunctionIdentifier("st_asgeojson", database),
          ConvertTo.registryExpressionInfo(database, "st_asgeojson"),
          (exprs: Seq[Expression]) => ConvertTo(exprs(0), "geojson", geometryAPI.name, Some("st_asgeojson"))
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
          FunctionIdentifier("st_difference", database),
          ST_Difference.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_Difference(exprs(0), exprs(1), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_union", database),
          ST_Union.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_Union(exprs(0), exprs(1), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_unaryunion", database),
          ST_UnaryUnion.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_UnaryUnion(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_simplify", database),
          ST_Simplify.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_Simplify(exprs(0), ColumnAdapter(exprs(1)).cast("double").expr, geometryAPI.name)
        )
        registry.registerFunction(
            FunctionIdentifier("st_envelope", database),
            ST_Envelope.registryExpressionInfo(database),
            (exprs: Seq[Expression]) => ST_Envelope(exprs(0), geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("st_buffer", database),
          ST_Buffer.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_Buffer(exprs(0), ColumnAdapter(exprs(1)).cast("double").expr, geometryAPI.name)
        )
        registry.registerFunction(
            FunctionIdentifier("st_bufferloop", database),
            ST_BufferLoop.registryExpressionInfo(database),
            (exprs: Seq[Expression]) =>
                ST_BufferLoop(
                    exprs(0),
                    ColumnAdapter(exprs(1)).cast("double").expr,
                    ColumnAdapter(exprs(2)).cast("double").expr,
                    geometryAPI.name
                )
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
        registry.registerFunction(
          FunctionIdentifier("st_union_agg", database),
          ST_UnionAgg.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => ST_UnionAgg(exprs(0), geometryAPI.name)
        )

        /** IndexSystem and GeometryAPI Specific methods */
        registry.registerFunction(
          FunctionIdentifier("grid_tessellateexplode", database),
          MosaicExplode.registryExpressionInfo(database),
          (exprs: Seq[Expression]) =>
              exprs match {
                  case e if e.length == 2 => MosaicExplode(e(0), e(1), lit(true).expr, indexSystem.name, geometryAPI.name)
                  case e                  => MosaicExplode(e(0), e(1), e(2), indexSystem.name, geometryAPI.name)
              }
        )
        registry.registerFunction(
          FunctionIdentifier("grid_tessellateaslong", database),
          MosaicFill.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => MosaicFill(exprs(0), exprs(1), lit(true).expr, indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("grid_tessellate", database),
          MosaicFill.registryExpressionInfo(database),
          (exprs: Seq[Expression]) =>
              exprs match {
                  case e if e.length == 2 => MosaicFill(e(0), e(1), lit(true).expr, indexSystem.name, geometryAPI.name)
                  case e                  => MosaicFill(e(0), e(1), e(2), indexSystem.name, geometryAPI.name)
              }
        )

        if (shouldUseDatabricksH3()) {
            // Forward the H3 calls to product directly
            registerProductH3(registry, database)
        } else {
            registry.registerFunction(
              FunctionIdentifier("grid_longlatascellid", database),
              PointIndexLonLat.registryExpressionInfo(database),
              (exprs: Seq[Expression]) => PointIndexLonLat(exprs(0), exprs(1), exprs(2), indexSystem.name)
            )

            registry.registerFunction(
              FunctionIdentifier("grid_polyfill", database),
              Polyfill.registryExpressionInfo(database),
              (exprs: Seq[Expression]) => Polyfill(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
            )

            registry.registerFunction(
              FunctionIdentifier("grid_boundaryaswkb", database),
              IndexGeometry.registryExpressionInfo(database),
              (exprs: Seq[Expression]) => IndexGeometry(exprs(0), Literal("WKB"), indexSystem.name, geometryAPI.name)
            )
        }

        registry.registerFunction(
          FunctionIdentifier("grid_pointascellid", database),
          PointIndexGeom.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => PointIndexGeom(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
        )

        registry.registerFunction(
          FunctionIdentifier("grid_boundary", database),
          IndexGeometry.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => IndexGeometry(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("grid_cellkring", database),
          CellKRing.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => CellKRing(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
            FunctionIdentifier("grid_cellkringexplode", database),
            CellKRingExplode.registryExpressionInfo(database),
            (exprs: Seq[Expression]) => CellKRingExplode(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("grid_cellkloop", database),
          CellKLoop.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => CellKLoop(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
            FunctionIdentifier("grid_cellkloopexplode", database),
            CellKLoopExplode.registryExpressionInfo(database),
            (exprs: Seq[Expression]) => CellKLoopExplode(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("grid_geometrykring", database),
          GeometryKRing.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => GeometryKRing(exprs(0), exprs(1), exprs(2), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
            FunctionIdentifier("grid_geometrykringexplode", database),
            GeometryKRingExplode.registryExpressionInfo(database),
            (exprs: Seq[Expression]) => GeometryKRingExplode(exprs(0), exprs(1), exprs(2), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
          FunctionIdentifier("grid_geometrykloop", database),
          GeometryKLoop.registryExpressionInfo(database),
          (exprs: Seq[Expression]) => GeometryKLoop(exprs(0), exprs(1), exprs(2), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
            FunctionIdentifier("grid_geometrykloopexplode", database),
            GeometryKLoopExplode.registryExpressionInfo(database),
            (exprs: Seq[Expression]) => GeometryKLoopExplode(exprs(0), exprs(1), exprs(2), indexSystem.name, geometryAPI.name)
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

        /** Legacy API Specific aliases */
        aliasFunction(registry, "index_geometry", database, "grid_boundaryaswkb", database)
        aliasFunction(registry, "mosaic_explode", database, "grid_tessellateexplode", database)
        aliasFunction(registry, "mosaicfill", database, "grid_tessellate", database)
        aliasFunction(registry, "point_index_geom", database, "grid_pointascellid", database)
        aliasFunction(registry, "point_index_lonlat", database, "grid_longlatascellid", database)
        aliasFunction(registry, "polyfill", database, "grid_polyfill", database)

    }

    def getGeometryAPI: GeometryAPI = this.geometryAPI

    def getIndexSystem: IndexSystem = this.indexSystem

    def getProductMethod(methodName: String): universe.MethodMirror = {
        val functionsModuleSymbol: universe.ModuleSymbol = mirror.staticModule(DATABRICKS_SQL_FUNCTIONS_MODULE)

        val functionsModuleMirror = mirror.reflectModule(functionsModuleSymbol)
        val instanceMirror = mirror.reflect(functionsModuleMirror.instance)

        val methodSymbol = functionsModuleSymbol.info.decl(universe.TermName(methodName)).asMethod
        instanceMirror.reflectMethod(methodSymbol)
    }

    def shouldUseDatabricksH3(): Boolean = {
        val spark = SparkSession.builder().getOrCreate()
        val isDatabricksH3Enabled = spark.conf.get(SPARK_DATABRICKS_GEO_H3_ENABLED, "false") == "true"
        indexSystem.name == H3.name && isDatabricksH3Enabled
    }

    // scalastyle:off object.name
    object functions extends Serializable {

        /**
          * functions should follow the pattern "def fname(argName: Type, ...):
          * returnType = ..." failing to do so may brake the R build.
          */

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
        def st_bufferloop(geom: Column, r1: Column, r2: Column): Column =
            ColumnAdapter(ST_BufferLoop(geom.expr, r1.cast("double").expr, r2.cast("double").expr, geometryAPI.name))
        def st_bufferloop(geom: Column, r1: Double, r2: Double): Column =
            ColumnAdapter(ST_BufferLoop(geom.expr, lit(r1).cast("double").expr, lit(r2).cast("double").expr, geometryAPI.name))
        def st_centroid2D(geom: Column): Column = ColumnAdapter(ST_Centroid(geom.expr, geometryAPI.name))
        def st_centroid3D(geom: Column): Column = ColumnAdapter(ST_Centroid(geom.expr, geometryAPI.name, 3))
        def st_convexhull(geom: Column): Column = ColumnAdapter(ST_ConvexHull(geom.expr, geometryAPI.name))
        def st_difference(geom1: Column, geom2: Column): Column = ColumnAdapter(ST_Difference(geom1.expr, geom2.expr, geometryAPI.name))
        def st_distance(geom1: Column, geom2: Column): Column = ColumnAdapter(ST_Distance(geom1.expr, geom2.expr, geometryAPI.name))
        def st_dump(geom: Column): Column = ColumnAdapter(FlattenPolygons(geom.expr, geometryAPI.name))
        def st_envelope(geom: Column): Column = ColumnAdapter(ST_Envelope(geom.expr, geometryAPI.name))
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
        def st_simplify(geom: Column, tolerance: Column): Column =
            ColumnAdapter(ST_Simplify(geom.expr, tolerance.cast("double").expr, geometryAPI.name))
        def st_simplify(geom: Column, tolerance: Double): Column =
            ColumnAdapter(ST_Simplify(geom.expr, lit(tolerance).cast("double").expr, geometryAPI.name))
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
        def st_union(leftGeom: Column, rightGeom: Column): Column = ColumnAdapter(ST_Union(leftGeom.expr, rightGeom.expr, geometryAPI.name))
        def st_unaryunion(geom: Column): Column = ColumnAdapter(ST_UnaryUnion(geom.expr, geometryAPI.name))

        /** Undocumented helper */
        def convert_to(inGeom: Column, outDataType: String): Column = ColumnAdapter(ConvertTo(inGeom.expr, outDataType, geometryAPI.name, Some("convert_to")))

        /** Geometry constructors */
        def st_point(xVal: Column, yVal: Column): Column = ColumnAdapter(ST_Point(xVal.expr, yVal.expr))
        def st_geomfromwkt(inGeom: Column): Column = ColumnAdapter(ConvertTo(inGeom.expr, "coords", geometryAPI.name, Some("st_geomfromwkt")))
        def st_geomfromwkb(inGeom: Column): Column = ColumnAdapter(ConvertTo(inGeom.expr, "coords", geometryAPI.name, Some("st_geomfromwkb")))
        def st_geomfromgeojson(inGeom: Column): Column = ColumnAdapter(ConvertTo(AsJSON(inGeom.expr), "coords", geometryAPI.name, Some("st_geomfromgeojson")))
        def st_makeline(points: Column): Column = ColumnAdapter(ST_MakeLine(points.expr, geometryAPI.name))
        def st_makepolygon(boundaryRing: Column): Column = ColumnAdapter(ST_MakePolygon(boundaryRing.expr, array().expr))
        def st_makepolygon(boundaryRing: Column, holeRingArray: Column): Column =
            ColumnAdapter(ST_MakePolygon(boundaryRing.expr, holeRingArray.expr))

        /** Geometry accessors */
        def st_asbinary(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkb", geometryAPI.name, Some("st_asbinary")))
        def st_asgeojson(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "geojson", geometryAPI.name, Some("st_asgeojson")))
        def st_astext(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkt", geometryAPI.name, Some("st_astext")))
        def st_aswkb(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkb", geometryAPI.name, Some("st_aswkb")))
        def st_aswkt(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkt", geometryAPI.name, Some("st_aswkt")))

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
        def st_union_agg(geom: Column): Column =
            ColumnAdapter(ST_UnionAgg(geom.expr, geometryAPI.name).toAggregateExpression(isDistinct = false))

        /** IndexSystem Specific */

        /** IndexSystem and GeometryAPI Specific methods */
        def grid_tessellateexplode(geom: Column, resolution: Column): Column =
            grid_tessellateexplode(geom, resolution, lit(true))
        def grid_tessellateexplode(geom: Column, resolution: Int): Column =
            grid_tessellateexplode(geom, lit(resolution), lit(true))
        def grid_tessellateexplode(geom: Column, resolution: Int, keepCoreGeometries: Boolean): Column =
            grid_tessellateexplode(geom, lit(resolution), lit(keepCoreGeometries))
        def grid_tessellateexplode(geom: Column, resolution: Int, keepCoreGeometries: Column): Column =
            grid_tessellateexplode(geom, lit(resolution), keepCoreGeometries)
        def grid_tessellateexplode(geom: Column, resolution: Column, keepCoreGeometries: Column): Column =
            ColumnAdapter(
                MosaicExplode(geom.expr, resolution.expr, keepCoreGeometries.expr, indexSystem.name, geometryAPI.name)
            )
        def grid_tessellate(geom: Column, resolution: Column): Column =
            grid_tessellate(geom, resolution, lit(true))
        def grid_tessellate(geom: Column, resolution: Int): Column =
            grid_tessellate(geom, lit(resolution), lit(true))
        def grid_tessellate(geom: Column, resolution: Column, keepCoreGeometries: Boolean): Column =
            grid_tessellate(geom, resolution, lit(keepCoreGeometries))
        def grid_tessellate(geom: Column, resolution: Int, keepCoreGeometries: Boolean): Column =
            grid_tessellate(geom, lit(resolution), lit(keepCoreGeometries))
        def grid_tessellate(geom: Column, resolution: Column, keepCoreGeometries: Column): Column =
            ColumnAdapter(
              MosaicFill(geom.expr, resolution.expr, keepCoreGeometries.expr, indexSystem.name, geometryAPI.name)
            )
        def grid_pointascellid(point: Column, resolution: Column): Column =
            ColumnAdapter(PointIndexGeom(point.expr, resolution.expr, indexSystem.name, geometryAPI.name))
        def grid_pointascellid(point: Column, resolution: Int): Column =
            ColumnAdapter(PointIndexGeom(point.expr, lit(resolution).expr, indexSystem.name, geometryAPI.name))
        def grid_longlatascellid(lon: Column, lat: Column, resolution: Column): Column = {
            if (shouldUseDatabricksH3()) {
                getProductMethod("h3_longlatascellid")
                    .apply(lon, lat, resolution)
                    .asInstanceOf[Column]
            } else {
                ColumnAdapter(PointIndexLonLat(lon.expr, lat.expr, resolution.expr, indexSystem.name))
            }
        }
        def grid_longlatascellid(lon: Column, lat: Column, resolution: Int): Column = grid_longlatascellid(lon, lat, lit(resolution))
        def grid_polyfill(geom: Column, resolution: Column): Column = {
            if (shouldUseDatabricksH3()) {
                getProductMethod("h3_polyfill")
                    .apply(geom, resolution)
                    .asInstanceOf[Column]
            } else {
                ColumnAdapter(Polyfill(geom.expr, resolution.expr, indexSystem.name, getGeometryAPI.name))
            }
        }
        def grid_polyfill(geom: Column, resolution: Int): Column = grid_polyfill(geom, lit(resolution))
        def grid_boundaryaswkb(indexID: Column): Column = {
            if (shouldUseDatabricksH3()) {
                getProductMethod("h3_boundaryaswkb")
                    .apply(indexID)
                    .asInstanceOf[Column]
            } else {
                ColumnAdapter(IndexGeometry(indexID.expr, lit("WKB").expr, indexSystem.name, getGeometryAPI.name))
            }
        }
        def grid_boundary(indexID: Column, format: Column): Column =
            ColumnAdapter(IndexGeometry(indexID.expr, format.expr, indexSystem.name, geometryAPI.name))
        def grid_boundary(indexID: Column, format: String): Column =
            ColumnAdapter(IndexGeometry(indexID.expr, lit(format).expr, indexSystem.name, geometryAPI.name))
        def grid_cellkring(cellId: Column, k: Column): Column = ColumnAdapter(CellKRing(cellId.expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_cellkring(cellId: Column, k: Int): Column = ColumnAdapter(CellKRing(cellId.expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_cellkringexplode(cellId: Column, k: Int): Column = ColumnAdapter(CellKRingExplode(cellId.expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_cellkringexplode(cellId: Column, k: Column): Column = ColumnAdapter(CellKRingExplode(cellId.expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_cellkloop(cellId: Column, k: Column): Column = ColumnAdapter(CellKLoop(cellId.expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_cellkloop(cellId: Column, k: Int): Column = ColumnAdapter(CellKLoop(cellId.expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_cellkloopexplode(cellId: Column, k: Int): Column = ColumnAdapter(CellKLoopExplode(cellId.expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_cellkloopexplode(cellId: Column, k: Column): Column = ColumnAdapter(CellKLoopExplode(cellId.expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykring(geom: Column, resolution: Column, k: Column): Column = ColumnAdapter(GeometryKRing(geom.expr, resolution.expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykring(geom: Column, resolution: Column, k: Int): Column = ColumnAdapter(GeometryKRing(geom.expr, resolution.expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykring(geom: Column, resolution: Int, k: Column): Column = ColumnAdapter(GeometryKRing(geom.expr, lit(resolution).expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykring(geom: Column, resolution: Int, k: Int): Column = ColumnAdapter(GeometryKRing(geom.expr, lit(resolution).expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykring(geom: Column, resolution: String, k: Column): Column = ColumnAdapter(GeometryKRing(geom.expr, lit(resolution).expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykring(geom: Column, resolution: String, k: Int): Column = ColumnAdapter(GeometryKRing(geom.expr, lit(resolution).expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykringexplode(geom: Column, resolution: Column, k: Column): Column = ColumnAdapter(GeometryKRingExplode(geom.expr, resolution.expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykringexplode(geom: Column, resolution: Column, k: Int): Column = ColumnAdapter(GeometryKRingExplode(geom.expr, resolution.expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykringexplode(geom: Column, resolution: Int, k: Column): Column = ColumnAdapter(GeometryKRingExplode(geom.expr, lit(resolution).expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykringexplode(geom: Column, resolution: Int, k: Int): Column = ColumnAdapter(GeometryKRingExplode(geom.expr, lit(resolution).expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykringexplode(geom: Column, resolution: String, k: Column): Column = ColumnAdapter(GeometryKRingExplode(geom.expr, lit(resolution).expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykringexplode(geom: Column, resolution: String, k: Int): Column = ColumnAdapter(GeometryKRingExplode(geom.expr, lit(resolution).expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykloop(geom: Column, resolution: Column, k: Column): Column = ColumnAdapter(GeometryKLoop(geom.expr, resolution.expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykloop(geom: Column, resolution: Column, k: Int): Column = ColumnAdapter(GeometryKLoop(geom.expr, resolution.expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykloop(geom: Column, resolution: Int, k: Column): Column = ColumnAdapter(GeometryKLoop(geom.expr, lit(resolution).expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykloop(geom: Column, resolution: Int, k: Int): Column = ColumnAdapter(GeometryKLoop(geom.expr, lit(resolution).expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykloop(geom: Column, resolution: String, k: Column): Column = ColumnAdapter(GeometryKLoop(geom.expr, lit(resolution).expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykloop(geom: Column, resolution: String, k: Int): Column = ColumnAdapter(GeometryKLoop(geom.expr, lit(resolution).expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykloopexplode(geom: Column, resolution: Column, k: Column): Column = ColumnAdapter(GeometryKLoopExplode(geom.expr, resolution.expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykloopexplode(geom: Column, resolution: Column, k: Int): Column = ColumnAdapter(GeometryKLoopExplode(geom.expr, resolution.expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykloopexplode(geom: Column, resolution: Int, k: Column): Column = ColumnAdapter(GeometryKLoopExplode(geom.expr, lit(resolution).expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykloopexplode(geom: Column, resolution: Int, k: Int): Column = ColumnAdapter(GeometryKLoopExplode(geom.expr, lit(resolution).expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykloopexplode(geom: Column, resolution: String, k: Column): Column = ColumnAdapter(GeometryKLoopExplode(geom.expr, lit(resolution).expr, k.expr, indexSystem.name, geometryAPI.name))
        def grid_geometrykloopexplode(geom: Column, resolution: String, k: Int): Column = ColumnAdapter(GeometryKLoopExplode(geom.expr, lit(resolution).expr, lit(k).expr, indexSystem.name, geometryAPI.name))
        def grid_wrapaschip(cellID: Column, isCore: Boolean, getCellGeom: Boolean): Column =
            struct(
                lit(isCore).alias("is_core"),
                cellID.alias("index_id"),
                (if (getCellGeom) grid_boundaryaswkb(cellID) else lit(null)).alias("wkb")
            ).cast(
                ChipType(indexSystem.getCellIdDataType)
            ).alias("chip")

        // Not specific to Mosaic
        def try_sql(inCol: Column): Column = ColumnAdapter(TrySql(inCol.expr))

        // Legacy API
        @deprecated("Please use 'grid_boundaryaswkb' or 'grid_boundary(..., format_name)' expressions instead.")
        def index_geometry(indexID: Column): Column = grid_boundaryaswkb(indexID)
        @deprecated("Please use 'grid_tessellateexplode' expression instead.")
        def mosaic_explode(geom: Column, resolution: Column): Column = grid_tessellateexplode(geom, resolution)
        @deprecated("Please use 'grid_tessellateexplode' expression instead.")
        def mosaic_explode(geom: Column, resolution: Column, keepCoreGeometries: Boolean): Column =
            grid_tessellateexplode(geom, resolution, lit(keepCoreGeometries))
        @deprecated("Please use 'grid_tessellateexplode' expression instead.")
        def mosaic_explode(geom: Column, resolution: Column, keepCoreGeometries: Column): Column =
            grid_tessellateexplode(geom, resolution, keepCoreGeometries)
        @deprecated("Please use 'grid_tessellateexplode' expression instead.")
        def mosaic_explode(geom: Column, resolution: Int): Column = grid_tessellateexplode(geom, resolution)
        @deprecated("Please use 'grid_tessellateexplode' expression instead.")
        def mosaic_explode(geom: Column, resolution: Int, keepCoreGeometries: Boolean): Column =
            grid_tessellateexplode(geom, resolution, keepCoreGeometries)
        @deprecated("Please use 'grid_tessellateexplode' expression instead.")
        def mosaic_explode(geom: Column, resolution: Int, keepCoreGeometries: Column): Column =
            grid_tessellateexplode(geom, resolution, keepCoreGeometries)
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
        def point_index_lonlat(lon: Column, lat: Column, resolution: Column): Column = grid_longlatascellid(lon, lat, resolution)
        @deprecated("Please use 'grid_longlatascellid' expressions instead.")
        def point_index_lonlat(lon: Column, lat: Column, resolution: Int): Column = grid_longlatascellid(lon, lat, resolution)
        @deprecated("Please use 'grid_polyfill' expressions instead.")
        def polyfill(geom: Column, resolution: Column): Column = grid_polyfill(geom, resolution)
        @deprecated("Please use 'grid_polyfill' expressions instead.")
        def polyfill(geom: Column, resolution: Int): Column = grid_polyfill(geom, resolution)

    }

}
// scalastyle:on object.name
// scalastyle:on line.size.limit

object MosaicContext {

    private var instance: Option[MosaicContext] = None

    def build(indexSystem: IndexSystem, geometryAPI: GeometryAPI): MosaicContext = {
        instance = Some(new MosaicContext(indexSystem, geometryAPI))
        instance.get.setCellIdDataType(indexSystem.getCellIdDataType.typeName)
        context()
    }

    def context(): MosaicContext =
        instance match {
            case Some(context) => context
            case None          => throw new Error("MosaicContext was not built.")
        }

    def reset(): Unit = instance = None

    def geometryAPI(): GeometryAPI = context().getGeometryAPI

    def indexSystem(): IndexSystem = context().getIndexSystem

}
