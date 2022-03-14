package com.databricks.mosaic.functions

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._

import com.databricks.mosaic.core.geometry.api.GeometryAPI
import com.databricks.mosaic.core.index.IndexSystem
import com.databricks.mosaic.expressions.constructors._
import com.databricks.mosaic.expressions.format._
import com.databricks.mosaic.expressions.geometry._
import com.databricks.mosaic.expressions.helper.TrySql
import com.databricks.mosaic.expressions.index._

class MosaicContext(indexSystem: IndexSystem, geometryAPI: GeometryAPI) extends Serializable {

    import org.apache.spark.sql.adapters.{Column => ColumnAdapter}

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
            (exprs: Seq[Expression]) => ST_MakeLine(exprs(0))
        )
        registry.registerFunction(
            FunctionIdentifier("st_polygon", database),
            ST_MakePolygon.registryExpressionInfo(database),
            (exprs: Seq[Expression]) =>
                exprs match {
                    case e if e.length == 1 => ST_MakePolygon(e.head, array().expr)
                    case e if e.length == 2 => ST_MakePolygon(e.head, e.last)
                }
        )
        registry.registerFunction(
            FunctionIdentifier("index_geometry", database),
            IndexGeometry.registryExpressionInfo(database),
            (exprs: Seq[Expression]) => IndexGeometry(exprs(0), indexSystem.name, geometryAPI.name)
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

        /** Aggregators */
        registry.registerFunction(
            FunctionIdentifier("st_intersection_aggregate", database),
            MosaicExplode.registryExpressionInfo(database),
            (exprs: Seq[Expression]) =>
                ST_IntersectionAggregate(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
            FunctionIdentifier("st_intersects_aggregate", database),
            ST_IntersectsAggregate.registryExpressionInfo(database),
            (exprs: Seq[Expression]) =>
                ST_IntersectsAggregate(exprs(0), exprs(1), geometryAPI.name)
        )

        /** IndexSystem and GeometryAPI Specific methods */
        registry.registerFunction(
            FunctionIdentifier("mosaic_explode", database),
            MosaicExplode.registryExpressionInfo(database),
            (exprs: Seq[Expression]) =>
                MosaicExplode(struct(ColumnAdapter(exprs(0)), ColumnAdapter(exprs(1))).expr, indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
            FunctionIdentifier("mosaicfill", database),
            MosaicFill.registryExpressionInfo(database),
            (exprs: Seq[Expression]) => MosaicFill(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
            FunctionIdentifier("point_index_latlon", database),
            PointIndexLonLat.registryExpressionInfo(database),
            (exprs: Seq[Expression]) => PointIndexLonLat(exprs(0), exprs(1), exprs(2), indexSystem.name)
        )
        registry.registerFunction(
            FunctionIdentifier("point_index", database),
            PointIndex.registryExpressionInfo(database),
            (exprs: Seq[Expression]) => PointIndex(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
        )
        registry.registerFunction(
            FunctionIdentifier("polyfill", database),
            Polyfill.registryExpressionInfo(database),
            (exprs: Seq[Expression]) => Polyfill(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
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
            TrySql.registryExpressionInfo(database, "st_length"),
            (exprs: Seq[Expression]) => TrySql(exprs(0))
        )
    }

    def getGeometryAPI: GeometryAPI = this.geometryAPI

    def getIndexSystem: IndexSystem = this.indexSystem

    // scalastyle:off object.name
    object functions extends Serializable {

        /** IndexSystem and GeometryAPI Agnostic methods */
        def as_hex(inGeom: Column): Column = ColumnAdapter(AsHex(inGeom.expr))
        def as_json(inGeom: Column): Column = ColumnAdapter(AsJSON(inGeom.expr))
        def st_point(xVal: Column, yVal: Column): Column = ColumnAdapter(ST_Point(xVal.expr, yVal.expr))
        def st_makeline(points: Column): Column = ColumnAdapter(ST_MakeLine(points.expr))
        def st_makepolygon(boundaryRing: Column): Column = ColumnAdapter(ST_MakePolygon(boundaryRing.expr, array().expr))
        def st_makepolygon(boundaryRing: Column, holeRingArray: Column): Column =
            ColumnAdapter(ST_MakePolygon(boundaryRing.expr, holeRingArray.expr))

        /** GeometryAPI Specific */
        def flatten_polygons(geom: Column): Column = ColumnAdapter(FlattenPolygons(geom.expr, geometryAPI.name))
        def st_xmax(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "X", "MAX"))
        def st_xmin(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "X", "MIN"))
        def st_ymax(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "Y", "MAX"))
        def st_ymin(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "Y", "MIN"))
        def st_zmax(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "Z", "MAX"))
        def st_zmin(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "Z", "MIN"))
        def st_isvalid(geom: Column): Column = ColumnAdapter(ST_IsValid(geom.expr, geometryAPI.name))
        def st_geometrytype(geom: Column): Column = ColumnAdapter(ST_GeometryType(geom.expr, geometryAPI.name))
        def st_area(geom: Column): Column = ColumnAdapter(ST_Area(geom.expr, geometryAPI.name))
        def st_centroid2D(geom: Column): Column = ColumnAdapter(ST_Centroid(geom.expr, geometryAPI.name))
        def st_centroid3D(geom: Column): Column = ColumnAdapter(ST_Centroid(geom.expr, geometryAPI.name, 3))
        def convert_to(inGeom: Column, outDataType: String): Column = ColumnAdapter(ConvertTo(inGeom.expr, outDataType, geometryAPI.name))
        def st_geomfromwkt(inGeom: Column): Column = ColumnAdapter(ConvertTo(inGeom.expr, "coords", geometryAPI.name))
        def st_geomfromwkb(inGeom: Column): Column = ColumnAdapter(ConvertTo(inGeom.expr, "coords", geometryAPI.name))
        def st_geomfromgeojson(inGeom: Column): Column = ColumnAdapter(ConvertTo(AsJSON(inGeom.expr), "coords", geometryAPI.name))
        def st_aswkt(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkt", geometryAPI.name))
        def st_astext(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkt", geometryAPI.name))
        def st_aswkb(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkb", geometryAPI.name))
        def st_asbinary(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkb", geometryAPI.name))
        def st_asgeojson(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "geojson", geometryAPI.name))
        def st_dump(geom: Column): Column = ColumnAdapter(FlattenPolygons(geom.expr, geometryAPI.name))
        def st_length(geom: Column): Column = ColumnAdapter(ST_Length(geom.expr, geometryAPI.name))
        def st_perimeter(geom: Column): Column = ColumnAdapter(ST_Length(geom.expr, geometryAPI.name))
        def st_distance(geom1: Column, geom2: Column): Column = ColumnAdapter(ST_Distance(geom1.expr, geom2.expr, geometryAPI.name))
        def st_contains(geom1: Column, geom2: Column): Column = ColumnAdapter(ST_Contains(geom1.expr, geom2.expr, geometryAPI.name))
        def st_translate(geom1: Column, xd: Column, yd: Column): Column =
            ColumnAdapter(ST_Translate(geom1.expr, xd.expr, yd.expr, geometryAPI.name))
        def st_scale(geom1: Column, xd: Column, yd: Column): Column =
            ColumnAdapter(ST_Scale(geom1.expr, xd.expr, yd.expr, geometryAPI.name))
        def st_rotate(geom1: Column, td: Column): Column = ColumnAdapter(ST_Rotate(geom1.expr, td.expr, geometryAPI.name))
        def st_convexhull(geom: Column): Column = ColumnAdapter(ST_ConvexHull(geom.expr, geometryAPI.name))
        def st_numpoints(geom: Column): Column = ColumnAdapter(ST_NumPoints(geom.expr, geometryAPI.name))
        def st_intersects(left: Column, right: Column): Column = ColumnAdapter(ST_Intersects(left.expr, right.expr, geometryAPI.name))
        def st_intersection(left: Column, right: Column): Column = ColumnAdapter(ST_Intersection(left.expr, right.expr, geometryAPI.name))

        /** Aggregators */
        def st_intersects_aggregate(leftIndex: Column, rightIndex: Column): Column =
            ColumnAdapter(ST_IntersectsAggregate(leftIndex.expr, rightIndex.expr, geometryAPI.name).toAggregateExpression(isDistinct = false))
        def st_intersection_aggregate(leftIndex: Column, rightIndex: Column): Column =
            ColumnAdapter(ST_IntersectionAggregate(leftIndex.expr, rightIndex.expr, geometryAPI.name, indexSystem.name).toAggregateExpression(isDistinct = false))

        /** IndexSystem Specific */

        /** IndexSystem and GeometryAPI Specific methods */
        def mosaic_explode(geom: Column, resolution: Column): Column =
            ColumnAdapter(MosaicExplode(struct(geom, resolution).expr, indexSystem.name, geometryAPI.name))
        def mosaic_explode(geom: Column, resolution: Int): Column =
            ColumnAdapter(MosaicExplode(struct(geom, lit(resolution)).expr, indexSystem.name, geometryAPI.name))
        def mosaicfill(geom: Column, resolution: Column): Column =
            ColumnAdapter(MosaicFill(geom.expr, resolution.expr, indexSystem.name, geometryAPI.name))
        def mosaicfill(geom: Column, resolution: Int): Column =
            ColumnAdapter(MosaicFill(geom.expr, lit(resolution).expr, indexSystem.name, geometryAPI.name))
        def point_index(point: Column, resolution: Column): Column =
            ColumnAdapter(PointIndex(point.expr, resolution.expr, indexSystem.name, geometryAPI.name))
        def point_index(point: Column, resolution: Int): Column =
            ColumnAdapter(PointIndex(point.expr, lit(resolution).expr, indexSystem.name, geometryAPI.name))
        def point_index_lonlat(lon: Column, lat: Column, resolution: Column): Column =
            ColumnAdapter(PointIndexLonLat(lon.expr, lat.expr, resolution.expr, indexSystem.name))
        def point_index_lonlat(lon: Column, lat: Column, resolution: Int): Column =
            ColumnAdapter(PointIndexLonLat(lon.expr, lat.expr, lit(resolution).expr, indexSystem.name))
        def polyfill(geom: Column, resolution: Column): Column =
            ColumnAdapter(Polyfill(geom.expr, resolution.expr, indexSystem.name, geometryAPI.name))
        def polyfill(geom: Column, resolution: Int): Column =
            ColumnAdapter(Polyfill(geom.expr, lit(resolution).expr, indexSystem.name, geometryAPI.name))
        def index_geometry(indexID: Column): Column = ColumnAdapter(IndexGeometry(indexID.expr, indexSystem.name, geometryAPI.name))

        // Not specific to Mosaic
        def try_sql(inCol: Column): Column = ColumnAdapter(TrySql(inCol.expr))

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
            case None          => throw new IllegalStateException("MosaicContext was not built.")
        }

    def geometryAPI: GeometryAPI = context.getGeometryAPI

    def indexSystem: IndexSystem = context.getIndexSystem

}