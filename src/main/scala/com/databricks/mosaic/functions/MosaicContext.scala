package com.databricks.mosaic.functions

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.{lit, struct}

import com.databricks.mosaic.core.geometry.api.GeometryAPI
import com.databricks.mosaic.core.index.IndexSystem
import com.databricks.mosaic.expressions.constructors._
import com.databricks.mosaic.expressions.format.{AsHex, AsJSON, ConvertTo}
import com.databricks.mosaic.expressions.geometry._
import com.databricks.mosaic.expressions.helper.TrySql
import com.databricks.mosaic.expressions.index._


case class MosaicContext(indexSystem: IndexSystem, geometryAPI: GeometryAPI) extends Serializable {

  import org.apache.spark.sql.adapters.{Column => ColumnAdapter}

  /**
   * Registers required parsers for SQL for Mosaic functionality.
   *
   * @param spark    SparkSession to which the parsers are registered to.
   * @param database A database to which functions are added to.
   *                 By default none is passed resulting in functions
   *                 being registered in default database.
   */
  // noinspection ZeroIndexToHead
  // scalastyle:off line.size.limit
  def register(
                spark: SparkSession,
                database: Option[String] = None
              ): Unit = {
    val registry = spark.sessionState.functionRegistry

    /** IndexSystem and GeometryAPI Agnostic methods */
    registry.registerFunction(FunctionIdentifier("as_hex", database), (exprs: Seq[Expression]) => AsHex(exprs(0)))
    registry.registerFunction(FunctionIdentifier("as_json", database), (exprs: Seq[Expression]) => AsJSON(exprs(0)))
    registry.registerFunction(FunctionIdentifier("st_point", database), (exprs: Seq[Expression]) => ST_Point(exprs(0), exprs(1)))
    registry.registerFunction(FunctionIdentifier("st_makeline", database), (exprs: Seq[Expression]) => ST_MakeLine(exprs(0)))
    registry.registerFunction(FunctionIdentifier("st_polygon", database),
      (exprs: Seq[Expression]) => exprs match {
        case e if e.length == 1 => ST_MakePolygon(e.head)
        case e if e.length == 2 => ST_MakePolygonWithHoles(e.head, e.last)
      })
    registry.registerFunction(FunctionIdentifier("index_geometry", database), (exprs: Seq[Expression]) => IndexGeometry(exprs(0), indexSystem.name, geometryAPI.name))

    /** GeometryAPI Specific */
    registry.registerFunction(FunctionIdentifier("flatten_polygons", database), (exprs: Seq[Expression]) => FlattenPolygons(exprs(0), geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_xmax", database), (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "X", "MAX"))
    registry.registerFunction(FunctionIdentifier("st_xmin", database), (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "X", "MIN"))
    registry.registerFunction(FunctionIdentifier("st_ymax", database), (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "Y", "MAX"))
    registry.registerFunction(FunctionIdentifier("st_ymin", database), (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "Y", "MIN"))
    registry.registerFunction(FunctionIdentifier("st_zmax", database), (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "Z", "MAX"))
    registry.registerFunction(FunctionIdentifier("st_zmin", database), (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "Z", "MIN"))
    registry.registerFunction(FunctionIdentifier("st_isvalid", database), (exprs: Seq[Expression]) => ST_IsValid(exprs(0), geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_geometrytype", database), (exprs: Seq[Expression]) => ST_GeometryType(exprs(0), geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_area", database), (exprs: Seq[Expression]) => ST_Area(exprs(0), geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_centroid2D", database), (exprs: Seq[Expression]) => ST_Centroid(exprs(0), geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_centroid3D", database), (exprs: Seq[Expression]) => ST_Centroid(exprs(0), geometryAPI.name, 3))
    registry.registerFunction(FunctionIdentifier("st_geomfromwkt", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_geomfromwkb", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_geomfromgeojson", database), (exprs: Seq[Expression]) => ConvertTo(AsJSON(exprs(0)), "coords", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("convert_to_hex", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "hex", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("convert_to_wkt", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("convert_to_wkb", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("convert_to_coords", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("convert_to_geojson", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "geojson", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_aswkt", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_astext", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_aswkb", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_asbinary", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_asgeojson", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "geojson", geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_length", database), (exprs: Seq[Expression]) => ST_Length(exprs(0), geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_perimeter", database), (exprs: Seq[Expression]) => ST_Length(exprs(0), geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_distance", database), (exprs: Seq[Expression]) => ST_Distance(exprs(0), exprs(1), geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("st_contains", database), (exprs: Seq[Expression]) => ST_Contains(exprs(0), exprs(1), geometryAPI.name))


    /** IndexSystem Specific */

    /** IndexSystem and GeometryAPI Specific methods */
    registry.registerFunction(
      FunctionIdentifier("mosaic_explode", database),
      (exprs: Seq[Expression]) => MosaicExplode(struct(ColumnAdapter(exprs(0)), ColumnAdapter(exprs(1))).expr, indexSystem.name, geometryAPI.name)
    )
    registry.registerFunction(FunctionIdentifier("mosaicfill", database), (exprs: Seq[Expression]) => MosaicFill(exprs(0), exprs(1), indexSystem.name, geometryAPI.name))
    registry.registerFunction(FunctionIdentifier("point_index", database), (exprs: Seq[Expression]) => PointIndex(exprs(0), exprs(1), exprs(2), indexSystem.name))
    registry.registerFunction(FunctionIdentifier("polyfill", database), (exprs: Seq[Expression]) => Polyfill(exprs(0), exprs(1), indexSystem.name, geometryAPI.name))

    //DataType keywords are needed at checkInput execution time.
    //They cant be passed as Expressions to ConvertTo Expression.
    //Instead they are passed as String instances and for SQL
    //parser purposes separate method names are defined.

    registry.registerFunction(FunctionIdentifier("st_dump", database), (exprs: Seq[Expression]) => FlattenPolygons(exprs(0), geometryAPI.name))

    // Not specific to Mosaic
    registry.registerFunction(FunctionIdentifier("try_sql", database), (exprs: Seq[Expression]) => TrySql(exprs(0)))
  }

  // scalastyle:off object.name
  object functions extends Serializable {
    /** IndexSystem and GeometryAPI Agnostic methods */
    def as_hex(inGeom: Column): Column = ColumnAdapter(AsHex(inGeom.expr))
    def as_json(inGeom: Column): Column = ColumnAdapter(AsJSON(inGeom.expr))
    def st_point(xVal: Column, yVal: Column): Column = ColumnAdapter(ST_Point(xVal.expr, yVal.expr))
    def st_makeline(points: Column): Column = ColumnAdapter(ST_MakeLine(points.expr))
    def st_makepolygon(boundaryRing: Column): Column = ColumnAdapter(ST_MakePolygon(boundaryRing.expr))
    def st_makepolygon(boundaryRing: Column, holeRingArray: Column): Column =
      ColumnAdapter(ST_MakePolygonWithHoles(boundaryRing.expr, holeRingArray.expr))
    
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

    /** IndexSystem Specific */

    /** IndexSystem and GeometryAPI Specific methods */
    def mosaic_explode(geom: Column, resolution: Column): Column = ColumnAdapter(MosaicExplode(struct(geom, resolution).expr, indexSystem.name, geometryAPI.name))
    def mosaic_explode(geom: Column, resolution: Int): Column = ColumnAdapter(MosaicExplode(struct(geom, lit(resolution)).expr, indexSystem.name, geometryAPI.name))
    def mosaicfill(geom: Column, resolution: Column): Column = ColumnAdapter(MosaicFill(geom.expr, resolution.expr, indexSystem.name, geometryAPI.name))
    def mosaicfill(geom: Column, resolution: Int): Column = ColumnAdapter(MosaicFill(geom.expr, lit(resolution).expr, indexSystem.name, geometryAPI.name))
    def point_index(lat: Column, lng: Column, resolution: Column): Column = ColumnAdapter(PointIndex(lat.expr, lng.expr, resolution.expr, indexSystem.name))
    def point_index(lat: Column, lng: Column, resolution: Int): Column = ColumnAdapter(PointIndex(lat.expr, lng.expr, lit(resolution).expr, indexSystem.name))
    def polyfill(geom: Column, resolution: Column): Column = ColumnAdapter(Polyfill(geom.expr, resolution.expr, indexSystem.name, geometryAPI.name))
    def polyfill(geom: Column, resolution: Int): Column = ColumnAdapter(Polyfill(geom.expr, lit(resolution).expr, indexSystem.name, geometryAPI.name))
    def index_geometry(indexID: Column): Column = ColumnAdapter(IndexGeometry(indexID.expr, indexSystem.name, geometryAPI.name))

    //Not specific to Mosaic
    def try_sql(inCol: Column): Column = ColumnAdapter(TrySql(inCol.expr))
  }
}
// scalastyle:on object.name
// scalastyle:on line.size.limit
