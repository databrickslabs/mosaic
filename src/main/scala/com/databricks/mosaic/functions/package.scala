package com.databricks.mosaic

import com.databricks.mosaic.core.{BNG, H3, IndexSystemID, S2}
import com.databricks.mosaic.expressions.format._
import com.databricks.mosaic.expressions.geometry._
import com.databricks.mosaic.expressions.helper.TrySql
import com.databricks.mosaic.index.{IndexGeometry, MosaicExplode, MosaicFill, PointIndex, Polyfill}
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.{Column, SparkSession}
import com.databricks.mosaic.index.MosaicExplodeESRI


/**
 * Object defining column functions and registering SQL parsers for Mosaic functionality.
 */
//noinspection ZeroIndexToHead
package object functions {

  /**
   * Registers required parsers for SQL for Mosaic functionality.
   * @param spark SparkSession to which the parsers are registered to.
   * @param database A database to which functions are added to.
   *                 By default none is passed resulting in functions
   *                 being registered in default database.
   */
  def register(spark: SparkSession, database: Option[String] = None): Unit = {
    val registry = spark.sessionState.functionRegistry

    registry.registerFunction(FunctionIdentifier("as_hex", database), (exprs: Seq[Expression]) => AsHex(exprs(0)))
    registry.registerFunction(FunctionIdentifier("as_json", database), (exprs: Seq[Expression]) => AsJSON(exprs(0)))
    registry.registerFunction(FunctionIdentifier("flatten_polygons", database), (exprs: Seq[Expression]) => FlattenPolygons(exprs(0)))
    registry.registerFunction(
      FunctionIdentifier("h3_mosaic_explode", database),
      (exprs: Seq[Expression]) => MosaicExplode(struct(ColumnAdapter(exprs(0)), ColumnAdapter(exprs(1))).expr, H3.name)
    )
    registry.registerFunction(FunctionIdentifier("h3_mosaicfill", database), (exprs: Seq[Expression]) => MosaicFill(exprs(0), exprs(1), H3.name))
    registry.registerFunction(FunctionIdentifier("h3_point_index", database), (exprs: Seq[Expression]) => PointIndex(exprs(0), exprs(1), exprs(2), H3.name))
    registry.registerFunction(FunctionIdentifier("h3_polyfill", database), (exprs: Seq[Expression]) => Polyfill(exprs(0), exprs(1), H3.name))

    //DataType keywords are needed at checkInput execution time.
    //They cant be passed as Expressions to ConvertTo Expression.
    //Instead they are passed as String instances and for SQL
    //parser purposes separate method names are defined.
    registry.registerFunction(FunctionIdentifier("st_geomfromwkt", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords"))
    registry.registerFunction(FunctionIdentifier("st_geomfromwkb", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords"))
    registry.registerFunction(FunctionIdentifier("st_geomfromgeojson", database), (exprs: Seq[Expression]) => ConvertTo(AsJSON(exprs(0)), "coords"))
    registry.registerFunction(FunctionIdentifier("convert_to_hex", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "hex"))
    registry.registerFunction(FunctionIdentifier("convert_to_wkt", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt"))
    registry.registerFunction(FunctionIdentifier("convert_to_wkb", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb"))
    registry.registerFunction(FunctionIdentifier("convert_to_coords", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords"))
    registry.registerFunction(FunctionIdentifier("convert_to_geojson", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "geojson"))
    registry.registerFunction(FunctionIdentifier("st_aswkt", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt"))
    registry.registerFunction(FunctionIdentifier("st_astext", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt"))
    registry.registerFunction(FunctionIdentifier("st_aswkb", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb"))
    registry.registerFunction(FunctionIdentifier("st_asbinary", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb"))
    registry.registerFunction(FunctionIdentifier("st_asgeojson", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "geojson"))
    registry.registerFunction(FunctionIdentifier("st_xmax", database), (exprs: Seq[Expression]) => ST_MinMaxXY(exprs(0), "X", "MAX"))
    registry.registerFunction(FunctionIdentifier("st_xmin", database), (exprs: Seq[Expression]) => ST_MinMaxXY(exprs(0), "X", "MIN"))
    registry.registerFunction(FunctionIdentifier("st_ymax", database), (exprs: Seq[Expression]) => ST_MinMaxXY(exprs(0), "Y", "MAX"))
    registry.registerFunction(FunctionIdentifier("st_ymin", database), (exprs: Seq[Expression]) => ST_MinMaxXY(exprs(0), "Y", "MIN"))
    registry.registerFunction(FunctionIdentifier("st_isvalid", database), (exprs: Seq[Expression]) => ST_IsValid(exprs(0)))
    registry.registerFunction(FunctionIdentifier("st_geometrytype", database), (exprs: Seq[Expression]) => ST_GeometryType(exprs(0)))
    registry.registerFunction(FunctionIdentifier("st_dump", database), (exprs: Seq[Expression]) => FlattenPolygons(exprs(0)))
    registry.registerFunction(FunctionIdentifier("st_area", database), (exprs: Seq[Expression]) => ST_Area(exprs(0)))
    registry.registerFunction(FunctionIdentifier("st_centroid2D", database), (exprs: Seq[Expression]) => ST_Centroid(exprs(0)))
    registry.registerFunction(FunctionIdentifier("st_centroid3D", database), (exprs: Seq[Expression]) => ST_Centroid(exprs(0), 3))
    registry.registerFunction(FunctionIdentifier("h3_index_geometry", database), (exprs: Seq[Expression]) => IndexGeometry(exprs(0), H3.name))

    //Not specific to Mosaic
    registry.registerFunction(FunctionIdentifier("try_sql", database), (exprs: Seq[Expression]) => TrySql(exprs(0)))
  }

  def convert_to(inGeom: Column, outDataType: String): Column = ColumnAdapter(ConvertTo(inGeom.expr, outDataType))
  def as_hex(inGeom: Column): Column = ColumnAdapter(AsHex(inGeom.expr))
  def as_json(inGeom: Column): Column = ColumnAdapter(AsJSON(inGeom.expr))
  def h3_mosaic_explode(geom: Column, resolution: Column): Column = ColumnAdapter(MosaicExplode(struct(geom, resolution).expr, H3.name))
  def h3_mosaic_explode(geom: Column, resolution: Int): Column = ColumnAdapter(MosaicExplode(struct(geom, lit(resolution)).expr, H3.name))
  def h3_mosaicfill(geom: Column, resolution: Column): Column = ColumnAdapter(MosaicFill(geom.expr, resolution.expr, H3.name))
  def h3_mosaicfill(geom: Column, resolution: Int): Column = ColumnAdapter(MosaicFill(geom.expr, lit(resolution).expr, H3.name))
  def h3_point_index(lat: Column, lng: Column, resolution: Column): Column = ColumnAdapter(PointIndex(lat.expr, lng.expr, resolution.expr, H3.name))
  def h3_point_index(lat: Column, lng: Column, resolution: Int): Column = ColumnAdapter(PointIndex(lat.expr, lng.expr, lit(resolution).expr, H3.name))
  def h3_polyfill(geom: Column, resolution: Column): Column = ColumnAdapter(Polyfill(geom.expr, resolution.expr, H3.name))
  def h3_polyfill(geom: Column, resolution: Int): Column = ColumnAdapter(Polyfill(geom.expr, lit(resolution).expr, H3.name))
  def h3_index_geometry(indexID: Column): Column = ColumnAdapter(IndexGeometry(indexID.expr, H3.name))
  def flatten_polygons(geom: Column): Column = ColumnAdapter(FlattenPolygons(geom.expr))
  def st_dump(geom: Column): Column = ColumnAdapter(FlattenPolygons(geom.expr))
  def st_xmax(geom: Column): Column = ColumnAdapter(ST_MinMaxXY(geom.expr, "X", "MAX"))
  def st_xmin(geom: Column): Column = ColumnAdapter(ST_MinMaxXY(geom.expr, "X", "MIN"))
  def st_ymax(geom: Column): Column = ColumnAdapter(ST_MinMaxXY(geom.expr, "Y", "MAX"))
  def st_ymin(geom: Column): Column = ColumnAdapter(ST_MinMaxXY(geom.expr, "Y", "MIN"))
  def st_isvalid(geom: Column): Column = ColumnAdapter(ST_IsValid(geom.expr))
  def st_geometrytype(geom: Column): Column = ColumnAdapter(ST_GeometryType(geom.expr))
  def st_area(geom: Column): Column = ColumnAdapter(ST_Area(geom.expr))
  def st_centroid2D(geom: Column): Column = ColumnAdapter(ST_Centroid(geom.expr))
  def st_centroid3D(geom: Column): Column = ColumnAdapter(ST_Centroid(geom.expr, 3))
  def h3_mosaic_explode_esri(geom: Column, resolution: Column): Column = ColumnAdapter(MosaicExplodeESRI(struct(geom, resolution).expr, H3.name))
  def st_geomfromwkt(inGeom: Column): Column = ColumnAdapter(ConvertTo(inGeom.expr, "coords"))
  def st_geomfromwkb(inGeom: Column): Column = ColumnAdapter(ConvertTo(inGeom.expr, "coords"))
  def st_geomfromgeojson(inGeom: Column): Column = ColumnAdapter(ConvertTo(AsJSON(inGeom.expr), "coords"))
  def st_aswkt(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkt"))
  def st_astext(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkt"))
  def st_aswkb(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkb"))
  def st_asbinary(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkb"))
  def st_asgeojson(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "geojson"))
  //Not specific to Mosaic
  def try_sql(inCol: Column): Column = ColumnAdapter(TrySql(inCol.expr))

  //With IndexSystemID matching
  def point_index(lat: Column, lng: Column, resolution: Column, indexSystemID: IndexSystemID): Column = indexSystemID match {
    case H3 => h3_point_index(lat, lng, resolution)
    case S2 => throw new NotImplementedError("S2 has not yet been implemented!")
    case BNG => throw new NotImplementedError("BNG has not yet been implemented!")
  }
  def mosaic_explode(geom: Column, resolution: Column, indexSystemID: IndexSystemID): Column = indexSystemID match {
    case H3 => h3_mosaic_explode(geom, resolution)
    case S2 => throw new NotImplementedError("S2 has not yet been implemented!")
    case BNG => throw new NotImplementedError("BNG has not yet been implemented!")
  }
  def mosaicfill(geom: Column, resolution: Column, indexSystemID: IndexSystemID): Column = indexSystemID match {
    case H3 => h3_mosaicfill(geom, resolution)
    case S2 => throw new NotImplementedError("S2 has not yet been implemented!")
    case BNG => throw new NotImplementedError("BNG has not yet been implemented!")
  }
  def polyfill(geom: Column, resolution: Column, indexSystemID: IndexSystemID): Column = indexSystemID match {
    case H3 => h3_polyfill(geom, resolution)
    case S2 => throw new NotImplementedError("S2 has not yet been implemented!")
    case BNG => throw new NotImplementedError("BNG has not yet been implemented!")
  }
  def index_geometry(geom: Column, indexSystemID: IndexSystemID): Column = indexSystemID match {
    case H3 => h3_index_geometry(geom)
    case S2 => throw new NotImplementedError("S2 has not yet been implemented!")
    case BNG => throw new NotImplementedError("BNG has not yet been implemented!")
  }
}
