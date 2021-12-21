package com.databricks.mosaic

import com.databricks.mosaic.expressions.format._
import com.databricks.mosaic.expressions.geometry.FlattenPolygons
import com.databricks.mosaic.index.h3.{H3_MosaicExplode, H3_MosaicFill, H3_PointIndex, H3_Polyfill}
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.{lit, struct, col}
import org.apache.spark.sql.{Column, SparkSession}
import com.databricks.mosaic.expressions.geometry.ST_MinMaxXY
import com.databricks.mosaic.expressions.geometry.ST_IsValid

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
    registry.registerFunction(FunctionIdentifier("flatten_polygons", database), (exprs: Seq[Expression]) => FlattenPolygons(exprs(0)))
    registry.registerFunction(
      FunctionIdentifier("h3_mosaic_explode", database),
      (exprs: Seq[Expression]) => H3_MosaicExplode(struct(ColumnAdapter(exprs(0)), ColumnAdapter(exprs(1))).expr)
    )
    registry.registerFunction(FunctionIdentifier("h3_mosaicfill", database), (exprs: Seq[Expression]) => H3_MosaicFill(exprs(0), exprs(1)))
    registry.registerFunction(FunctionIdentifier("h3_point_index", database), (exprs: Seq[Expression]) => H3_PointIndex(exprs(0), exprs(1), exprs(2)))
    registry.registerFunction(FunctionIdentifier("h3_polyfill", database), (exprs: Seq[Expression]) => H3_Polyfill(exprs(0), exprs(1)))

    //DataType keywords are needed at checkInput execution time.
    //They cant be passed as Expressions to ConvertTo Expression.
    //Instead they are passed as String instances and for SQL
    //parser purposes separate method names are defined.
    registry.registerFunction(FunctionIdentifier("convert_to_hex", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "hex"))
    registry.registerFunction(FunctionIdentifier("convert_to_wkt", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt"))
    registry.registerFunction(FunctionIdentifier("convert_to_wkb", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb"))
    registry.registerFunction(FunctionIdentifier("convert_to_coords", database), (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords"))
    registry.registerFunction(FunctionIdentifier("st_xmax", database), (exprs: Seq[Expression]) => ST_MinMaxXY(exprs(0), "X", "MAX"))
    registry.registerFunction(FunctionIdentifier("st_xmin", database), (exprs: Seq[Expression]) => ST_MinMaxXY(exprs(0), "X", "MIN"))
    registry.registerFunction(FunctionIdentifier("st_ymax", database), (exprs: Seq[Expression]) => ST_MinMaxXY(exprs(0), "Y", "MAX"))
    registry.registerFunction(FunctionIdentifier("st_ymin", database), (exprs: Seq[Expression]) => ST_MinMaxXY(exprs(0), "Y", "MIN"))
    registry.registerFunction(FunctionIdentifier("st_isvalid", database), (exprs: Seq[Expression]) => ST_IsValid(exprs(0)))
  }

  def convert_to(inGeom: Column, outDataType: String): Column = ColumnAdapter(ConvertTo(inGeom.expr, outDataType))
  def as_hex(inGeom: Column): Column = ColumnAdapter(AsHex(inGeom.expr))
  def h3_mosaic_explode(geom: Column, resolution: Column): Column = ColumnAdapter(H3_MosaicExplode(struct(geom, resolution).expr))
  def h3_mosaic_explode(geom: Column, resolution: Int): Column = ColumnAdapter(H3_MosaicExplode(struct(geom, lit(resolution)).expr))
  def h3_mosaicfill(geom: Column, resolution: Column): Column = ColumnAdapter(H3_MosaicFill(geom.expr, resolution.expr))
  def h3_mosaicfill(geom: Column, resolution: Int): Column = ColumnAdapter(H3_MosaicFill(geom.expr, lit(resolution).expr))
  def h3_point_index(lat: Column, lng: Column, resolution: Column): Column = ColumnAdapter(H3_PointIndex(lat.expr, lng.expr, resolution.expr))
  def h3_point_index(lat: Column, lng: Column, resolution: Int): Column = ColumnAdapter(H3_PointIndex(lat.expr, lng.expr, lit(resolution).expr))
  def h3_polyfill(geom: Column, resolution: Column): Column = ColumnAdapter(H3_Polyfill(geom.expr, resolution.expr))
  def h3_polyfill(geom: Column, resolution: Int): Column = ColumnAdapter(H3_Polyfill(geom.expr, lit(resolution).expr))
  def flatten_polygons(geom: Column): Column = ColumnAdapter(FlattenPolygons(geom.expr))
  def st_xmax(geom: Column): Column = ColumnAdapter(ST_MinMaxXY(geom.expr, "X", "MAX"))
  def st_xmin(geom: Column): Column = ColumnAdapter(ST_MinMaxXY(geom.expr, "X", "MIN"))
  def st_ymax(geom: Column): Column = ColumnAdapter(ST_MinMaxXY(geom.expr, "Y", "MAX"))
  def st_ymin(geom: Column): Column = ColumnAdapter(ST_MinMaxXY(geom.expr, "Y", "MIN"))
  def st_isvalid(geom: Column): Column = ColumnAdapter(ST_IsValid(geom.expr))
}
