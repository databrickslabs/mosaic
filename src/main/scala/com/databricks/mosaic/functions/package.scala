package com.databricks.mosaic

import com.databricks.mosaic.expressions.format._
import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{Column, SparkSession}

//noinspection ZeroIndexToHead
package object functions {

  def register(spark: SparkSession, database: Option[String] = None): Unit = {
    val registry = spark.sessionState.functionRegistry
    registry.registerFunction(FunctionIdentifier("wkb_to_hex", database), (exprs: Seq[Expression]) => WKBToHex(exprs(0)))
    registry.registerFunction(FunctionIdentifier("wkb_to_wkt", database), (exprs: Seq[Expression]) => WKBToWKT(exprs(0)))
    registry.registerFunction(FunctionIdentifier("wkt_to_hex", database), (exprs: Seq[Expression]) => WKTToHex(exprs(0)))
    registry.registerFunction(FunctionIdentifier("wkt_to_wkb", database), (exprs: Seq[Expression]) => WKTToWKB(exprs(0)))
    registry.registerFunction(FunctionIdentifier("hex_to_wkb", database), (exprs: Seq[Expression]) => HexToWKB(exprs(0)))
    registry.registerFunction(FunctionIdentifier("hex_to_wkt", database), (exprs: Seq[Expression]) => HexToWKT(exprs(0)))
  }


  def wkb_to_hex(left: Column): Column = ColumnAdapter(WKBToHex(left.expr))
  def wkb_to_wkt(left: Column): Column = ColumnAdapter(WKBToWKT(left.expr))
  def wkt_to_hex(left: Column): Column = ColumnAdapter(WKTToHex(left.expr))
  def wkt_to_wkb(left: Column): Column = ColumnAdapter(WKTToWKB(left.expr))
  def hex_to_wkb(left: Column): Column = ColumnAdapter(HexToWKB(left.expr))
  def hex_to_wkt(left: Column): Column = ColumnAdapter(HexToWKT(left.expr))

}
