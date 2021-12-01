package com.databricks.mosaic.expressions.format

import com.databricks.mosaic.expressions.format.Conversions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.io.{WKBReader, WKBWriter}

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Returns the byte array representation of a geometry.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(a);
       [FE 00 05 ... AA] // random byte content provided for illustration purpose only.
  """,
  since = "1.0")
case class HexToWKB(wkb_hex: Expression) extends UnaryExpression with ExpectsInputTypes with NullIntolerant with CodegenFallback {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def dataType: DataType = BinaryType

  override def toString: String = s"hex_to_wkb($wkb_hex)"

  override def nullSafeEval(input1: Any): Any = {
    val geom = hex2geom(input1)
    geom2wkb(geom)
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
    val res = HexToWKB(asArray(0))
    res.copyTagsFrom(this)
    res
  }

  override def child: Expression = wkb_hex
}
