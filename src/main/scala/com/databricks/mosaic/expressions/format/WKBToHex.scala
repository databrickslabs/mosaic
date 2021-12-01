package com.databricks.mosaic.expressions.format

import com.databricks.mosaic.expressions.format.Conversions.wkb2hex
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Returns the wkb hex string representation.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(a);
       "00001005FA...00A" // random hex content provided for illustration only
  """,
  since = "1.0")
case class WKBToHex(wkb_bytes: Expression) extends UnaryExpression with ExpectsInputTypes with NullIntolerant with CodegenFallback {

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override def dataType: DataType = StringType

  override def toString: String = s"wkb_to_hex($wkb_bytes)"

  override def nullSafeEval(input1: Any): Any = {
    val hex = wkb2hex(input1)
    hex
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
    val res = WKBToHex(asArray(0))
    res.copyTagsFrom(this)
    res
  }

  override def child: Expression = wkb_bytes
}
