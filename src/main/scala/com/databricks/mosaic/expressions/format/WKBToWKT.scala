package com.databricks.mosaic.expressions.format

import com.databricks.mosaic.expressions.format.Conversions.{geom2wkt, wkb2geom}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.io.{WKBReader, WKBWriter, WKTWriter}

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Returns the wkt string representation.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(a);
       "POLYGON(0 0, ...)"
  """,
  since = "3.1.0")
case class WKBToWKT(wkb_bytes: Expression) extends UnaryExpression with ExpectsInputTypes with NullIntolerant with CodegenFallback {

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override def dataType: DataType = StringType

  override def toString: String = s"wkb_to_wkt($wkb_bytes)"

  override def nullSafeEval(input1: Any): Any = {
    val geom = wkb2geom(input1)
    geom2wkt(geom)
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
    val res = WKBToWKT(asArray(0))
    res.copyTagsFrom(this)
    res
  }

  override def child: Expression = wkb_bytes
}
