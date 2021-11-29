package com.databricks.mosaic.expressions.format

import com.databricks.mosaic.expressions.format.Conversions.{geom2wkb, wkt2geom}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.io.{WKBWriter, WKTReader}

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Returns the byte array representation of a geometry.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(a);
       [FE 00 05 ... AA] // random byte content provided for illustration purpose only.
  """,
  since = "3.1.0")
case class WKTToWKB(wkt_text: Expression) extends UnaryExpression with ExpectsInputTypes with NullIntolerant with CodegenFallback {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def dataType: DataType = BinaryType

  override def toString: String = s"wkb_from_wkt($wkt_text)"

  override def nullSafeEval(input1: Any): Any = {
    val geom = wkt2geom(input1)
    val wkb = geom2wkb(geom)
    wkb
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
    val res = WKTToWKB(asArray(0))
    res.copyTagsFrom(this)
    res
  }

  override def child: Expression = wkt_text
}
