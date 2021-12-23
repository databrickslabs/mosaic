package com.databricks.mosaic.expressions.geometry

import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  ExpressionDescription,
  NullIntolerant,
  UnaryExpression
}
import com.databricks.mosaic.core.types.any2geometry
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DoubleType

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Returns the length measurement of the geometry.",
  examples = """
    Examples:
      > SELECT _FUNC_(a);
       15.2512
  """,
  since = "1.0"
)
case class ST_Length(inputGeom: Expression)
    extends UnaryExpression
    with NullIntolerant
    with CodegenFallback {
  def dataType = DoubleType
  def child = inputGeom
  override def nullSafeEval(input1: Any): Any = {
    val geom = any2geometry(input1, inputGeom.dataType)
    geom.getLength
  }
  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
    val res = ST_Length(asArray(0))
    res.copyTagsFrom(this)
    res
  }
}
