package com.databricks.mosaic.expressions.geometry

import org.apache.spark.sql.catalyst.expressions.{
  BinaryExpression,
  Expression,
  ExpressionDescription,
  NullIntolerant
}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DoubleType

import com.databricks.mosaic.core.types.any2geometry

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Return the Euclidean distance between A and B.",
  examples = """
    Examples:
      > SELECT _FUNC_(A, B);
       15.2512
  """,
  since = "1.0"
)
case class ST_Distance(leftGeom: Expression, rightGeom: Expression)
    extends BinaryExpression
    with NullIntolerant
    with CodegenFallback {
  override def left: org.apache.spark.sql.catalyst.expressions.Expression = leftGeom
  override def right: org.apache.spark.sql.catalyst.expressions.Expression = rightGeom
  override def dataType: org.apache.spark.sql.types.DataType = DoubleType
  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val geom1 = any2geometry(input1, leftGeom.dataType)
    val geom2 = any2geometry(input2, rightGeom.dataType)
    geom1.distance(geom2)
  }
  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
    val res = ST_Distance(asArray(0), asArray(1))
    res.copyTagsFrom(this)
    res
  }
}
