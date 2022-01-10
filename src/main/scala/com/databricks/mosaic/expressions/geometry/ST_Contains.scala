package com.databricks.mosaic.expressions.geometry

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, NullIntolerant}
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.apache.spark.sql.types.BooleanType

import com.databricks.mosaic.core.geometry.api.GeometryAPI

@ExpressionDescription(
  usage = "_FUNC_(expr1) - Returns true if A contains B.",
  examples =
    """
    Examples:
      > SELECT _FUNC_(A, B);
       true
  """,
  since = "1.0"
)
case class ST_Contains(leftGeom: Expression, rightGeom: Expression, geometryAPIName: String)
  extends BinaryExpression
    with NullIntolerant
    with CodegenFallback {
  override def left: Expression = leftGeom

  override def right: Expression = rightGeom

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val geometryAPI = GeometryAPI(geometryAPIName)
    val geom1 = geometryAPI.geometry(input1, leftGeom.dataType)
    val geom2 = geometryAPI.geometry(input2, rightGeom.dataType)
    geom1.contains(geom2)
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
    val res = ST_Contains(asArray(0), asArray(1), geometryAPIName)
    res.copyTagsFrom(this)
    res
  }
}
