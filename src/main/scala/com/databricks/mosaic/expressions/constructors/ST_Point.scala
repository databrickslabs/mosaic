package com.databricks.mosaic.expressions.constructors

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

import com.databricks.mosaic.core.types.InternalGeometryType
import com.databricks.mosaic.core.types.model._

@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Creates a new Point geometry from X and Y, where both values are DoubleType.",
  examples = """
    Examples:
      > SELECT _FUNC_(A, B);
  """,
  since = "1.0"
)
case class ST_Point(xVal: Expression, yVal: Expression) extends BinaryExpression with NullIntolerant with CodegenFallback {

    override def left: Expression = xVal

    override def right: Expression = yVal

    override def dataType: DataType = InternalGeometryType

    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val coordArray = Seq(input1, input2).map(_.asInstanceOf[Double])
        val coord = InternalCoord(ArrayData.toArrayData(coordArray))
        val point = new InternalGeometry(GeometryTypeEnum.POINT.id, Array(Array(coord)), Array(Array()))
        point.serialize
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_Point(asArray(0), asArray(1))
        res.copyTagsFrom(this)
        res
    }

}
