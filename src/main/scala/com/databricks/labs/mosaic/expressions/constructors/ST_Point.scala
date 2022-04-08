package com.databricks.labs.mosaic.expressions.constructors

import com.databricks.labs.mosaic.core.types.InternalGeometryType
import com.databricks.labs.mosaic.core.types.model._

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

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
        val point = new InternalGeometry(GeometryTypeEnum.POINT.id, 0, Array(Array(coord)), Array(Array()))
        point.serialize
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_Point(asArray(0), asArray(1))
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(xVal = newLeft, yVal = newRight)

}

object ST_Point {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_Point].getCanonicalName,
          db.orNull,
          "st_point",
          """
            |    _FUNC_(expr1, expr2) - Creates a new Point geometry from X and Y, where both values are DoubleType.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(A, B);
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
