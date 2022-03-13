package com.databricks.mosaic.expressions.geometry

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BooleanType, DataType}

import com.databricks.mosaic.core.geometry.api.GeometryAPI

case class ST_Intersects(leftGeom: Expression, rightGeom: Expression, geometryAPIName: String)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback {

    override def left: Expression = leftGeom

    override def right: Expression = rightGeom

    override def dataType: DataType = BooleanType

    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom1 = geometryAPI.geometry(input1, leftGeom.dataType)
        val geom2 = geometryAPI.geometry(input2, rightGeom.dataType)
        geom1.intersects(geom2)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_Intersects(asArray(0), asArray(1), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(leftGeom = newLeft, rightGeom = newRight)

}


object ST_Intersects {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
            classOf[ST_Intersects].getCanonicalName,
            db.orNull,
            "st_intersects",
            """
              |    _FUNC_(expr1, expr2) - Returns the intersects predicate of the two geometries.
            """.stripMargin,
            "",
            """
              |    Examples:
              |      > SELECT _FUNC_(a, b);
              |        POLYGON(...)
              |  """.stripMargin,
            "",
            "misc_funcs",
            "1.0",
            "",
            "built-in"
        )

}