package com.databricks.mosaic.expressions.geometry

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, IntegerType}

import com.databricks.mosaic.core.geometry.api.GeometryAPI

case class ST_NumPoints(inputGeom: Expression, geometryAPIName: String) extends UnaryExpression with NullIntolerant with CodegenFallback {

    /**
      * ST_NumPoints expression returns the number of points for a given geometry.
      * expression.
      */

    override def child: Expression = inputGeom

    /** Output Data Type */
    override def dataType: DataType = IntegerType

    override def nullSafeEval(input1: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        geom.numPoints
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = ST_NumPoints(asArray(0), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inputGeom = newChild)

}

object ST_NumPoints {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_NumPoints].getCanonicalName,
          db.orNull,
          "st_numpoints",
          """
            |    _FUNC_(expr1) - Returns the number of points of the geometry.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        15
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
