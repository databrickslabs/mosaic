package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class ST_Transform(inputGeom: Expression, srid: Expression, geometryAPIName: String)
    extends BinaryExpression
      with NullIntolerant
      with CodegenFallback
      with RequiresCRS {

    override def nullSafeEval(input1: Any, input2: Any): Any = {
        checkEncoding(inputGeom.dataType)
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        val transformedGeom = geom.transformCRSXY(input2.asInstanceOf[Int])
        geometryAPI.serialize(transformedGeom, dataType)
    }

    /** Output Data Type */
    override def dataType: DataType = inputGeom.dataType

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_Transform(asArray(0), asArray(1), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def left: Expression = inputGeom

    override def right: Expression = srid

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(inputGeom = newLeft, srid = newRight)

}
object ST_Transform {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_Transform].getCanonicalName,
          db.orNull,
          "ST_Transform",
          """
            |    _FUNC_(expr1, expr2) - Reproject a geometry to a different spatial reference system.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, b);
            |        POINT (1 1)
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )
}
