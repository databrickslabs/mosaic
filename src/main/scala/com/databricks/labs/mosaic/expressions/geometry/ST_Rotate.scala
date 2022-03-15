package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.geometry.GeometryTransformationsCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

case class ST_Rotate(inputGeom: Expression, td: Expression, geometryAPIName: String) extends BinaryExpression with NullIntolerant {

    /**
      * ST_Rotate expression returns are covered by the
      * [[org.locationtech.jts.geom.Geometry]] instance extracted from inputGeom
      * expression.
      */

    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom = geometryAPI.geometry(input1, dataType)
        val tDist = input2.asInstanceOf[Double]
        val result = geom.rotate(tDist)
        geometryAPI.serialize(result, dataType)
    }

    /** Output Data Type */
    override def dataType: DataType = inputGeom.dataType

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(2).map(_.asInstanceOf[Expression])
        val res = ST_Rotate(asArray(0), asArray(1), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def left: Expression = inputGeom

    override def right: Expression = td

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          (leftEval, rightEval) => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val (code, result) = GeometryTransformationsCodeGen.rotate(ctx, leftEval, rightEval, inputGeom.dataType, geometryAPI)

              geometryAPIName match {
                  case "ESRI" => s"""
                                   |$code
                                   |${ev.value} = $result;
                                   |""".stripMargin
                  case "JTS" => s"""
                                   |try {
                                   |$code
                                   |${ev.value} = $result;
                                   |} catch (Exception e) {
                                   | throw e;
                                   |}
                                   |""".stripMargin

              }
          }
        )

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(inputGeom = newLeft, td = newRight)

}

object ST_Rotate {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String], name: String): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_Length].getCanonicalName,
          db.orNull,
          name,
          """
            |    _FUNC_(expr1) - Rotates a given geometry by td.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        13.23
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )
}
