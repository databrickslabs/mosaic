package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.geometry.GeometryTransformationsCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

case class ST_Scale(inputGeom: Expression, xd: Expression, yd: Expression, geometryAPIName: String)
    extends TernaryExpression
      with NullIntolerant {

    /**
      * ST_Scale expression returns are covered by the
      * [[org.locationtech.jts.geom.Geometry]] instance extracted from inputGeom
      * expression.
      */

    // noinspection DuplicatedCode
    override def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom = geometryAPI.geometry(input1, dataType)
        val xDist = input2.asInstanceOf[Double]
        val yDist = input3.asInstanceOf[Double]
        val output = geom.scale(xDist, yDist)
        geometryAPI.serialize(output, dataType)
    }

    /** Output Data Type */
    override def dataType: DataType = inputGeom.dataType

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(3).map(_.asInstanceOf[Expression])
        val res = ST_Scale(asArray(0), asArray(1), asArray(2), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override def first: Expression = inputGeom

    override def second: Expression = xd

    override def third: Expression = yd

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          (firstEval, secondEval, thirdEval) => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val (code, result) =
                  GeometryTransformationsCodeGen.scale(ctx, firstEval, secondEval, thirdEval, inputGeom.dataType, geometryAPI)

              geometryAPIName match {
                  case "OGC" => s"""
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

    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
        copy(inputGeom = newFirst, xd = newSecond, yd = newThird)

}

object ST_Scale {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String], name: String): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_Scale].getCanonicalName,
          db.orNull,
          name,
          """
            |    _FUNC_(expr1, xd, yd) - Returns a new geometry scaled using xd for x axis and yd for y axis.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a, xd, yd);
            |        POLYGON ((...))
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )

}
