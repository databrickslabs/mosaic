package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, DoubleType}

case class ST_Length(inputGeom: Expression, geometryAPIName: String) extends UnaryExpression with NullIntolerant {

    def dataType: DataType = DoubleType

    def child: Expression = inputGeom

    override def nullSafeEval(input1: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        geom.getLength
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = ST_Length(asArray(0), geometryAPIName)
        res.copyTagsFrom(this)
        res
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          leftEval => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, inputGeom.dataType, geometryAPI)

              geometryAPIName match {
                  case "OGC" => s"""
                                   |$inCode
                                   |${ev.value} = $geomInRef.getEsriGeometry().calculateLength2D();
                                   |""".stripMargin
                  case "JTS" => s"""
                                   |try {
                                   |$inCode
                                   |${ev.value} = $geomInRef.getLength();
                                   |} catch (Exception e) {
                                   | throw e;
                                   |}
                                   |""".stripMargin

              }
          }
        )

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inputGeom = newChild)

}

object ST_Length {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String], name: String): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_Length].getCanonicalName,
          db.orNull,
          name,
          """
            |    _FUNC_(expr1) - Returns the validity for a given geometry.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        true
            |  """.stripMargin,
          "",
          "misc_funcs",
          "1.0",
          "",
          "built-in"
        )
}
