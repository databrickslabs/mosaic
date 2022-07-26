package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BooleanType, DataType}

import scala.util.Try

case class ST_IsValid(inputGeom: Expression, geometryAPIName: String) extends UnaryExpression with NullIntolerant {

    override def child: Expression = inputGeom

    override def dataType: DataType = BooleanType

    override def nullSafeEval(input1: Any): Any = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        // Invalid format should not crash the execution
        Try {
            val geom = geometryAPI.geometry(input1, inputGeom.dataType)
            geom.isValid
        }.getOrElse(false)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = ST_IsValid(asArray(0), geometryAPIName)
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
              val geometryIsValidStatement = geometryAPI.geometryIsValidCode
              // Invalid format should not crash the execution
              s"""
                 |try {
                 |$inCode
                 |${ev.value} = $geomInRef.$geometryIsValidStatement;
                 |} catch (Exception e) {
                 | ${ev.value} = false;
                 |}
                 |""".stripMargin
          }
        )

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(inputGeom = newChild)

}

object ST_IsValid {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_GeometryType].getCanonicalName,
          db.orNull,
          "st_isvalid",
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
          "predicate_funcs",
          "1.0",
          "",
          "built-in"
        )

}
