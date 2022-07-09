package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{ESRI, JTS}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BooleanType, DataType}

import scala.util.{Success, Try}

case class ST_Contains(leftGeom: Expression, rightGeom: Expression, geometryAPIName: String) extends BinaryExpression with NullIntolerant {

    override def left: Expression = leftGeom

    override def right: Expression = rightGeom

    override def dataType: DataType = BooleanType

    // noinspection DuplicatedCode
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

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          (leftEval, rightEval) => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              // TODO: code can be simplified if the function is registered and called 2 times
              // not merged into the same code block due to JTS IOException throwing
              // ESRI code will always remain simpler
              val tryIO = Try {
                  val (leftInCode, leftGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, leftGeom.dataType, geometryAPI)
                  val (rightInCode, rightGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, rightEval, rightGeom.dataType, geometryAPI)
                  ((leftInCode, leftGeomInRef), (rightInCode, rightGeomInRef))
              }
              (tryIO, geometryAPI) match {
                  case (
                        Success(((leftInCode, leftGeomInRef), (rightInCode, rightGeomInRef))),
                        ESRI
                      ) => s"""
                              |$leftInCode
                              |$rightInCode
                              |${ev.value} = $leftGeomInRef.contains($rightGeomInRef);
                              |""".stripMargin
                  case (
                        Success(((leftInCode, leftGeomInRef), (rightInCode, rightGeomInRef))),
                        JTS
                      ) => s"""
                              |try {
                              |$leftInCode
                              |$rightInCode
                              |${ev.value} = $leftGeomInRef.contains($rightGeomInRef);
                              |} catch (Exception e) {
                              | throw e;
                              |}
                              |""".stripMargin
                  case _ => throw new IllegalArgumentException(s"Geometry API unsupported: $geometryAPIName.")

              }
          }
        )

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(leftGeom = newLeft, rightGeom = newRight)

}

object ST_Contains {

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[ST_Contains].getCanonicalName,
          db.orNull,
          "st_contains",
          """
            |    _FUNC_(expr1) - Return the contains relationship between left and right.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(A, B);
            |        true
            |  """.stripMargin,
          "",
          "predicate_funcs",
          "1.0",
          "",
          "built-in"
        )

}
