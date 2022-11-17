package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.locationtech.jts.geom.util.GeometryTransformer

abstract class BinarySpatioTemporalPredicateExpression(f: (MosaicGeometry, MosaicGeometry) => Boolean) extends BinaryExpression {

    def geometryAPI: GeometryAPI

    override def dataType: DataType = BooleanType

    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val geom1 = geometryAPI.geometry(input1, left.dataType)
        val geom2 = geometryAPI.geometry(input2, right.dataType)
        f(geom1, geom2)
    }

}

case class ST_Contains(left: Expression, right: Expression, geometryAPIName: String)
    extends BinarySpatioTemporalPredicateExpression(f = (geom1, geom2) => geom1.contains(geom2))
      with NullIntolerant {

    override def geometryAPI: GeometryAPI = GeometryAPI(geometryAPIName)

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          (leftEval, rightEval) => {
              val geometryAPI = GeometryAPI.apply(geometryAPIName)
              val (leftInCode, leftGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, left.dataType, geometryAPI)
              val (rightInCode, rightGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, rightEval, right.dataType, geometryAPI)
              geometryAPI.codeGenTryWrap(s"""
                                            |$leftInCode
                                            |$rightInCode
                                            |${ev.value} = $leftGeomInRef.contains($rightGeomInRef);
                                            |""".stripMargin)
          }
        )

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        copy(left = newLeft, right = newRight)

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
