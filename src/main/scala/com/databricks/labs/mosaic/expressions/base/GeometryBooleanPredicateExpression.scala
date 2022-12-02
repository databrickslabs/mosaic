package com.databricks.labs.mosaic.expressions.base

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{BooleanType, DataType}

import scala.reflect.ClassTag

abstract class GeometryBooleanPredicateExpression[T <: Expression: ClassTag](
    leftGeomExpr: Expression,
    rightGeomExpr: Expression
) extends BinaryExpression
      with NullIntolerant
      with Serializable {

    protected val geometryAPI: GeometryAPI = MosaicContext.geometryAPI()

    /**
      * ST_Area expression returns area covered by the
      * [[org.locationtech.jts.geom.Geometry]] instance extracted from inputGeom
      * expression.
      */

    override def left: Expression = leftGeomExpr

    override def right: Expression = rightGeomExpr

    /** Output Data Type */
    override def dataType: DataType = BooleanType

    def geomTransform(leftGeom: MosaicGeometry, rightGeom: MosaicGeometry): Boolean

    def geomTransformCodeGen(ctx: CodegenContext, leftGeomRef: String, rightGeomRef: String): (String, String)

    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val leftGeom = geometryAPI.geometry(input1, leftGeomExpr.dataType)
        val rightGeom = geometryAPI.geometry(input2, rightGeomExpr.dataType)
        geomTransform(leftGeom, rightGeom)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 2)

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
        nullSafeCodeGen(
          ctx,
          ev,
          (leftEval, rightEval) => {
              val (leftInCode, leftGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, leftGeomExpr.dataType, geometryAPI)
              val (rightInCode, rightGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, rightEval, rightGeomExpr.dataType, geometryAPI)
              val (resultCode, resultGeomRef) = geomTransformCodeGen(ctx, leftGeomInRef, rightGeomInRef)
              geometryAPI.codeGenTryWrap(s"""
                                            |$leftInCode
                                            |$rightInCode
                                            |$resultCode
                                            |${ev.value} = $resultGeomRef;
                                            |""".stripMargin)
          }
        )
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
        makeCopy(Array(newLeft, newRight))

}
