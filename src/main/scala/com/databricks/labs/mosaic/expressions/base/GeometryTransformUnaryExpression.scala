package com.databricks.labs.mosaic.expressions.base

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

abstract class GeometryTransformUnaryExpression[T <: Expression: ClassTag](inputGeom: Expression, param1: Expression)
    extends BinaryExpression
      with NullIntolerant
      with Serializable {

    protected val geometryAPI: GeometryAPI = MosaicContext.geometryAPI()

    override def left: Expression = inputGeom

    override def right: Expression = param1

    /** Output Data Type */
    override def dataType: DataType = inputGeom.dataType

    def geomTransform(geometry: MosaicGeometry, param1: Any): Any

    def geomTransformCodeGen(ctx: CodegenContext, geomRef: String, param1Ref: String): (String, String)

    override def nullSafeEval(input1: Any, input2: Any): Any = {
        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        val result = geomTransform(geom, input2).asInstanceOf[MosaicGeometry]
        geometryAPI.serialize(result, dataType)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 2)

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          (geomEval, param1Eval) => {
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, geomEval, inputGeom.dataType, geometryAPI)
              val (resultCode, resultRef) = geomTransformCodeGen(ctx, geomInRef, param1Eval)
                val (outCode, geomOutRef) = ConvertToCodeGen.writeGeometryCode(ctx, resultRef, dataType, geometryAPI)
              geometryAPI.codeGenTryWrap(s"""
                                            |$inCode
                                            |$resultCode
                                            |$outCode
                                            |${ev.value} = $geomOutRef;
                                            |""".stripMargin)
          }
        )

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
        val newArgs = newLeft :: newRight :: Nil
        makeCopy(newArgs.toArray)
    }

}
