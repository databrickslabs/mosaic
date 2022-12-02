package com.databricks.labs.mosaic.expressions.base

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

abstract class GeometryTransformBinaryExpression[T <: Expression: ClassTag](inputGeom: Expression, param1: Expression, param2: Expression)
    extends TernaryExpression
      with NullIntolerant
      with Serializable {

    protected val geometryAPI: GeometryAPI = MosaicContext.geometryAPI()

    override def first: Expression = inputGeom

    override def second: Expression = param1

    override def third: Expression = param2

    /** Output Data Type */
    override def dataType: DataType = inputGeom.dataType

    def geomTransform(geometry: MosaicGeometry, param1: Any, param2: Any): Any

    def geomTransformCodeGen(ctx: CodegenContext, geomRef: String, param1Ref: String, param2Ref: String): (String, String)

    override def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
        val geom = geometryAPI.geometry(input1, inputGeom.dataType)
        val result = geomTransform(geom, input2, input3).asInstanceOf[MosaicGeometry]
        geometryAPI.serialize(result, dataType)
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 3)

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          (geomEval, param1Eval, param2Eval) => {
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, geomEval, dataType, geometryAPI)
              val (resultCode, resultRef) = geomTransformCodeGen(ctx, geomInRef, param1Eval, param2Eval)
              val (outCode, geomOutRef) = ConvertToCodeGen.writeGeometryCode(ctx, resultRef, dataType, geometryAPI)
              geometryAPI.codeGenTryWrap(s"""
                                            |$inCode
                                            |$resultCode
                                            |$outCode
                                            |${ev.value} = $geomOutRef;
                                            |""".stripMargin)
          }
        )

    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = {
        val newArgs = newFirst :: newSecond :: newThird :: Nil
        makeCopy(newArgs.toArray)
    }

}
