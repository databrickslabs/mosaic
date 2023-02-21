package com.databricks.labs.mosaic.expressions.geometry.base

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}

import scala.reflect.ClassTag

/**
  * Base class for all unary geometry expressions. It provides the boilerplate
  * for creating a function builder for a given expression. It minimises amount
  * of code needed to create a new expression.
  *
  * @param leftGeometryExpr
  *   The expression for the left/first geometry.
  * @param rightGeometryExpr
  *   The expression for the right/second geometry.
  * @param returnsGeometry
  *   Whether the expression returns a geometry or not.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class BinaryVectorExpression[T <: Expression: ClassTag](
    leftGeometryExpr: Expression,
    rightGeometryExpr: Expression,
    returnsGeometry: Boolean,
    expressionConfig: MosaicExpressionConfig
) extends BinaryExpression
      with VectorExpression
      with NullIntolerant
      with Serializable {

    override def left: Expression = leftGeometryExpr

    override def right: Expression = rightGeometryExpr

    override def geometryAPI: GeometryAPI = getGeometryAPI(expressionConfig)

    /**
      * The function to be overriden by the extending class. It is called when
      * the expression is evaluated. It provides the vector geometries to the
      * expression. It abstracts spark serialization from the caller.
      * @param leftGeometry
      *   The left/first geometry.
      * @param rightGeometry
      *   The right/second geometry.
      * @return
      *   A result of the expression.
      */
    def geometryTransform(leftGeometry: MosaicGeometry, rightGeometry: MosaicGeometry): Any

    /**
      * Evaluation of the expression. It evaluates the geometry and deserialises
      * the geometry.
      * @param leftGeometryRow
      *   The row containing the left/first geometry.
      * @param rightGeometryRow
      *   The row containing the right/second geometry.
      * @return
      *   The result of the expression.
      */
    override def nullSafeEval(leftGeometryRow: Any, rightGeometryRow: Any): Any = {
        val leftGeometry = geometryAPI.geometry(leftGeometryRow, leftGeometryExpr.dataType)
        val rightGeometry = geometryAPI.geometry(rightGeometryRow, rightGeometryExpr.dataType)
        val result = geometryTransform(leftGeometry, rightGeometry)
        serialise(result, returnsGeometry, leftGeometryExpr.dataType)
    }

    /**
      * The function to be overriden by the extending class. It is called when
      * the expression codegen is evaluated. It abstracts spark serialization
      * and deserialization from the caller codegen.
      * @param leftMosaicGeometryRef
      *   The left/first mosaic geometry reference.
      * @param rightMosaicGeometryRef
      *   The right/second mosaic geometry reference.
      * @param ctx
      *   The codegen context.
      * @return
      *   A tuple containing the code and the reference to the result.
      */
    def geometryCodeGen(leftMosaicGeometryRef: String, rightMosaicGeometryRef: String, ctx: CodegenContext): (String, String)

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 2, expressionConfig)

    override def withNewChildrenInternal(
        newFirst: Expression,
        newSecond: Expression
    ): Expression = makeCopy(Array(newFirst, newSecond))

    /**
      * The actual codegen implementation. It abstracts spark serialization and
      * deserialization from the caller codegen. The extending class does not
      * need to override this method.
      * @param ctx
      *   The codegen context.
      * @param ev
      *   The expression code.
      * @return
      *   The result of the expression.
      */
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          (leftEval, rightEval) => {
              val (leftInCode, leftGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, leftGeometryExpr.dataType, geometryAPI)
              val (rightInCode, rightGeomInRef) = ConvertToCodeGen.readGeometryCode(ctx, rightEval, rightGeometryExpr.dataType, geometryAPI)
              val leftMosaicGeomRef = mosaicGeometryRef(leftGeomInRef)
              val rightMosaicGeomRef = mosaicGeometryRef(rightGeomInRef)
              val (expressionCode, resultRef) = geometryCodeGen(leftMosaicGeomRef, rightMosaicGeomRef, ctx)
              val (serialiseCode, serialisedRef) = serialiseCodegen(resultRef, returnsGeometry, leftGeometryExpr.dataType, ctx)
              geometryAPI.codeGenTryWrap(s"""
                                            |$leftInCode
                                            |$rightInCode
                                            |$expressionCode
                                            |$serialiseCode
                                            |${ev.value} = $serialisedRef;
                                            |""".stripMargin)
          }
        )

}
