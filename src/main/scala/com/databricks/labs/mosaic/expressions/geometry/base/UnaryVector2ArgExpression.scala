package com.databricks.labs.mosaic.expressions.geometry.base

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}

import scala.reflect.ClassTag

/**
  * Base class for all unary geometry expressions that require 2 additional
  * argument. It provides the boilerplate for creating a function builder for a
  * given expression. It minimises amount of code needed to create a new
  * expression. The term unary refers to number of input geometries. By
  * convention the number of arguments will be handled via number in the class
  * name.
  *
  * @param geometryExpr
  *   The expression for the geometry.
  * @param arg1Expr
  *   The expression for the first argument.
  * @param arg2Expr
  *   The expression for the second argument.
  * @param returnsGeometry
  *   Whether the expression returns a geometry or not.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class UnaryVector2ArgExpression[T <: Expression: ClassTag](
    geometryExpr: Expression,
    arg1Expr: Expression,
    arg2Expr: Expression,
    returnsGeometry: Boolean,
    expressionConfig: MosaicExpressionConfig
) extends TernaryExpression
      with VectorExpression
      with NullIntolerant
      with Serializable {

    override def first: Expression = geometryExpr

    override def second: Expression = arg1Expr

    override def third: Expression = arg2Expr

    override def geometryAPI: GeometryAPI = getGeometryAPI(expressionConfig)

    /**
      * The function to be overriden by the extending class. It is called when
      * the expression is evaluated. It provides the vector geometry to the
      * expression. It abstracts spark serialization from the caller.
      * @param geometry
      *   The geometry.
      * @param arg1
      *   The first argument.
      * @param arg2
      *   The second argument.
      * @return
      *   A result of the expression.
      */
    def geometryTransform(geometry: MosaicGeometry, arg1: Any, arg2: Any): Any

    /**
      * Evaluation of the expression. It evaluates the geometry and deserialises
      * the geometry.
      * @param geometryRow
      *   The row containing the geometry.
      * @param arg1
      *   The first argument.
      * @param arg2
      *   The second argument.
      * @return
      *   The result of the expression.
      */
    override def nullSafeEval(geometryRow: Any, arg1: Any, arg2: Any): Any = {
        val geometry = geometryAPI.geometry(geometryRow, geometryExpr.dataType)
        val result = geometryTransform(geometry, arg1, arg2)
        serialise(result, returnsGeometry, geometryExpr.dataType)
    }

    /**
      * The function to be overriden by the extending class. It is called when
      * the expression codegen is evaluated. It abstracts spark serialization
      * and deserialization from the caller codegen.
      * @param mosaicGeometryRef
      *   The reference to mosaic geometry.
      * @param arg1Ref
      *   The first argument reference.
      * @param arg2Ref
      *   The second argument reference.
      * @param ctx
      *   The codegen context.
      * @return
      *   A tuple containing the code and the reference to the result.
      */
    def geometryCodeGen(mosaicGeometryRef: String, arg1Ref: String, arg2Ref: String, ctx: CodegenContext): (String, String)

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 3, expressionConfig)

    override def withNewChildrenInternal(
        newFirst: Expression,
        newSecond: Expression,
        newThird: Expression
    ): Expression = makeCopy(Array(newFirst, newSecond, newThird))

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
          (geomEval, arg1Eval, arg2Eval) => {
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, geomEval, geometryExpr.dataType, geometryAPI)
              val mosaicGeomRef = mosaicGeometryRef(geomInRef)
              val (expressionCode, resultRef) = geometryCodeGen(mosaicGeomRef, arg1Eval, arg2Eval, ctx)
              val (serialiseCode, serialisedRef) = serialiseCodegen(resultRef, returnsGeometry, geometryExpr.dataType, ctx)
              geometryAPI.codeGenTryWrap(s"""
                                            |$inCode
                                            |$expressionCode
                                            |$serialiseCode
                                            |${ev.value} = $serialisedRef;
                                            |""".stripMargin)
          }
        )

}
