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
  * Base class for all unary geometry expressions that require 1 additional
  * argument. It provides the boilerplate for creating a function builder for a
  * given expression. It minimises amount of code needed to create a new
  * expression. The term unary refers to number of input geometries. By
  * convention the number of arguments will be handled via number in the class
  * name.
  *
  * @param geometryExpr
  *   The expression for the geometry.
  * @param argExpr
  *   The expression for the argument.
  * @param returnsGeometry
  *   Whether the expression returns a geometry or not.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class UnaryVector1ArgExpression[T <: Expression: ClassTag](
    geometryExpr: Expression,
    argExpr: Expression,
    returnsGeometry: Boolean,
    expressionConfig: MosaicExpressionConfig
) extends BinaryExpression
      with VectorExpression
      with NullIntolerant
      with Serializable {

    override def left: Expression = geometryExpr

    override def right: Expression = argExpr

    override def geometryAPI: GeometryAPI = getGeometryAPI(expressionConfig)

    /**
      * The function to be overriden by the extending class. It is called when
      * the expression is evaluated. It provides the vector geometry to the
      * expression. It abstracts spark serialization from the caller.
      * @param geometry
      *   The geometry.
      * @param arg
      *   The argument.
      * @return
      *   A result of the expression.
      */
    def geometryTransform(geometry: MosaicGeometry, arg: Any): Any

    /**
      * Evaluation of the expression. It evaluates the geometry and deserialises
      * the geometry.
      * @param geometryRow
      *   The row containing the geometry.
      *
      * @return
      *   The result of the expression.
      */
    override def nullSafeEval(geometryRow: Any, arg: Any): Any = {
        val geometry = geometryAPI.geometry(geometryRow, geometryExpr.dataType)
        val result = geometryTransform(geometry, arg)
        serialise(result, returnsGeometry, geometryExpr.dataType)
    }

    /**
      * The function to be overriden by the extending class. It is called when
      * the expression codegen is evaluated. It abstracts spark serialization
      * and deserialization from the caller codegen.
      * @param geometryRef
      *   The geometry reference.
      * @param argRef
      *   The argument reference.
      * @param ctx
      *   The codegen context.
      * @return
      *   A tuple containing the code and the reference to the result.
      */
    def geometryCodeGen(geometryRef: String, argRef: String, ctx: CodegenContext): (String, String)

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
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, leftEval, geometryExpr.dataType, geometryAPI)
              val mosaicGeomRef = mosaicGeometryRef(geomInRef)
              val (expressionCode, resultRef) = geometryCodeGen(mosaicGeomRef, rightEval, ctx)
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
