package com.databricks.labs.mosaic.expressions.geometry.base

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}

import scala.reflect.ClassTag

/**
  * Base class for all unary geometry expressions. It provides the boilerplate
  * for creating a function builder for a given expression. It minimises amount
  * of code needed to create a new expression.
  *
  * @param geometryExpr
  *   The expression for the geometry.
  * @param returnsGeometry
  *   Whether the expression returns a geometry or not.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class UnaryVectorExpression[T <: Expression: ClassTag](
    geometryExpr: Expression,
    returnsGeometry: Boolean,
    expressionConfig: MosaicExpressionConfig
) extends UnaryExpression
      with VectorExpression
      with NullIntolerant
      with Serializable {

    override def child: Expression = geometryExpr

    override def geometryAPI: GeometryAPI = getGeometryAPI(expressionConfig)

    /**
      * The function to be overriden by the extending class. It is called when
      * the expression is evaluated. It provides the vector geometry to the
      * expression. It abstracts spark serialization from the caller.
      * @param geometry
      *   The geometry.
      * @return
      *   A result of the expression.
      */
    def geometryTransform(geometry: MosaicGeometry): Any

    /**
      * Evaluation of the expression. It evaluates the geometry and deserialises
      * the geometry.
      * @param geometryRow
      *   The row containing the geometry.
      *
      * @return
      *   The result of the expression.
      */
    override def nullSafeEval(geometryRow: Any): Any = {
        val geometry = geometryAPI.geometry(geometryRow, geometryExpr.dataType)
        val result = geometryTransform(geometry)
        serialise(result, returnsGeometry, geometryExpr.dataType)
    }

    /**
      * The function to be overriden by the extending class. It is called when
      * the expression codegen is evaluated. It abstracts spark serialization
      * and deserialization from the caller codegen.
      * @param mosaicGeometryRef
      *   The reference to mosaic geometry.
      * @param ctx
      *   The codegen context.
      * @return
      *   A tuple containing the code and the reference to the result.
      */
    def geometryCodeGen(mosaicGeometryRef: String, ctx: CodegenContext): (String, String)

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 1, expressionConfig)

    override def withNewChildInternal(
        newFirst: Expression
    ): Expression = makeCopy(Array(newFirst))

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
          eval => {
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, eval, geometryExpr.dataType, geometryAPI)
              val mosaicGeomRef = mosaicGeometryRef(geomInRef)
              val (expressionCode, resultRef) = geometryCodeGen(mosaicGeomRef, ctx)
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
