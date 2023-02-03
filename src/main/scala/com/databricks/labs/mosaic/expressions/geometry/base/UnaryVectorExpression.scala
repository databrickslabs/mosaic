package com.databricks.labs.mosaic.expressions.geometry.base

import com.databricks.labs.mosaic.codegen.format.ConvertToCodeGen
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemID}
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
      with NullIntolerant
      with Serializable {

    val indexSystem: IndexSystem = IndexSystemID.getIndexSystem(IndexSystemID(expressionConfig.getIndexSystem))
    val geometryAPI: GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    override def child: Expression = geometryExpr

    def mosaicGeometryRef(geometryRef: String): String = {
        s"${geometryAPI.mosaicGeometryClass}.apply($geometryRef)"
    }

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

    def serialise(result: Any): Any = {
        if (returnsGeometry) {
            geometryAPI.serialize(result.asInstanceOf[MosaicGeometry], geometryExpr.dataType)
        } else {
            result
        }
    }

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
        serialise(result)
    }

    def geometryCodeGen(geometryRef: String, ctx: CodegenContext): (String, String)

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 2, expressionConfig)

    override def withNewChildInternal(
        newFirst: Expression
    ): Expression = makeCopy(Array(newFirst))

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
        nullSafeCodeGen(
          ctx,
          ev,
          eval => {
              val (inCode, geomInRef) = ConvertToCodeGen.readGeometryCode(ctx, eval, geometryExpr.dataType, geometryAPI)
              val (expressionCode, resultRef) = geometryCodeGen(geomInRef, ctx)
              val (serialiseCode, serialisedRef) =
                  if (returnsGeometry) {
                      ConvertToCodeGen.writeGeometryCode(ctx, resultRef, geometryExpr.dataType, geometryAPI)
                  } else {
                      ("", resultRef) // noop code
                  }
              geometryAPI.codeGenTryWrap(s"""
                                            |$inCode
                                            |$expressionCode
                                            |$serialiseCode
                                            |${ev.value} = $serialisedRef;
                                            |""".stripMargin)
          }
        )

}
