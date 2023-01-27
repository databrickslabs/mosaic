package com.databricks.labs.mosaic.expressions.base

import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.reflect.ClassTag
import scala.util.Try

/**
  * GenericExpressionFactory is a factory that creates a function builder for a
  * given expression. It handles the boilerplate code needed to create a
  * function builder for a given expression. The functions reflect the types and
  * identify the correct constructors to be used.
  */
object GenericExpressionFactory {

    /**
      * Implements the makeCopy in a generic way so we dont need to repeat the
      * same code over and over again.
      * @param toCopy
      *   The expression to copy.
      * @param newArgs
      *   The new arguments for the expression.
      * @param nChildren
      *   The number of children expressions the expression has in the logical
      *   tree.
      * @param expressionConfig
      *   Additional arguments for the expression (expressionConfigs).
      * @tparam T
      *   The type of the expression.
      * @return
      *   A copy of the expression.
      */
    def makeCopyImpl[T <: Expression: ClassTag](
        toCopy: Expression,
        newArgs: Array[AnyRef],
        nChildren: Int,
        expressionConfig: MosaicExpressionConfig
    ): Expression = {
        val newInstance = construct[T](newArgs.take(nChildren).map(_.asInstanceOf[Expression]), expressionConfig)
        newInstance.copyTagsFrom(toCopy)
        newInstance
    }

    /**
      * Constructs an expression with the given arguments. It identifies the
      * correct constructor to be used.
      * @param args
      *   The arguments for the expression.
      * @param expressionConfig
      *   Additional arguments for the expression (expressionConfigs).
      * @tparam T
      *   The type of the expression.
      * @return
      *   An instance of the expression.
      */
    def construct[T <: Expression: ClassTag](args: Array[_ <: Expression], expressionConfig: MosaicExpressionConfig): Expression = {
        val clazz = implicitly[ClassTag[T]].runtimeClass
        val allArgs = args ++ Seq(expressionConfig)
        val constructors = clazz.getConstructors

        constructors
            .map(constructor =>
                Try {
                    val argClasses = constructor.getParameterTypes
                    val castedArgs = allArgs
                        .take(argClasses.length)
                        .zip(argClasses)
                        .map { case (arg, tpe) => tpe.cast(arg) }
                        .toSeq
                        .asInstanceOf[Seq[AnyRef]]
                    constructor.newInstance(castedArgs: _*)
                }
            )
            .filter(_.isSuccess)
            .head
            .get
            .asInstanceOf[Expression]
    }

    /**
      * Creates a function builder for a given expression. It identifies the
      * correct constructor to be used.
      * @param expressionConfig
      *   Additional arguments for the expression (expressionConfigs).
      * @tparam T
      *   The type of the expression.
      * @return
      *   A function builder for the expression.
      */
    def getBaseBuilder[T <: Expression: ClassTag](nChildren: Int, expressionConfig: MosaicExpressionConfig): FunctionBuilder =
        (children: Seq[Expression]) => GenericExpressionFactory.construct[T](children.take(nChildren).toArray, expressionConfig)

}
