package com.databricks.labs.mosaic.expressions.base

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.reflect.ClassTag
import scala.util.Try

object GenericExpressionFactory {

    def makeCopyImpl[T <: Expression: ClassTag](
        toCopy: Expression,
        newArgs: Array[AnyRef],
        nChildren: Int,
        additionalArgs: Any*
    ): Expression = {
        val newInstance = construct[T](newArgs.take(nChildren).map(_.asInstanceOf[Expression]), additionalArgs: _*)
        newInstance.copyTagsFrom(toCopy)
        newInstance
    }

    def construct[T <: Expression: ClassTag](args: Array[_ <: Expression], additionalArgs: Any*): Expression = {
        val clazz = implicitly[ClassTag[T]].runtimeClass
        val allArgs = args ++ additionalArgs.toSeq
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

    def getBaseBuilder[T <: Expression: ClassTag](nChildren: Int, additionalArgs: Any*): FunctionBuilder =
        (children: Seq[Expression]) => GenericExpressionFactory.construct[T](children.take(nChildren).toArray, additionalArgs: _*)

}
