package com.databricks.labs.mosaic.expressions.base

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.reflect.ClassTag

object GenericExpressionFactory {

    def makeCopyImpl[T <: Expression: ClassTag](toCopy: Expression, newArgs: Array[AnyRef], nChildren: Int): Expression = {
        val newInstance = construct[T](newArgs.take(nChildren).map(_.asInstanceOf[Expression]))
        newInstance.copyTagsFrom(toCopy)
        newInstance
    }

    def construct[T <: Expression: ClassTag](args: Array[_ <: Expression]): Expression = {
        val clazz = implicitly[ClassTag[T]].runtimeClass
        val argsClasses = Array.fill(args.length)(classOf[Expression])
        val res = clazz.getDeclaredConstructor(argsClasses: _*)
        val newInstance = res.newInstance(args: _*).asInstanceOf[Expression]
        newInstance
    }

    def getBaseBuilder[T <: Expression: ClassTag](nChildren: Int): FunctionBuilder =
        (children: Seq[Expression]) => GenericExpressionFactory.construct[T](children.take(nChildren).toArray)

}
