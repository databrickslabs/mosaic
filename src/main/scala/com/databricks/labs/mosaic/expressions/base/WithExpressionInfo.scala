package com.databricks.labs.mosaic.expressions.base

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}

import scala.reflect.ClassTag

trait WithExpressionInfo {

    def name: String

    def database: Option[String] = None

    def usage: String = ""

    def example: String = ""

    def group: String = "misc_funcs"

    def builder(args: Any*): FunctionBuilder

    final def getExpressionInfo[T <: Expression: ClassTag](database: Option[String] = None): ExpressionInfo = {
        val clazz = implicitly[ClassTag[T]].runtimeClass
        new ExpressionInfo(
            clazz.getCanonicalName,
            database.getOrElse(this.database.orNull),
            name,
            usage,
            "",
            example,
            "",
            group,
            "1.0",
            "",
            "built-in"
        )
    }

}
