package com.databricks.labs.mosaic.expressions.base

import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}

import scala.reflect.ClassTag

/**
  * WithExpressionInfo is a trait that defines the interface for adding
  * expression to spark SQL. Any expression that needs to be added to spark SQL
  * should extend this trait.
  */
trait WithExpressionInfo {

    def name: String

    def database: Option[String] = None

    def usage: String = ""

    def example: String = ""

    def group: String = "misc_funcs"

    /**
      * Returns the expression builder (parser for spark SQL).
      * @return
      *   An expression builder.
      */
    def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder

    /**
      * Returns the expression info for the expression based on the expression's
      * type. Simplifies the amount of boilerplate code needed to add an
      * expression to spark SQL.
      * @return
      *   An ExpressionInfo object.
      */
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
