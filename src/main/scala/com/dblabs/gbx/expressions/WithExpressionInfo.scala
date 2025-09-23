package com.dblabs.gbx.expressions

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder

/**
  * WithExpressionInfo is a trait that defines the interface for adding
  * expression to spark SQL. Any expression that needs to be added to spark SQL
  * should extend this trait.
  */
trait WithExpressionInfo {

    def name: String

    def builder(): FunctionBuilder = {
        throw new IllegalAccessException("Builder not implemented")
    }

    //    // For future use with ExpressionConfig
    //    def builder(expressionConfig: ExpressionConfig): FunctionBuilder = {
    //        throw new IllegalAccessException("Builder not implemented")
    //    }

}
