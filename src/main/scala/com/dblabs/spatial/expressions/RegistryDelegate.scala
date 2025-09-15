package com.dblabs.spatial.expressions

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry

case class RegistryDelegate(registry: FunctionRegistry, prefix: Option[String] = None) {

    private def getName(companion: WithExpressionInfo): String = {
        prefix.map(p => s"${p}_${companion.name}").getOrElse(companion.name)
    }

    def register(companion: WithExpressionInfo): Unit = {
        val name = getName(companion)
        registry.registerFunction(
          FunctionIdentifier(name),
          companion.builder(),
          source = "built-in"
        )
    }

    // For future use with ExpressionConfig
    //    def register(companion: WithExpressionInfo, ec: ExpressionConfig): Unit = {
    //        val name = getName(companion)
    //        registry.registerFunction(
    //          FunctionIdentifier(name),
    //          companion.builder(ec),
    //          source = "built-in"
    //        )
    //    }

}
