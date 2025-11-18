package com.databricks.labs.gbx.expressions

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

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

    /* ExpressionInfo(String className, String db, String name, String usage, String arguments, String examples,
                      String note, String group, String since, String deprecated, String source)
    */
    def info(): ExpressionInfo = new ExpressionInfo(
        getImplementingClassFullName,
        db,
        name,
        getUsage,
        getExtendedUsage,
        getExamples,
        note,
        group,
        since,
        deprecated,
        source
    )

    private val source: String = "built-in"

    def since: String = "1.0"  // baseline

    private val db: String = "<default>"

    def usageArgs: String         // must override

    def description: String       // must override

    def extendedUsageArgs: String // must override

    def examples: String          // must override

    def extendedDescription: String = "" // additional

    def deprecated: String = ""

    def note: String = ""

    def group: String = ""

    // help expressions
    val _TILE_TYPE_ = "tile: <Raster Tile>"
    val _TILE_ARRAY_TYPE_ = "tiles: Array<Raster Tile>"
    val _TILE_RESULT_ = """{index_id: ..., raster: [00 01 10 ... 00], parentPath: "...", driver: "GTiff" }"""

    // for private functions
    val _FUNC_ = "_FUNC_" // swaps with `name`
    val _ARGS_ = "_ARGS_" // swaps with `usageArgs`


    private def getUsage: String = s"${name}(${usageArgs}) - ${description}"

    private def getExtendedUsageArgs: String = {
        if (extendedUsageArgs.isEmpty) {
            usageArgs
        } else {
            extendedUsageArgs
        }
    }

    private def getExtendedUsage: String = s"${name}(${getExtendedUsageArgs}) - ${extendedDescription}"

    private def getExamples: String = {
        this.examples
            .replace(_FUNC_, name)
            .replace(_ARGS_, usageArgs)
    }

    private def getImplementingClassFullName: String = {
        this.getClass.getName.replace("$", "") // fully qualified name of the implementing class
    }

}
