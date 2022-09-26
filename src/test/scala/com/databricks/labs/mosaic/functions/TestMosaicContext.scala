package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.test.SparkSuite
import org.apache.spark.sql.adapters.Column
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.types.LongType
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class TestMosaicContext extends AnyFlatSpec with SparkSuite with MockFactory {

    "MosaicContext" should "detect if product H3 is enabled" in {

        val ix = stub[IndexSystem]
        ix.defaultDataTypeID _ when () returns LongType
        ix.name _ when () returns "H3"
        val mc = MosaicContext.build(ix, stub[GeometryAPI])

        val registry = spark.sessionState.functionRegistry

        assert(!mc.usingProductH3())

        // Mocking h3_longlatash3 function
        registry.registerFunction(
          FunctionIdentifier("h3_longlatash3", None),
          new ExpressionInfo("mock", "mock"),
          (exprs: Seq[Expression]) => Column(exprs.head).expr
        )

        assert(mc.usingProductH3())
    }

    it should "forward functions to product" in {

        val functionBuilder = stub[FunctionBuilder]
        val indexSystem = stub[IndexSystem]
        indexSystem.name _ when () returns "H3"
        indexSystem.defaultDataTypeID _ when () returns LongType

        val mc = MosaicContext.build(indexSystem, stub[GeometryAPI])

        val registry = spark.sessionState.functionRegistry

        // Register mock product functions
        registry.registerFunction(
          FunctionIdentifier("h3_longlatash3", None),
          new ExpressionInfo("product", "h3_longlatash3"),
          (exprs: Seq[Expression]) => Column(exprs.head).expr
        )
        registry.registerFunction(
          FunctionIdentifier("h3_polyfillash3", None),
          new ExpressionInfo("product", "h3_polyfillash3"),
          functionBuilder
        )
        registry.registerFunction(
          FunctionIdentifier("h3_boundaryaswkb", None),
          new ExpressionInfo("product", "h3_boundaryaswkb"),
          functionBuilder
        )

        mc.register(spark)

        assert(registry.lookupFunction(FunctionIdentifier("point_index_lonlat", None)).get.getName == "h3_longlatash3")
        assert(registry.lookupFunction(FunctionIdentifier("polyfill", None)).get.getName == "h3_polyfillash3")
        assert(registry.lookupFunction(FunctionIdentifier("index_geometry", None)).get.getName == "h3_boundaryaswkb")
    }

    "getProductMethod" should "get method via reflection" in {
        val indexSystem = stub[IndexSystem]
        indexSystem.name _ when () returns "H3"
        indexSystem.defaultDataTypeID _ when () returns LongType

        val mc = MosaicContext.build(indexSystem, stub[GeometryAPI])

        val method = mc.getProductMethod("sample_increment")

        assert(method.apply(1).asInstanceOf[Int] == 2)

    }

}
