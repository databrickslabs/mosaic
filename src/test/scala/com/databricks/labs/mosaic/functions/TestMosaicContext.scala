package com.databricks.labs.mosaic.functions

import com.databricks.labs.mosaic.{H3, SPARK_DATABRICKS_GEO_H3_ENABLED}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.test.SparkSuite
import org.apache.spark.sql.adapters.Column
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class TestMosaicContext extends AnyFlatSpec with SparkSuite with MockFactory {

    def getMosaicContext: MosaicContext = {
        val ix = stub[IndexSystem]
        ix.defaultDataTypeID _ when () returns LongType
        ix.name _ when () returns H3.name
        MosaicContext.build(ix, stub[GeometryAPI])
    }

    "MosaicContext" should "detect if product H3 is enabled" in {

        val mc = getMosaicContext

        assert(!mc.shouldUseDatabricksH3())

        spark.conf.set(SPARK_DATABRICKS_GEO_H3_ENABLED, "false")

        assert(!mc.shouldUseDatabricksH3())

        spark.conf.set(SPARK_DATABRICKS_GEO_H3_ENABLED, "true")

        assert(mc.shouldUseDatabricksH3())
    }

    it should "lookup correct sql functions" in {

        val functionBuilder = stub[FunctionBuilder]
        val mc = getMosaicContext

        val registry = spark.sessionState.functionRegistry

        spark.conf.set(SPARK_DATABRICKS_GEO_H3_ENABLED, "true")

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

        assert(registry.lookupFunction(FunctionIdentifier("grid_longlatascellid", None)).get.getName == "h3_longlatash3")
        assert(registry.lookupFunction(FunctionIdentifier("grid_polyfill", None)).get.getName == "h3_polyfillash3")
        assert(registry.lookupFunction(FunctionIdentifier("grid_boundaryaswkb", None)).get.getName == "h3_boundaryaswkb")
    }

    it should "forward the calls to databricks h3 functions" in {
        val mc = getMosaicContext
        spark.conf.set(SPARK_DATABRICKS_GEO_H3_ENABLED, "true")
        import mc.functions._

        val result = spark
            .range(10)
            .withColumn("grid_longlatascellid", grid_longlatascellid(col("id"), col("id"), col("id")))
            .withColumn("grid_polyfill", grid_polyfill(col("id"), col("id")))
            .withColumn("grid_boundaryaswkb", grid_boundaryaswkb(col("id")))
            .collect()

        assert(result.length == 10)
        assert(result.forall(_.getAs[String]("grid_longlatascellid") == "dummy_h3_longlatascellid"))
        assert(result.forall(_.getAs[String]("grid_polyfill") == "dummy_h3_polyfill"))
        assert(result.forall(_.getAs[String]("grid_boundaryaswkb") == "dummy_h3_boundaryaswkb"))
    }

    "getProductMethod" should "get method via reflection" in {

        val mc = getMosaicContext

        val method = mc.getProductMethod("sample_increment")

        assert(method.apply(1).asInstanceOf[Int] == 2)
    }

}
