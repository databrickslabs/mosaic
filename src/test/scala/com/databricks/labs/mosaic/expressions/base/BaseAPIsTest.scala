package com.databricks.labs.mosaic.expressions.base

import com.databricks.labs.mosaic.expressions.raster.RST_BandMetaData
import com.databricks.labs.mosaic.functions.{MosaicContext, MosaicExpressionConfig}
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.should.Matchers.{be, noException}

class BaseAPIsTest extends MosaicSpatialQueryTest with SharedSparkSession {

    object DummyExpression extends WithExpressionInfo {

        override def name: String = "dummy"
        override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = (_: Seq[Expression]) => lit(0).expr

    }

    test("WithExpression Auxiliary tests") {
        noException should be thrownBy DummyExpression.name
        noException should be thrownBy DummyExpression.database
        noException should be thrownBy DummyExpression.usage
        noException should be thrownBy DummyExpression.example
        noException should be thrownBy DummyExpression.group
        noException should be thrownBy DummyExpression.builder(MosaicExpressionConfig(spark))
    }

    testAllNoCodegen("GenericExpressionFactory Auxiliary tests") { (_: MosaicContext) =>
        {
            assume(System.getProperty("os.name") == "Linux")
            noException should be thrownBy {
                val expressionConfig = MosaicExpressionConfig(spark)
                val builder = GenericExpressionFactory.getBaseBuilder[RST_BandMetaData](2, expressionConfig)
                builder(Seq(lit(0).expr, lit(0).expr, lit(0).expr))
            }
        }
    }

}
