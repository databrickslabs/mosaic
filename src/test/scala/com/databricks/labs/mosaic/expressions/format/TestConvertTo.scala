package com.databricks.labs.mosaic.expressions.format

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.NullType
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestConvertTo extends AnyFlatSpec with MockFactory {

    "ConvertTo" should "throw an exception when serializing non existing format." in {
        val expr = stub[Expression]
        expr.dataType _ when () returns NullType
        val converted = ConvertTo(expr, "WKT", "non-existent-format")

        val result = converted.checkInputDataTypes()
        assert(result.isFailure)
    }

}
