package com.databricks.labs.mosaic.core.expressions

import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaUnusedSymbol
class WithExpressionInfoTest extends AnyFunSuite with MockFactory {

  abstract class TestExpression extends WithExpressionInfo {
    override final def usage: String = super.usage
    override final def example: String = super.example
    override final def group: String = super.group
    override final def database: Option[String] = super.database
  }

  val mockExpression: TestExpression = mock[TestExpression]

  test("MosaicExpressionConfig") {
    mockExpression.name _ expects() returning "test"
    mockExpression.getExpressionInfo() shouldBe a[ExpressionInfo]
  }

}
