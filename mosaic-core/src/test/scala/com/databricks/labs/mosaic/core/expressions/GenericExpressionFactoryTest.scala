package com.databricks.labs.mosaic.core.expressions

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Add, Expression}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GenericExpressionFactoryTest extends AnyFunSuite with MockFactory {

  test("GenericExpressionFactory should make copy of Add expression") {
    val addExpression = Add(mock[Expression], mock[Expression])
    val newArgs = Array(mock[Expression], mock[Expression])

    GenericExpressionFactory.makeCopyImpl[Add](
      addExpression, newArgs.map(_.asInstanceOf[AnyRef]), 2, mock[MosaicExpressionConfig]
    ) shouldBe Add(newArgs(0), newArgs(1))
  }

  test("GenericExpressionFactory should generate a base builder") {
    val mockExpr = mock[Expression]
    mockExpr.toString _ expects() returning "mockExpr" anyNumberOfTimes()
    GenericExpressionFactory.getBaseBuilder[Add](2, mock[MosaicExpressionConfig]) shouldBe a[FunctionBuilder]
    val builder = GenericExpressionFactory.getBaseBuilder[Add](2, mock[MosaicExpressionConfig])
    builder.apply(Seq(mockExpr, mockExpr)) shouldBe Add(mockExpr, mockExpr)
  }

}
