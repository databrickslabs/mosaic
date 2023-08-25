package com.databricks.labs.mosaic.core

import org.apache.spark.sql.adapters.Column
import org.apache.spark.sql.catalyst.expressions.{Add, Expression}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{be, noException}

class AdaptorsTest extends AnyFunSuite with MockFactory {

  test("Could should be constructable outside of spark") {
    val mockExpression = mock[Expression]
    noException should be thrownBy Column(Add(mockExpression, mockExpression))
  }

}
