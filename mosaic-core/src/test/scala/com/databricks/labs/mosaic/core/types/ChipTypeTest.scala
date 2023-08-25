package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.types.LongType
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class ChipTypeTest extends AnyFunSuite with MockFactory {

  test("ChipType") {
    ChipType(LongType).typeName should be ("struct")
    ChipType(LongType).simpleString should be ("CHIP")
  }

}
