package com.databricks.labs.mosaic.core.types

import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class HexTypeTest extends AnyFunSuite with MockFactory {

  test("HexType") {
    val hexType = new HexType()
    hexType.typeName should be ("struct")
    hexType.simpleString should be ("HEX")
  }

}
