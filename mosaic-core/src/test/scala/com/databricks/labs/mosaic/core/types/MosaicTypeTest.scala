package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.types.LongType
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class MosaicTypeTest extends AnyFunSuite with MockFactory {

  test("MosaicType") {
    val mosaicType = MosaicType(LongType)
    mosaicType.typeName should be ("struct")
    mosaicType.simpleString should be ("MOSAIC")
  }

}
