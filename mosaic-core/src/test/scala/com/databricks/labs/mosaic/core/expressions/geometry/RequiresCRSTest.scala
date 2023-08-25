package com.databricks.labs.mosaic.core.expressions.geometry

import com.databricks.labs.mosaic.core.types.GeoJSONType
import org.apache.spark.sql.types.StringType
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class RequiresCRSTest extends AnyFunSuite with MockFactory {

  test("RequiresCRS should return correct encoding for each geometry type") {
    object TestObject extends RequiresCRS {}
    noException should be thrownBy TestObject.checkEncoding(GeoJSONType)
    an[Exception] should be thrownBy TestObject.checkEncoding(StringType)
  }

}
