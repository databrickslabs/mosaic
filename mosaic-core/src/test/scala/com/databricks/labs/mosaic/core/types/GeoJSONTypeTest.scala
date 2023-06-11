package com.databricks.labs.mosaic.core.types

import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GeoJSONTypeTest extends AnyFunSuite with MockFactory {

  test("GeoJSONType") {
    val geoJSONType = new GeoJSONType()
    geoJSONType.typeName should be ("struct")
    geoJSONType.simpleString should be ("GEOJSON")
  }

}
