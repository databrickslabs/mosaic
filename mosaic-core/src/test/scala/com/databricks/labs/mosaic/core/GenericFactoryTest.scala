package com.databricks.labs.mosaic.core

import com.databricks.labs.mosaic.core.GenericServiceFactory.{GeometryAPIFactory, IndexSystemFactory, RasterAPIFactory}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.RasterAPI
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GenericFactoryTest extends AnyFunSuite with MockFactory {

  test("GeometryAPIFactory should getGeometryAPI based on META-INF/services") {
    // We need a mock GeometryAPI to test the factory
    // The generated class path is com.databricks.labs.mosaic.core.geometry.api.GenericFactoryTest$$anon$1
    // Make sure that this is the first mock in the file ot match $$anon$1
    val mockGeometryAPI = mock[GeometryAPI]
    mockGeometryAPI.name _ expects() returning "MockGeometryAPI" anyNumberOfTimes()

    noException should be thrownBy GeometryAPIFactory.getGeometryAPI("MockGeometryAPI", Array(this))
    an[IllegalArgumentException] should be thrownBy GeometryAPIFactory.getGeometryAPI("MockGeometryAPI")
  }

  test("IndexSystemFactory should getGeometryAPI based on META-INF/services") {
    // We need a mock IndexSystem to test the factory
    // The generated class path is com.databricks.labs.mosaic.core.geometry.api.GenericFactoryTest$$anon$2
    // Make sure that this is the second mock in the file ot match $$anon$2
    val mockIndexSystem = mock[IndexSystem]
    mockIndexSystem.name _ expects() returning "MockIndexSystem" anyNumberOfTimes()

    noException should be thrownBy IndexSystemFactory.getIndexSystem("MockIndexSystem", Array(this))
    an[IllegalArgumentException] should be thrownBy IndexSystemFactory.getIndexSystem("MockIndexSystem")
  }

  test("RasterAPIFactory should getRasterAPI based on META-INF/services") {
    // We need a mock RasterAPI to test the factory
    // The generated class path is com.databricks.labs.mosaic.core.geometry.api.GenericFactoryTest$$anon$3
    // Make sure that this is the third mock in the file ot match $$anon$3
    val mockRasterAPI = mock[RasterAPI]
    mockRasterAPI.name _ expects() returning "MockRasterAPI" anyNumberOfTimes()

    noException should be thrownBy RasterAPIFactory.getRasterAPI("MockRasterAPI", Array(this))
    an[IllegalArgumentException] should be thrownBy RasterAPIFactory.getRasterAPI("MockRasterAPI")
  }

}
