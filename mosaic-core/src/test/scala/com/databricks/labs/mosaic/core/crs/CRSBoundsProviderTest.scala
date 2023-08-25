package com.databricks.labs.mosaic.core.crs

import com.databricks.labs.mosaic.core.expressions.geometry.RequiresCRS
import com.databricks.labs.mosaic.core.geometry.MosaicPoint
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types.GeoJSONType
import org.apache.spark.sql.types.StringType
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class CRSBoundsProviderTest extends AnyFunSuite with MockFactory {

  val mockGeometryAPI: GeometryAPI = mock[GeometryAPI]
  val mockPoint1: MosaicPoint = mock[MosaicPoint]
  val mockPoint2: MosaicPoint = mock[MosaicPoint]


  def doMock(): Unit = {
    mockPoint1.getX _ expects() returning -180.00 anyNumberOfTimes()
    mockPoint1.getY _ expects() returning -90.00 anyNumberOfTimes()
    mockPoint2.getX _ expects() returning 180.00 anyNumberOfTimes()
    mockPoint2.getY _ expects() returning 90.00 anyNumberOfTimes()
    mockGeometryAPI.fromCoords _ expects Seq(-180.00, -90.00) returning mockPoint1 anyNumberOfTimes()
    mockGeometryAPI.fromCoords _ expects Seq(180.00, 90.00) returning mockPoint2 anyNumberOfTimes()
  }


  test("CRSBoundsProvider should load resource file and return correct bounds for EPSG:4326") {
    doMock()

    val boundsProvider = CRSBoundsProvider(geometryAPI = mockGeometryAPI)
    val bounds4326 = boundsProvider.bounds("EPSG", 4326)

    bounds4326.lowerLeft.getX shouldBe -180.00
    bounds4326.lowerLeft.getY shouldBe -90.00
    bounds4326.upperRight.getX shouldBe 180.00
    bounds4326.upperRight.getY shouldBe 90.00
    bounds4326.getUpperX shouldBe 180.00
    bounds4326.getUpperY shouldBe 90.00
    bounds4326.getLowerX shouldBe -180.00
    bounds4326.getLowerY shouldBe -90.00
  }

  test("CRSBoundsProvider should load resource file and return correct reprojected bounds for EPSG:4326") {
    doMock()

    val boundsProvider = CRSBoundsProvider(geometryAPI = mockGeometryAPI)
    val bounds4326 = boundsProvider.reprojectedBounds("EPSG", 4326)

    bounds4326.lowerLeft.getX shouldBe -180.00
    bounds4326.lowerLeft.getY shouldBe -90.00
    bounds4326.upperRight.getX shouldBe 180.00
    bounds4326.upperRight.getY shouldBe 90.00
  }

  test("CRSBoundsProvider should fail to load resource file and throw exception for EPSG:-9999") {
    doMock()

    val boundsProvider = CRSBoundsProvider(geometryAPI = mockGeometryAPI)

    an[Exception] should be thrownBy boundsProvider.bounds("EPSG", 9999)
    an[Exception] should be thrownBy boundsProvider.reprojectedBounds("EPSG", 9999)
  }

  test("RequiresCRS should return correct encoding for each geometry type") {
    object TestObject extends RequiresCRS {}


    noException should be thrownBy TestObject.checkEncoding(GeoJSONType)
    an[Exception] should be thrownBy TestObject.checkEncoding(StringType)
  }

}
