package com.databricks.labs.mosaic.core.crs

import com.databricks.labs.mosaic.core.geometry.api.{ESRI, JTS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestCRSBoundsProvider extends AnyFlatSpec {

    "CRSBoundsProvider" should "load resource file and return correct bounds for EPSG:4326 and EPSG:27700 for ESRI geometry API" in {
        val boundsProvider = CRSBoundsProvider(geometryAPI = ESRI)
        val bounds4326 = boundsProvider.bounds("EPSG", 4326)
        val bounds27700 = boundsProvider.bounds("EPSG", 27700)
        bounds4326.lowerLeft.getX shouldBe -180.00
        bounds4326.lowerLeft.getY shouldBe -90.00
        bounds4326.upperRight.getX shouldBe 180.00
        bounds4326.upperRight.getY shouldBe 90.00
        bounds27700.lowerLeft.getX shouldBe -7.56
        bounds27700.lowerLeft.getY shouldBe 49.96
        bounds27700.upperRight.getX shouldBe 1.78
        bounds27700.upperRight.getY shouldBe 60.84
    }

    "CRSBoundsProvider" should "load resource file and return correct bounds for EPSG:4326 and EPSG:27700 for JTS geometry API" in {
        val boundsProvider = CRSBoundsProvider(geometryAPI = JTS)
        val bounds4326 = boundsProvider.bounds("EPSG", 4326)
        val bounds27700 = boundsProvider.bounds("EPSG", 27700)
        bounds4326.lowerLeft.getX shouldBe -180.00
        bounds4326.lowerLeft.getY shouldBe -90.00
        bounds4326.upperRight.getX shouldBe 180.00
        bounds4326.upperRight.getY shouldBe 90.00
        bounds27700.lowerLeft.getX shouldBe -7.56
        bounds27700.lowerLeft.getY shouldBe 49.96
        bounds27700.upperRight.getX shouldBe 1.78
        bounds27700.upperRight.getY shouldBe 60.84
    }

}
