package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI.{JTS, ESRI}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestH3IndexSystem extends AnyFlatSpec {

    "Auxiliary Methods" should "not cause any Errors." in {
        val indexRes = H3IndexSystem.pointToIndex(10, 10, 10)
        noException shouldBe thrownBy { H3IndexSystem.format(indexRes) }
        noException shouldBe thrownBy { H3IndexSystem.getResolutionStr(10) }
        noException shouldBe thrownBy { H3IndexSystem.indexToGeometry(H3IndexSystem.format(indexRes), JTS) }
    }

    "Point to Index" should "generate index ID for all valid resolutions." in {
        val indexRes0 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 0)
        val indexRes1 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 1)
        val indexRes2 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 2)
        val indexRes3 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 3)
        val indexRes4 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 4)
        val indexRes5 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 5)
        val indexRes6 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 6)
        val indexRes7 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 7)
        val indexRes8 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 8)
        val indexRes9 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 9)
        val indexRes10 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 10)
        val indexRes11 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 11)
        val indexRes12 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 12)
        val indexRes13 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 13)
        val indexRes14 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 14)
        val indexRes15 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 15)

        indexRes0 shouldBe 576918149140578303L
        indexRes1 shouldBe 581412952674926591L
        indexRes2 shouldBe 585913253767413759L
        indexRes3 shouldBe 590416509797400575L
        indexRes4 shouldBe 594920100834836479L
        indexRes5 shouldBe 599423697240981503L
        indexRes6 shouldBe 603927296734134271L
        indexRes7 shouldBe 608430896244064255L
        indexRes8 shouldBe 612934495858851839L
        indexRes9 shouldBe 617438095484387327L
        indexRes10 shouldBe 621941695111593983L
        indexRes11 shouldBe 626445294738935807L
        indexRes12 shouldBe 630948894366305791L
        indexRes13 shouldBe 635452493993676095L
        indexRes14 shouldBe 639956093621046583L
        indexRes15 shouldBe 644459693248417076L

    }

    "gridCenterAsWKB" should "generate index Centroid for valid Indexes for all geometries." in {
        val indexRes0 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 0)
        val indexRes9 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 9)
        val indexRes15 = H3IndexSystem.pointToIndex(-0.075441, 51.505480, 15)

        val centroidRes0 = H3IndexSystem.GridCenterAsWKB(indexRes0, JTS)
        val centroidRes9 = H3IndexSystem.GridCenterAsWKB(indexRes9, ESRI)
        val centroidRes15 = H3IndexSystem.GridCenterAsWKB(indexRes15, JTS)

        val List(x, y) = centroidRes0.toInternal.boundaries(0)(0).coords
        ((x * 1e6).round / 1e6) shouldBe -11.601626
        ((y * 1e6).round / 1e6) shouldBe 52.675751

        // Reduced the precision here to avoid rounding errors.
        centroidRes0.toWKT should (include("POINT") and include("-11.60162") and include("52.67575"))
        centroidRes9.toWKT should (include("POINT") and include("-0.07363") and include("51.50493"))
        centroidRes15.toWKT should (include("POINT") and include("-0.07544") and include("51.50547"))

    }



}
