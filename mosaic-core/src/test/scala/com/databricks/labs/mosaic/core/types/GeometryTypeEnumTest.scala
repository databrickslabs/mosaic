package com.databricks.labs.mosaic.core.types

import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import com.databricks.labs.mosaic.core.types.GeometryTypeEnum._

class GeometryTypeEnumTest extends AnyFunSuite with MockFactory {

  test("HexType") {

    GeometryTypeEnum.fromString("POINT") shouldBe POINT
    GeometryTypeEnum.fromString("LINESTRING") shouldBe LINESTRING
    GeometryTypeEnum.fromString("POLYGON") shouldBe POLYGON
    GeometryTypeEnum.fromString("MULTIPOINT") shouldBe MULTIPOINT
    GeometryTypeEnum.fromString("MULTILINESTRING") shouldBe MULTILINESTRING
    GeometryTypeEnum.fromString("MULTIPOLYGON") shouldBe MULTIPOLYGON
    GeometryTypeEnum.fromString("GEOMETRYCOLLECTION") shouldBe GEOMETRYCOLLECTION
    GeometryTypeEnum.fromString("LINEARRING") shouldBe LINEARRING
    an[Error] should be thrownBy GeometryTypeEnum.fromString("NOT A GEOM")

    GeometryTypeEnum.fromId(1) shouldBe POINT
    GeometryTypeEnum.fromId(2) shouldBe MULTIPOINT
    GeometryTypeEnum.fromId(3) shouldBe LINESTRING
    GeometryTypeEnum.fromId(4) shouldBe MULTILINESTRING
    GeometryTypeEnum.fromId(5) shouldBe POLYGON
    GeometryTypeEnum.fromId(6) shouldBe MULTIPOLYGON
    GeometryTypeEnum.fromId(7) shouldBe LINEARRING
    GeometryTypeEnum.fromId(8) shouldBe GEOMETRYCOLLECTION
    an[Error] should be thrownBy GeometryTypeEnum.fromId(9)

    GeometryTypeEnum.groupOf(POINT) shouldBe POINT
    GeometryTypeEnum.groupOf(MULTIPOINT) shouldBe POINT
    GeometryTypeEnum.groupOf(LINESTRING) shouldBe LINESTRING
    GeometryTypeEnum.groupOf(MULTILINESTRING) shouldBe LINESTRING
    GeometryTypeEnum.groupOf(POLYGON) shouldBe POLYGON
    GeometryTypeEnum.groupOf(MULTIPOLYGON) shouldBe POLYGON
    GeometryTypeEnum.groupOf(GEOMETRYCOLLECTION) shouldBe GEOMETRYCOLLECTION

    GeometryTypeEnum.isFlat(POINT) shouldBe true
    GeometryTypeEnum.isFlat(MULTIPOINT) shouldBe false
    GeometryTypeEnum.isFlat(LINESTRING) shouldBe true
    GeometryTypeEnum.isFlat(MULTILINESTRING) shouldBe false
    GeometryTypeEnum.isFlat(POLYGON) shouldBe true
    GeometryTypeEnum.isFlat(MULTIPOLYGON) shouldBe false
    GeometryTypeEnum.isFlat(GEOMETRYCOLLECTION) shouldBe false

  }

}
