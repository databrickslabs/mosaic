package com.databricks.labs.mosaic.core.index

import com.databricks.labs.mosaic.core.geometry.MosaicGeometryJTS
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class TestBNGIndexSystem extends AnyFunSuite {

    test("Point to Index should generate index ID for positive resolutions.") {
        val indexRes1 = BNGIndexSystem.pointToIndex(538825, 179111, 1)
        val indexRes2 = BNGIndexSystem.pointToIndex(538825, 179111, 2)
        val indexRes3 = BNGIndexSystem.pointToIndex(538825, 179111, 3)
        val indexRes4 = BNGIndexSystem.pointToIndex(538825, 179111, 4)
        val indexRes5 = BNGIndexSystem.pointToIndex(538825, 179111, 5)
        val indexRes6 = BNGIndexSystem.pointToIndex(538825, 179111, 6)

        indexRes1 shouldBe 105010
        indexRes2 shouldBe 10501370
        indexRes3 shouldBe 1050138790
        indexRes4 shouldBe 105013887910L
        indexRes5 shouldBe 10501388279110L
        indexRes6 shouldBe 1050138825791110L

        BNGIndexSystem.format(indexRes1) shouldBe "TQ"
        BNGIndexSystem.format(indexRes2) shouldBe "TQ37"
        BNGIndexSystem.format(indexRes3) shouldBe "TQ3879"
        BNGIndexSystem.format(indexRes4) shouldBe "TQ388791"
        BNGIndexSystem.format(indexRes5) shouldBe "TQ38827911"
        BNGIndexSystem.format(indexRes6) shouldBe "TQ3882579111"

        BNGIndexSystem.getResolution(BNGIndexSystem.indexDigits(105010)) shouldBe 1
        BNGIndexSystem.getResolution(BNGIndexSystem.indexDigits(10501370)) shouldBe 2
        BNGIndexSystem.getResolution(BNGIndexSystem.indexDigits(1050138790)) shouldBe 3
        BNGIndexSystem.getResolution(BNGIndexSystem.indexDigits(105013887910L)) shouldBe 4
        BNGIndexSystem.getResolution(BNGIndexSystem.indexDigits(10501388279110L)) shouldBe 5
        BNGIndexSystem.getResolution(BNGIndexSystem.indexDigits(1050138825791110L)) shouldBe 6

    }

    test("Point to Index should generate index ID for negative resolutions.") {
        val indexResN1 = BNGIndexSystem.pointToIndex(538825, 179111, -1)
        val indexResN2 = BNGIndexSystem.pointToIndex(538825, 179111, -2)
        val indexResN3 = BNGIndexSystem.pointToIndex(538825, 179111, -3)
        val indexResN4 = BNGIndexSystem.pointToIndex(538825, 179111, -4)
        val indexResN5 = BNGIndexSystem.pointToIndex(538825, 179111, -5)
        val indexResN6 = BNGIndexSystem.pointToIndex(538825, 179111, -6)

        indexResN1 shouldBe 1050
        indexResN2 shouldBe 105012
        indexResN3 shouldBe 10501373
        indexResN4 shouldBe 1050138794L
        indexResN5 shouldBe 105013887911L
        indexResN6 shouldBe 10501388279114L

        BNGIndexSystem.format(indexResN1) shouldBe "T"
        BNGIndexSystem.format(indexResN2) shouldBe "TQNW"
        BNGIndexSystem.format(indexResN3) shouldBe "TQ37NE"
        BNGIndexSystem.format(indexResN4) shouldBe "TQ3879SE"
        BNGIndexSystem.format(indexResN5) shouldBe "TQ388791SW"
        BNGIndexSystem.format(indexResN6) shouldBe "TQ38827911SE"

        BNGIndexSystem.getResolution(BNGIndexSystem.indexDigits(1050)) shouldBe -1
        BNGIndexSystem.getResolution(BNGIndexSystem.indexDigits(105012)) shouldBe -2
        BNGIndexSystem.getResolution(BNGIndexSystem.indexDigits(10501373)) shouldBe -3
        BNGIndexSystem.getResolution(BNGIndexSystem.indexDigits(1050138794L)) shouldBe -4
        BNGIndexSystem.getResolution(BNGIndexSystem.indexDigits(105013887911L)) shouldBe -5
        BNGIndexSystem.getResolution(BNGIndexSystem.indexDigits(10501388279114L)) shouldBe -6

        an[IllegalStateException] should be thrownBy BNGIndexSystem.pointToIndex(Double.NaN, 100.0, 5)
        an[IllegalStateException] should be thrownBy BNGIndexSystem.pointToIndex(100.0, Double.NaN, 5)

    }

    test("Parse and Format should generate consistent results.") {
        BNGIndexSystem.parse("T") shouldBe 1050
        BNGIndexSystem.format(1050) shouldBe "T"

        BNGIndexSystem.parse("TQ") shouldBe 105010
        BNGIndexSystem.format(105010) shouldBe "TQ"

        BNGIndexSystem.parse("TQNW") shouldBe 105012
        BNGIndexSystem.format(105012) shouldBe "TQNW"

        BNGIndexSystem.parse("TQ38827911") shouldBe 10501388279110L
        BNGIndexSystem.format(10501388279110L) shouldBe "TQ38827911"

        BNGIndexSystem.parse("TQ38827911SE") shouldBe 10501388279114L
        BNGIndexSystem.format(10501388279114L) shouldBe "TQ38827911SE"
    }

    test("KDisk should generate index IDs for negative resolutions.") {
        val index = 1050138794L // "TQ3879SE" res -4

        val kDisk1 = BNGIndexSystem.kLoop(index, 1).map(BNGIndexSystem.format)
        val kDisk2 = BNGIndexSystem.kLoop(index, 2).map(BNGIndexSystem.format)
        val kDisk3 = BNGIndexSystem.kLoop(index, 3).map(BNGIndexSystem.format)
        kDisk1 should contain theSameElementsAs Seq(
          Seq("TQ3878NW", "TQ3878NE"), // bottom
          Seq("TQ3978NW", "TQ3979SW"), // right
          Seq("TQ3979NW", "TQ3879NE"), // top
          Seq("TQ3879NW", "TQ3879SW") // left
        ).flatten
        kDisk2 should contain theSameElementsAs Seq(
          Seq("TQ3778SE", "TQ3878SW", "TQ3878SE", "TQ3978SW"), // bottom
          Seq("TQ3978SE", "TQ3978NE", "TQ3979SE", "TQ3979NE"), // right
          Seq("TQ3980SE", "TQ3980SW", "TQ3880SE", "TQ3880SW"), // top
          Seq("TQ3780SE", "TQ3779NE", "TQ3779SE", "TQ3778NE") // left
        ).flatten
        kDisk3 should contain theSameElementsAs Seq(
          Seq("TQ3777NW", "TQ3777NE", "TQ3877NW", "TQ3877NE", "TQ3977NW", "TQ3977NE"), // bottom
          Seq("TQ4077NW", "TQ4078SW", "TQ4078NW", "TQ4079SW", "TQ4079NW", "TQ4080SW"), // right
          Seq("TQ4080NW", "TQ3980NE", "TQ3980NW", "TQ3880NE", "TQ3880NW", "TQ3780NE"), // top
          Seq("TQ3780NW", "TQ3780SW", "TQ3779NW", "TQ3779SW", "TQ3778NW", "TQ3778SW") // left
        ).flatten
    }

    test("KDisk should generate index IDs for positive resolutions.") {
        val index = 1050138790
        val kDisk1 = BNGIndexSystem.kLoop(index, 1).map(BNGIndexSystem.format)
        val kDisk2 = BNGIndexSystem.kLoop(index, 2).map(BNGIndexSystem.format)
        val kDisk3 = BNGIndexSystem.kLoop(index, 3).map(BNGIndexSystem.format)
        kDisk1 should contain theSameElementsAs Seq(
          Seq("TQ3778", "TQ3779"), // bottom
          Seq("TQ3780", "TQ3878"), // right
          Seq("TQ3880", "TQ3978"), // top
          Seq("TQ3979", "TQ3980") // left
        ).flatten
        kDisk2 should contain theSameElementsAs Seq(
          Seq("TQ3677", "TQ3777", "TQ3877", "TQ3977"), // bottom
          Seq("TQ4077", "TQ4078", "TQ4079", "TQ4080"), // right
          Seq("TQ4081", "TQ3981", "TQ3881", "TQ3781"), // top
          Seq("TQ3681", "TQ3680", "TQ3679", "TQ3678") // left
        ).flatten
        kDisk3 should contain theSameElementsAs Seq(
          Seq("TQ3576", "TQ3676", "TQ3776", "TQ3876", "TQ3976", "TQ4076"), // bottom
          Seq("TQ4176", "TQ4177", "TQ4178", "TQ4179", "TQ4180", "TQ4181"), // right
          Seq("TQ4182", "TQ4082", "TQ3982", "TQ3882", "TQ3782", "TQ3682"), // top
          Seq("TQ3582", "TQ3581", "TQ3580", "TQ3579", "TQ3578", "TQ3577") // left
        ).flatten
    }

    test("KRing should generate index IDs for positive resolutions.") {
        val index = 1050138790
        val kRing1 = BNGIndexSystem.kRing(index, 1).map(BNGIndexSystem.format)
        val kRing2 = BNGIndexSystem.kRing(index, 2).map(BNGIndexSystem.format)
        val kRing3 = BNGIndexSystem.kRing(index, 3).map(BNGIndexSystem.format)
        val kDisk1 = BNGIndexSystem.kLoop(index, 1).map(BNGIndexSystem.format)
        val kDisk2 = BNGIndexSystem.kLoop(index, 2).map(BNGIndexSystem.format)
        val kDisk3 = BNGIndexSystem.kLoop(index, 3).map(BNGIndexSystem.format)
        kRing1 should contain theSameElementsAs Seq(BNGIndexSystem.format(index)).union(kDisk1)
        kRing2 should contain theSameElementsAs Seq(BNGIndexSystem.format(index)).union(kDisk1).union(kDisk2)
        kRing3 should contain theSameElementsAs Seq(BNGIndexSystem.format(index)).union(kDisk1).union(kDisk2).union(kDisk3)
    }

    test("KRing should generate index IDs for string cell ID") {
        val index = "TQ3878NW"
        val kRing1 = BNGIndexSystem.asInstanceOf[IndexSystem].kRing(index, 1)
        val kRing2 = BNGIndexSystem.asInstanceOf[IndexSystem].kRing(index, 2)
        val kRing3 = BNGIndexSystem.asInstanceOf[IndexSystem].kRing(index, 3)

        kRing1 should contain theSameElementsAs BNGIndexSystem.kRing(BNGIndexSystem.parse(index), 1).map(BNGIndexSystem.format)
        kRing2 should contain theSameElementsAs BNGIndexSystem.kRing(BNGIndexSystem.parse(index), 2).map(BNGIndexSystem.format)
        kRing3 should contain theSameElementsAs BNGIndexSystem.kRing(BNGIndexSystem.parse(index), 3).map(BNGIndexSystem.format)
    }

    test("KLoop should generate index IDs for string cell ID") {
        val index = "TQ3878NW"
        val kLoop1 = BNGIndexSystem.asInstanceOf[IndexSystem].kLoop(index, 1)
        val kLoop2 = BNGIndexSystem.asInstanceOf[IndexSystem].kLoop(index, 2)
        val kLoop3 = BNGIndexSystem.asInstanceOf[IndexSystem].kLoop(index, 3)

        kLoop1 should contain theSameElementsAs BNGIndexSystem.kLoop(BNGIndexSystem.parse(index), 1).map(BNGIndexSystem.format)
        kLoop2 should contain theSameElementsAs BNGIndexSystem.kLoop(BNGIndexSystem.parse(index), 2).map(BNGIndexSystem.format)
        kLoop3 should contain theSameElementsAs BNGIndexSystem.kLoop(BNGIndexSystem.parse(index), 3).map(BNGIndexSystem.format)
    }

    test("IsValid should return correct validity") {
        val cellId = BNGIndexSystem.pointToIndex(-50000.0, 50.0, 3)
        val cellId2 = BNGIndexSystem.pointToIndex(50.0, 500000000.0, 4)
        BNGIndexSystem.isValid(cellId) shouldBe false
        BNGIndexSystem.isValid(cellId2) shouldBe false
    }

    test("Test coerce geometries") {
        val geomsWKTs1 = Seq(
          "POLYGON ((-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5))",
          "MULTIPOLYGON (((-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)),((-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)))",
          "LINESTRING (-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)",
          "POINT (-0.1 51.5)"
        )
        val geomsWKTs2 = Seq(
          "LINESTRING (-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)",
          "MULTIPOINT (-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)",
          "LINESTRING (-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)"
        )
        val geomsWKTs3 = Seq(
          "POINT (-0.1 51.5)",
          "MULTIPOINT (-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5)"
        )
        val geomsWKTs4 = Seq(
          "GEOMETRYCOLLECTION (POINT (-0.1 51.5), MULTIPOINT (-0.1 51.5, -0.1 51.6, 0.1 51.6, 0.1 51.5, -0.1 51.5))"
        )

        BNGIndexSystem
            .coerceChipGeometry(geomsWKTs1.map(MosaicGeometryJTS.fromWKT))
            .map(g => GeometryTypeEnum.fromString(g.getGeometryType))
            .forall(Seq(POLYGON, MULTIPOLYGON).contains(_)) shouldBe true
        BNGIndexSystem
            .coerceChipGeometry(geomsWKTs2.map(MosaicGeometryJTS.fromWKT))
            .map(g => GeometryTypeEnum.fromString(g.getGeometryType))
            .forall(Seq(LINESTRING, MULTILINESTRING).contains(_)) shouldBe true
        BNGIndexSystem
            .coerceChipGeometry(geomsWKTs3.map(MosaicGeometryJTS.fromWKT))
            .map(g => GeometryTypeEnum.fromString(g.getGeometryType))
            .forall(Seq(POINT, MULTIPOINT).contains(_)) shouldBe true
        BNGIndexSystem.coerceChipGeometry(geomsWKTs4.map(MosaicGeometryJTS.fromWKT)).isEmpty shouldBe true
    }

    test("Auxiliary methods should not throw exceptions") {
        noException should be thrownBy BNGIndexSystem.getResolution(5)
        noException should be thrownBy BNGIndexSystem.getResolution("100m")
        noException should be thrownBy BNGIndexSystem.getResolution(UTF8String.fromString("100m"))
        an[IllegalStateException] should be thrownBy BNGIndexSystem.getResolution(true)
        BNGIndexSystem.getResolutionStr(4) shouldEqual "100m"
        BNGIndexSystem.getResolutionStr(-4) shouldEqual "500m"
        BNGIndexSystem.getResolutionStr(7) shouldEqual ""
//        an[Exception] should be thrownBy BNGIndexSystem.polyfill(null, 0, None)
    }

    test("Issue 354: KRing should work near the edge of the grid") {
        val kring = BNGIndexSystem.kRing("TM99", 1)
        kring should contain theSameElementsAs Seq("TM99", "TM88", "TM98", "TN08", "TN09", "TH00", "TG90", "TG80", "TM89")
    }

}
