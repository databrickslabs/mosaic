package com.databricks.labs.mosaic.core.geometry.multilinestring

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringJTS
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonJTS
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

//noinspection ScalaRedundantCast
class TestMultiLineStringJTS extends AnyFlatSpec {

    "MosaicMultiLineStringJTS" should "return Nil for holes and hole points calls." in {
        val multiLineString = MosaicMultiLineStringJTS.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        multiLineString.getHoles shouldEqual Nil
        multiLineString.getHolePoints shouldEqual Nil
    }

    "MosaicMultiLineStringJTS" should "return seq(this) for shells and flatten calls." in {
        val multiLineString = MosaicMultiLineStringJTS.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        val lineString = MosaicLineStringJTS.fromWKT("LINESTRING(1 1, 2 2, 3 3)")
        multiLineString.getShells.head.equals(lineString) shouldBe true
        multiLineString.flatten.head.equals(lineString) shouldBe true
    }

    "MosaicMultiLineStringJTS" should "return number of points." in {
        val multiLineString = MosaicMultiLineStringJTS.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        multiLineString.numPoints shouldEqual 6
    }

    "MosaicMultiLineStringJTS" should "read all supported formats" in {
        val multiLineString = MosaicMultiLineStringJTS.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        noException should be thrownBy MosaicMultiLineStringJTS.fromWKB(multiLineString.toWKB)
        noException should be thrownBy MosaicMultiLineStringJTS.fromHEX(multiLineString.toHEX)
        noException should be thrownBy MosaicMultiLineStringJTS.fromJSON(multiLineString.toJSON)
        noException should be thrownBy MosaicMultiLineStringJTS.fromInternal(multiLineString.toInternal.serialize.asInstanceOf[InternalRow])
        multiLineString.equals(MosaicMultiLineStringJTS.fromWKB(multiLineString.toWKB)) shouldBe true
        multiLineString.equals(MosaicMultiLineStringJTS.fromHEX(multiLineString.toHEX)) shouldBe true
        multiLineString.equals(MosaicMultiLineStringJTS.fromJSON(multiLineString.toJSON)) shouldBe true
        multiLineString.equals(
          MosaicMultiLineStringJTS.fromInternal(multiLineString.toInternal.serialize.asInstanceOf[InternalRow])
        ) shouldBe true
    }

    "MosaicMultiLineStringJTS" should "be instantiable from a Seq of MosaicLineStringJTS" in {
        val multiLineStringReference =
            MosaicMultiLineStringJTS.fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
        val multiLinesSeq = Seq("LINESTRING (10 10, 20 20, 10 40)", "LINESTRING (40 40, 30 30, 40 20, 30 10)")
            .map(MosaicLineStringJTS.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringJTS])
        val multiLineStringTest = MosaicMultiLineStringJTS.fromSeq(multiLinesSeq)
        multiLineStringReference.equals(multiLineStringTest) shouldBe true
    }

    "MosaicMultiLineStringJTS" should "not fail for empty Seq" in {
        val expected = MosaicMultiLineStringJTS.fromWKT(
            "MULTILINESTRING EMPTY"
        )
        val actual = MosaicMultiLineStringJTS.fromSeq(Seq[MosaicMultiLineStringJTS]())
        expected.equals(actual) shouldBe true
    }

    "MosaicMultiLineStringJTS" should "return a Seq of MosaicLineStringJTS object when calling asSeq" in {
        val multiLineString = MosaicMultiLineStringJTS
            .fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
            .asInstanceOf[MosaicMultiLineStringJTS]
        val linesSeqReference = Seq("LINESTRING (10 10, 20 20, 10 40)", "LINESTRING (40 40, 30 30, 40 20, 30 10)")
            .map(MosaicLineStringJTS.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringJTS])
        val lineSeqTest = multiLineString.asSeq.map(_.asInstanceOf[MosaicLineStringJTS])
        val results = linesSeqReference
            .zip(lineSeqTest)
            .map { case (a: MosaicLineStringJTS, b: MosaicLineStringJTS) => a.equals(b) }
        results should contain only true
    }

    "MosaicMultiLineStringJTS" should "return a Seq of MosaicLineStringJTS object with the correct SRID when calling asSeq" in {
        val srid = 32632
        val multiLineString = MosaicMultiLineStringJTS
            .fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
            .asInstanceOf[MosaicMultiLineStringJTS]
        multiLineString.setSpatialReference(srid)
        val linesSeqReference = Seq("LINESTRING (10 10, 20 20, 10 40)", "LINESTRING (40 40, 30 30, 40 20, 30 10)")
            .map(MosaicLineStringJTS.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringJTS])
        linesSeqReference.foreach(_.setSpatialReference(srid))
        val lineSeqTest = multiLineString.asSeq.map(_.asInstanceOf[MosaicLineStringJTS])

        lineSeqTest.map(_.getSpatialReference) should contain only srid

        val results = linesSeqReference
            .zip(lineSeqTest)
            .map { case (a: MosaicLineStringJTS, b: MosaicLineStringJTS) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "MosaicMultiLineStringJTS" should "maintain SRID across operations" in {
        val srid = 32632
        val multiLineString = MosaicMultiLineStringJTS
            .fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
            .asInstanceOf[MosaicMultiLineStringJTS]
        val anotherLine = MosaicLineStringJTS.fromWKT("LINESTRING (20 10, 20 20, 20 40)").asInstanceOf[MosaicLineStringJTS]
        val poly = MosaicPolygonJTS.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        multiLineString.setSpatialReference(srid)

        // MosaicGeometryJTS
        multiLineString.buffer(2d).getSpatialReference shouldBe srid
        multiLineString.convexHull.getSpatialReference shouldBe srid
        multiLineString.getCentroid.getSpatialReference shouldBe srid
        multiLineString.intersection(poly).getSpatialReference shouldBe srid
        multiLineString.rotate(45).getSpatialReference shouldBe srid
        multiLineString.scale(2d, 2d).getSpatialReference shouldBe srid
        multiLineString.simplify(0.001).getSpatialReference shouldBe srid
        multiLineString.translate(2d, 2d).getSpatialReference shouldBe srid
        multiLineString.union(anotherLine).getSpatialReference shouldBe srid

        // MosaicMultiLineString
        multiLineString.flatten.head.getSpatialReference shouldBe srid
        multiLineString.getShellPoints.head.head.getSpatialReference shouldBe srid

        // MosaicMultiLineStringJTS
        multiLineString.asSeq.head.getSpatialReference shouldBe srid
        multiLineString.getBoundary.getSpatialReference shouldBe srid
        multiLineString.getShells.head.getSpatialReference shouldBe srid
        multiLineString.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

}
