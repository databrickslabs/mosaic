package com.databricks.labs.mosaic.core.geometry.multilinestring

import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineStringESRI
import com.databricks.labs.mosaic.core.geometry.polygon.MosaicPolygonESRI
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.InternalRow

//noinspection ScalaRedundantCast
class TestMultiLineStringESRI extends AnyFlatSpec {

    "MosaicMultiLineStringESRI" should "return Nil for holes and hole points calls." in {
        val multiLineString = MosaicMultiLineStringESRI.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        multiLineString.getHoles shouldEqual Nil
        multiLineString.getHolePoints shouldEqual Nil
    }

    "MosaicMultiLineStringESRI" should "return seq(this) for shells and flatten calls." in {
        val multiLineString = MosaicMultiLineStringESRI.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        val lineString = MosaicLineStringESRI.fromWKT("LINESTRING(1 1, 2 2, 3 3)")
        multiLineString.getShells.head.equals(lineString) shouldBe true
        multiLineString.flatten.head.equals(lineString) shouldBe true
    }

    "MosaicMultiLineStringESRI" should "return number of points." in {
        val multiLineString = MosaicMultiLineStringESRI.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        multiLineString.numPoints shouldEqual 6
    }

    "MosaicMultiLineStringESRI" should "read all supported formats" in {
        val multiLineString = MosaicMultiLineStringESRI.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        noException should be thrownBy MosaicMultiLineStringESRI.fromWKB(multiLineString.toWKB)
        noException should be thrownBy MosaicMultiLineStringESRI.fromHEX(multiLineString.toHEX)
        noException should be thrownBy MosaicMultiLineStringESRI.fromJSON(multiLineString.toJSON)
        noException should be thrownBy MosaicMultiLineStringESRI.fromInternal(
          multiLineString.toInternal.serialize.asInstanceOf[InternalRow]
        )
        multiLineString.equals(MosaicMultiLineStringESRI.fromWKB(multiLineString.toWKB)) shouldBe true
        multiLineString.equals(MosaicMultiLineStringESRI.fromHEX(multiLineString.toHEX)) shouldBe true
        multiLineString.equals(MosaicMultiLineStringESRI.fromJSON(multiLineString.toJSON)) shouldBe true
        multiLineString.equals(
          MosaicMultiLineStringESRI.fromInternal(multiLineString.toInternal.serialize.asInstanceOf[InternalRow])
        ) shouldBe true
    }

    "MosaicMultiLineStringESRI" should "be instantiable from a Seq of MosaicLineStringESRI" in {
        val multiLineStringReference =
            MosaicMultiLineStringESRI.fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
        val multiLinesSeq = Seq("LINESTRING (10 10, 20 20, 10 40)", "LINESTRING (40 40, 30 30, 40 20, 30 10)")
            .map(MosaicLineStringESRI.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringESRI])
        val multiLineStringTest = MosaicMultiLineStringESRI.fromSeq(multiLinesSeq)
        multiLineStringReference.equals(multiLineStringTest) shouldBe true
    }

    "MosaicMultiLineStringESRI" should "not fail for empty Seq" in {
        val expected = MosaicMultiLineStringESRI.fromWKT(
            "MULTILINESTRING EMPTY"
        )
        val actual = MosaicMultiLineStringESRI.fromSeq(Seq())
        expected.equals(actual) shouldBe true
    }

    "MosaicMultiLineStringESRI" should "return a Seq of MosaicLineStringESRI object when calling asSeq" in {
        val multiLineString = MosaicMultiLineStringESRI
            .fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
            .asInstanceOf[MosaicMultiLineStringESRI]
        val linesSeqReference = Seq("LINESTRING (10 10, 20 20, 10 40)", "LINESTRING (40 40, 30 30, 40 20, 30 10)")
            .map(MosaicLineStringESRI.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringESRI])
        val lineSeqTest = multiLineString.asSeq.map(_.asInstanceOf[MosaicLineStringESRI])
        val results = linesSeqReference
            .zip(lineSeqTest)
            .map { case (a: MosaicLineStringESRI, b: MosaicLineStringESRI) => a.equals(b) }
        results should contain only true
    }

    "MosaicMultiLineStringESRI" should "return a Seq of MosaicLineStringESRI object with the correct SRID when calling asSeq" in {
        val srid = 32632
        val multiLineString = MosaicMultiLineStringESRI
            .fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
            .asInstanceOf[MosaicMultiLineStringESRI]
        multiLineString.setSpatialReference(srid)
        val linesSeqReference = Seq("LINESTRING (10 10, 20 20, 10 40)", "LINESTRING (40 40, 30 30, 40 20, 30 10)")
            .map(MosaicLineStringESRI.fromWKT)
            .map(_.asInstanceOf[MosaicLineStringESRI])
        linesSeqReference.foreach(_.setSpatialReference(srid))
        val lineSeqTest = multiLineString.asSeq.map(_.asInstanceOf[MosaicLineStringESRI])

        lineSeqTest.map(_.getSpatialReference) should contain only srid

        val results = linesSeqReference
            .zip(lineSeqTest)
            .map { case (a: MosaicLineStringESRI, b: MosaicLineStringESRI) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "MosaicMultiLineStringESRI" should "maintain SRID across operations" in {
        val srid = 32632
        val multiLineString = MosaicMultiLineStringESRI
            .fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
            .asInstanceOf[MosaicMultiLineStringESRI]
        val anotherLine = MosaicLineStringESRI.fromWKT("LINESTRING (20 10, 20 20, 20 40)").asInstanceOf[MosaicLineStringESRI]
        val poly = MosaicPolygonESRI.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        multiLineString.setSpatialReference(srid)

        // MosaicGeometryESRI
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

        // MosaicMultiLineStringESRI
        multiLineString.asSeq.head.getSpatialReference shouldBe srid
        multiLineString.getBoundary.getSpatialReference shouldBe srid
        multiLineString.getShells.head.getSpatialReference shouldBe srid
        multiLineString.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

}
