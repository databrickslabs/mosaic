package com.databricks.labs.mosaic.core.geometry.geometrycollection

import com.databricks.labs.mosaic.core.geometry.MosaicGeometryESRI
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.{noException, _}

class TestGeometryCollectionESRI extends AnyFunSuite {

    test("MosaicGeometryCollectionESRI should serialise and deserialize to/from Internal") {

        val geomCollection = MosaicGeometryCollectionESRI.fromWKT(
          "GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (6 1, 6 2, 6 3), POLYGON ((7 0, 7 1, 8 1, 8 0, 7 0))," +
              " MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((1 1, 1 2, 2 2, 2 1, 1 1)))" +
              ", MULTILINESTRING ((3 1, 4 2, 5 3), (4 4, 5 5, 6 6))" +
              ", MULTIPOINT (9 1, 9 2, 9 3))" +
              ")"
        )

        val internal = geomCollection.toInternal

        val deserialized = MosaicGeometryCollectionESRI.fromInternal(internal.serialize.asInstanceOf[InternalRow])

        val expected = geomCollection.asInstanceOf[MosaicGeometryCollectionESRI].asSeq.flatMap(_.flatten).reduce(_ union _)

        deserialized.equals(expected) shouldBe true

    }

    test("MosaicGeometryCollectionESRI should support type conversions") {
        val geometryCollection = MosaicGeometryCollectionESRI.fromWKT(
          "GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (6 1, 6 2, 6 3), POLYGON ((7 0, 7 1, 8 1, 8 0, 7 0))," +
              " MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((1 1, 1 2, 2 2, 2 1, 1 1)))" +
              ", MULTILINESTRING ((3 1, 4 2, 5 3), (4 4, 5 5, 6 6))" +
              ", MULTIPOINT (9 1, 9 2, 9 3))" +
              ")"
        )

        val geomWKB = geometryCollection.toWKB
        val geomJSON = geometryCollection.toJSON
        val geomWKT = geometryCollection.toWKT
        val geomHex = geometryCollection.toHEX

        MosaicGeometryCollectionESRI.fromWKB(geomWKB).equals(geometryCollection) shouldBe true
        MosaicGeometryCollectionESRI.fromJSON(geomJSON).equals(geometryCollection) shouldBe true
        MosaicGeometryCollectionESRI.fromWKT(geomWKT).equals(geometryCollection) shouldBe true
        MosaicGeometryCollectionESRI.fromHEX(geomHex).equals(geometryCollection) shouldBe true

    }

    test("MosaicGeometryCollectionESRI should support geometry operations") {
        val geometryCollection = MosaicGeometryCollectionESRI.fromWKT(
          "GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (6 1, 6 2, 6 3), POLYGON ((7 0, 7 1, 8 1, 8 0, 7 0))," +
              " MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((1 1, 1 2, 2 2, 2 1, 1 1)))" +
              ", MULTILINESTRING ((3 1, 4 2, 5 3), (4 4, 5 5, 6 6))" +
              ", MULTIPOINT (9 1, 9 2, 9 3))" +
              ")"
        )

        val point = MosaicGeometryESRI.fromWKT("POINT (11 1)")
        val multiline = MosaicGeometryESRI.fromWKT("MULTILINESTRING ((0 0, 2 1), (4 1, 4 2, 4 3))")
        val unionGeom = geometryCollection.union(point)
        val intersectionGeom = geometryCollection.intersection(multiline)
        val differenceGeom = geometryCollection.difference(multiline)

        noException should be thrownBy geometryCollection.buffer(1.0)
        noException should be thrownBy geometryCollection.boundary
        noException should be thrownBy geometryCollection.getBoundary
        noException should be thrownBy geometryCollection.convexHull
        noException should be thrownBy geometryCollection.getCentroid
        noException should be thrownBy geometryCollection.getArea
        noException should be thrownBy geometryCollection.getLength
        noException should be thrownBy geometryCollection.scale(2.0, 2.0)
        noException should be thrownBy geometryCollection.simplify(0.5)
        noException should be thrownBy geometryCollection.envelope
        noException should be thrownBy geometryCollection.getShells
        noException should be thrownBy geometryCollection.getHoles

        // Mosaic geometry collection will simplify the geometry collection into up to 3 pieces always
        // 1st for MultiPolygons (if any) 2nd for MultiLines (if any) 3rd for MultiPoints (if any)
        // order of the pieces isn't guaranteed, always check type of the piece.

        unionGeom.getNumGeometries shouldBe 3
        GeometryTypeEnum.fromString(unionGeom.getGeometryType) shouldBe GEOMETRYCOLLECTION

        val pieces = unionGeom.flatten

        pieces.find(g => GeometryTypeEnum.fromString(g.getGeometryType) == MULTIPOINT).get.getNumGeometries shouldBe 5
        pieces.find(g => GeometryTypeEnum.fromString(g.getGeometryType) == MULTILINESTRING).get.getNumGeometries shouldBe 3
        pieces.find(g => GeometryTypeEnum.fromString(g.getGeometryType) == MULTIPOLYGON).get.getNumGeometries shouldBe 3

        intersectionGeom.getNumGeometries shouldBe 2
        GeometryTypeEnum.fromString(intersectionGeom.getGeometryType) shouldBe GEOMETRYCOLLECTION

        geometryCollection.distance(point) shouldBe 2.0 +- 0.0001

        // Reconstruct original geometry from different results
        intersectionGeom.union(differenceGeom).difference(multiline).getLength shouldBe
            geometryCollection.getLength +- 0.0001

        geometryCollection.rotate(45.0).getLength shouldBe geometryCollection.getLength +- 0.0001
        geometryCollection.translate(1.0, 1.0).getLength shouldBe geometryCollection.getLength +- 0.0001

    }

    test("MosaicGeometryCollectionJTS should handle nested collections") {
        val nestedGeomCollection = MosaicGeometryCollectionESRI.fromWKT(
            "GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (6 1, 6 2, 6 3), POLYGON ((7 0, 7 1, 8 1, 8 0, 7 0))," +
                " MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((1 1, 1 2, 2 2, 2 1, 1 1)))," +
                " MULTILINESTRING ((3 1, 4 2, 5 3), (4 4, 5 5, 6 6))," +
                " MULTIPOINT (9 1, 9 2, 9 3)," +
                " GEOMETRYCOLLECTION (POINT (5 1), LINESTRING (6 1, 6 2, 6 3), POLYGON ((7 0, 7 1, 8 1, 8 0, 7 0))," +
                " MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((1 1, 1 2, 2 2, 2 1, 1 1)))," +
                " MULTILINESTRING ((3 1, 4 2, 5 3), (4 4, 5 5, 6 6))," +
                " MULTIPOINT (9 1, 9 2, 9 3))" +
                ")")

        noException should be thrownBy nestedGeomCollection.getShells
        noException should be thrownBy nestedGeomCollection.getHoles
        noException should be thrownBy nestedGeomCollection.getShellPoints
        noException should be thrownBy nestedGeomCollection.getHolePoints
        noException should be thrownBy nestedGeomCollection.mapXY((x, y) => (x + 1, y - 1))

        MosaicGeometryCollectionESRI.fromSeq(nestedGeomCollection.flatten).equals(nestedGeomCollection.compactGeometry) shouldBe true
    }


}
