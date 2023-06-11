package com.databricks.labs.mosaic.core.expressions.geometry

import com.databricks.labs.mosaic.core.codegen.format.GeometryIOCodeGen
import com.databricks.labs.mosaic.core.geometry.MosaicGeometry
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.IndexSystem
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.{BinaryType, StringType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class VectorExpressionTest extends AnyFunSuite with MockFactory {

  val mockExpression: VectorExpression = mock[VectorExpression]
  val mockIndexSystem: IndexSystem = mock[IndexSystem]
  val mockGeometryAPI: GeometryAPI = mock[GeometryAPI]
  val mockPoint: MosaicGeometry = mock[MosaicGeometry]
  val mockCtx: CodegenContext = mock[CodegenContext]
  val mockIO: GeometryIOCodeGen = mock[GeometryIOCodeGen]

  def doMock(): Unit = {
    mockExpression.geometryAPI _ expects() returning mockGeometryAPI anyNumberOfTimes()
    mockExpression.mosaicGeomClass _ expects() returning "M_GEOMETRY" anyNumberOfTimes()
    mockExpression.geomClass _ expects() returning "GEOMETRY" anyNumberOfTimes()
    mockExpression.CRSBoundsProviderClass _ expects() returning "CRSBoundsProvider" anyNumberOfTimes()
    mockExpression.geometryAPIClass _ expects() returning "GEOMETRY_API" anyNumberOfTimes()
    mockIndexSystem.name _ expects() returning "INDEX_SYSTEM" anyNumberOfTimes()
    mockGeometryAPI.name _ expects() returning "GEOMETRY_API" anyNumberOfTimes()
    mockGeometryAPI.serialize _ expects(mockPoint, StringType) returning "POINT EMPTY" anyNumberOfTimes()
    mockGeometryAPI.ioCodeGen _ expects() returning mockIO anyNumberOfTimes()
    mockGeometryAPI.geometryClass _ expects() returning "GEOMETRY" anyNumberOfTimes()
    mockGeometryAPI.mosaicGeometryClass _ expects() returning "M_GEOMETRY" anyNumberOfTimes()
    mockPoint.toWKT _ expects() returning "POINT EMPTY" anyNumberOfTimes()
    mockCtx.freshName _ expects "baseGeometry" returning "baseGeometry1" anyNumberOfTimes()
    mockIO.toWKT _ expects(mockCtx, "baseGeometry1", mockGeometryAPI) returning
      ("String wkt1 = baseGeometry1.toWKT();", "wkt1") anyNumberOfTimes()
  }


  test("VectorExpression should be correctly initialised from expressionConfig") {
    doMock()
    mockExpression.mosaicGeomClass shouldBe "M_GEOMETRY"
    mockExpression.geomClass shouldBe "GEOMETRY"
    mockExpression.CRSBoundsProviderClass shouldBe "CRSBoundsProvider"
    mockExpression.geometryAPIClass shouldBe "GEOMETRY_API"
  }

  test("VectorExpression should serialise a result or a geometry result") {
    doMock()

    object TestObject extends VectorExpression {
      override def geometryAPI: GeometryAPI = mockGeometryAPI
    }

    TestObject.serialise("POINT EMPTY", returnsGeometry = false, StringType) shouldBe "POINT EMPTY"
    TestObject.serialise("POINT EMPTY".getBytes, returnsGeometry = false, BinaryType) shouldBe "POINT EMPTY".getBytes
    TestObject.serialise(mockPoint, returnsGeometry = true, StringType) shouldBe "POINT EMPTY"

  }

  test("VectorExpression should generate serialise code") {
    doMock()

    object TestObject extends VectorExpression {
      override def geometryAPI: GeometryAPI = mockGeometryAPI
    }

    TestObject.serialiseCodegen("geom1", returnsGeometry = true, StringType, mockCtx) shouldBe
      (
        """
          |GEOMETRY baseGeometry1 = geom1.getGeom();
          |String wkt1 = baseGeometry1.toWKT();
          |""".stripMargin, "wkt1")

    TestObject.serialiseCodegen("geom1", returnsGeometry = false, StringType, mockCtx) shouldBe
      ("", "geom1")
  }

  test("VectorExpression should return correct mosaicGeometryRef") {
    doMock()

    object TestObject extends VectorExpression {
      override def geometryAPI: GeometryAPI = mockGeometryAPI
    }

    TestObject.mosaicGeometryRef("geom1") shouldBe "M_GEOMETRY.apply(geom1)"

  }

  test("VectorExpression should implement accessor methods") {
    doMock()

    object TestObject extends VectorExpression {
      override def geometryAPI: GeometryAPI = mockGeometryAPI
    }

    noException should be thrownBy TestObject.mosaicGeomClass
    noException should be thrownBy TestObject.geomClass
    noException should be thrownBy TestObject.CRSBoundsProviderClass
    noException should be thrownBy TestObject.geometryAPIClass

  }


}
