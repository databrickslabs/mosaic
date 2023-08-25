package com.databricks.labs.mosaic.core.expressions.raster

import com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfig
import com.databricks.labs.mosaic.core.raster.{MosaicRaster, RasterAPI}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Abs, Expression}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types
import org.apache.spark.unsafe.types.UTF8String
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.{a, _}

class RasterGeneratorExpressionTest extends AnyFunSuite with MockFactory {

  val mockChildExpression: Expression = mock[Expression]
  val mockRasterAPI: RasterAPI = mock[RasterAPI]
  val mockExpressionConfig: MosaicExpressionConfig = mock[MosaicExpressionConfig]
  mockExpressionConfig.getRasterAPI _ expects * returning mockRasterAPI anyNumberOfTimes()
  mockRasterAPI.enable _ expects() returning mockRasterAPI anyNumberOfTimes()
  val mockRaster: MosaicRaster = mock[MosaicRaster]

  // Mocking doesn't work well with templates, so we create a dummy class to extend the abstract class
  // We are using Abs as a template in order to test makeCopy which is linked to GenericExpressionFactory
  abstract class DummyExpr extends RasterGeneratorExpression[Abs](
    mockChildExpression, mockExpressionConfig
  ) {
    // For partial mocking, make methods that are testable final, scalamock will not mock final methods
    override final def eval(childRasterRow: InternalRow): TraversableOnce[InternalRow] =
      super.eval(childRasterRow)

    override final def position: Boolean = super.position

    override final def inline: Boolean = super.inline

    override final def elementSchema: StructType = super.elementSchema

    override final val rasterAPI: RasterAPI = mockRasterAPI

    override final def makeCopy(newArgs: Array[AnyRef]): Expression = super.makeCopy(newArgs)

    override final def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
      super.withNewChildrenInternal(newChildren)

  }

  val bytes: Array[Byte] = "POINT EMPTY".getBytes
  val utf8Str: UTF8String = types.UTF8String.fromString("path")
  val tiles: Seq[(Long, (Int, Int, Int, Int))] =
    Seq((1L, (1, 2, 3, 4)), (2L, (5, 6, 7, 8)), (3L, (9, 10, 11, 12)))

  val mockExpression: DummyExpr = mock[DummyExpr]


  def doMock(): Unit = {
    mockExpressionConfig.getRasterAPI _ expects * returning mockRasterAPI anyNumberOfTimes()
    mockExpressionConfig.getRasterCheckpoint _ expects() returning "path" anyNumberOfTimes()
    mockRasterAPI.raster _ expects "path" returning mockRaster anyNumberOfTimes()
    mockRaster.cleanUp _ expects() returning null anyNumberOfTimes()
    tiles.foreach(
      tile => mockRaster.saveCheckpoint _ expects(*, tile._1, tile._2, "path") returning "path" anyNumberOfTimes()
    )
    mockChildExpression.toString _ expects() returning "child" anyNumberOfTimes()
    mockChildExpression.eval _ expects * returning utf8Str anyNumberOfTimes()
    mockExpression.rasterGenerator _ expects mockRaster returning tiles anyNumberOfTimes()
    mockExpression.children _ expects() returning IndexedSeq(mockChildExpression) anyNumberOfTimes()
  }


  test("RasterGeneratorExpression should implement accessor methods") {
    doMock()

    mockExpression.position shouldBe false
    mockExpression.inline shouldBe false
    mockExpression.elementSchema shouldBe a[StructType]
  }

  test("RasterGeneratorExpression should evaluate") {
    doMock()

    val runtimePath = UTF8String.fromString("path")
    val result = mockExpression.eval(InternalRow(runtimePath))

    result shouldBe a[List[String]]
  }

  test("RasterGeneratorExpression should make copy") {
    doMock()

    mockExpression.makeCopy(Array[AnyRef](mockChildExpression)) shouldBe a[Abs]
    mockExpression.withNewChildrenInternal(Array(mockChildExpression)) shouldBe a[Abs]
  }


}
