package com.databricks.labs.mosaic.core.expressions.raster

import com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfig
import com.databricks.labs.mosaic.core.raster.{MosaicRaster, MosaicRasterBand, RasterAPI}
import org.apache.spark.sql.catalyst.expressions.{Add, Expression}
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.{a, _}

class RasterBandExpressionTest extends AnyFunSuite with MockFactory {

  val mockLeftExpression: Expression = mock[Expression]
  val mockRightExpression: Expression = mock[Expression]
  val mockRasterAPI: RasterAPI = mock[RasterAPI]
  val mockExpressionConfig: MosaicExpressionConfig = mock[MosaicExpressionConfig]
  mockExpressionConfig.getRasterAPI _ expects * returning mockRasterAPI anyNumberOfTimes()
  mockRasterAPI.enable _ expects() returning mockRasterAPI anyNumberOfTimes()
  val mockRaster: MosaicRaster = mock[MosaicRaster]
  val mockBand: MosaicRasterBand = mock[MosaicRasterBand]

  // Mocking doesn't work well with templates, so we create a dummy class to extend the abstract class
  // We are using Abs as a template in order to test makeCopy which is linked to GenericExpressionFactory
  abstract class DummyExpr extends RasterBandExpression[Add](
    mockLeftExpression, mockRightExpression, BinaryType, mockExpressionConfig
  ) {
    // For partial mocking, make methods that are testable final, scalamock will not mock final methods
    override final def nullSafeEval(leftRasterRow: Any, rightRasterRow: Any): Any =
      super.nullSafeEval(leftRasterRow, rightRasterRow)

    override final def left: Expression = super.left

    override final def right: Expression = super.right
    
    override final def dataType: DataType = super.dataType

    override final val rasterAPI: RasterAPI = mockRasterAPI

    override final def makeCopy(newArgs: Array[AnyRef]): Expression = super.makeCopy(newArgs)

    override final def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
      super.withNewChildrenInternal(newLeft, newRight)

  }

  val bytes: Array[Byte] = "POINT EMPTY".getBytes

  val mockExpression: DummyExpr = mock[DummyExpr]


  def doMock(): Unit = {
    mockExpressionConfig.getRasterAPI _ expects * returning mockRasterAPI anyNumberOfTimes()
    mockExpression.bandTransform _ expects(mockRaster, mockBand) returning mockRaster anyNumberOfTimes()
    mockRasterAPI.raster _ expects "path" returning mockRaster anyNumberOfTimes()
    mockRaster.cleanUp _ expects() returning null anyNumberOfTimes()
    mockRaster.getBand _ expects 1 returning mockBand anyNumberOfTimes()
    mockLeftExpression.toString _ expects() returning "left" anyNumberOfTimes()
    mockRightExpression.toString _ expects() returning "right" anyNumberOfTimes()
  }


  test("RasterBandExpression should implement accessor methods") {
    doMock()

    mockExpression.left shouldBe mockLeftExpression
    mockExpression.right shouldBe mockRightExpression
    mockExpression.dataType shouldBe BinaryType
  }

  test("RasterBandExpression should evaluate") {
    doMock()

    val runtimePath = UTF8String.fromString("path")
    val result = mockExpression.nullSafeEval(runtimePath, 1)

    result shouldBe mockRaster
  }

  test("RasterBandExpression should make copy") {
    doMock()

    mockExpression.makeCopy(Array[AnyRef](mockLeftExpression, mockRightExpression)) shouldBe a[Add]
    mockExpression.withNewChildrenInternal(mockLeftExpression, mockRightExpression) shouldBe a[Add]
  }


}
