package com.databricks.labs.mosaic.core.expressions.raster

import com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfig
import com.databricks.labs.mosaic.core.raster.{MosaicRaster, RasterAPI}
import org.apache.spark.sql.catalyst.expressions.{Conv, Expression}
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.{a, _}

class Raster2ArgExpressionTest extends AnyFunSuite with MockFactory {

  val mockFirstExpression: Expression = mock[Expression]
  val mockSecondExpression: Expression = mock[Expression]
  val mockThirdExpression: Expression = mock[Expression]
  val mockRasterAPI: RasterAPI = mock[RasterAPI]
  val mockExpressionConfig: MosaicExpressionConfig = mock[MosaicExpressionConfig]
  mockExpressionConfig.getRasterAPI _ expects * returning mockRasterAPI anyNumberOfTimes()
  mockRasterAPI.enable _ expects() returning mockRasterAPI anyNumberOfTimes()
  val mockRaster: MosaicRaster = mock[MosaicRaster]

  // Mocking doesn't work well with templates, so we create a dummy class to extend the abstract class
  // We are using Abs as a template in order to test makeCopy which is linked to GenericExpressionFactory
  abstract class DummyExpr extends Raster2ArgExpression[Conv](
    mockFirstExpression, mockSecondExpression, mockThirdExpression, BinaryType, mockExpressionConfig
  ) {
    // For partial mocking, make methods that are testable final, scalamock will not mock final methods
    override final def nullSafeEval(leftGeometryRow: Any, arg1Row: Any, arg2Row: Any): Any =
      super.nullSafeEval(leftGeometryRow, arg1Row, arg2Row)

    override final def first: Expression = super.first

    override final def second: Expression = super.second

    override final def third: Expression = super.third

    override final def dataType: DataType = super.dataType

    override final val rasterAPI: RasterAPI = mockRasterAPI

    override final def makeCopy(newArgs: Array[AnyRef]): Expression = super.makeCopy(newArgs)

    override final def withNewChildrenInternal(newFirst: Expression, newArg1: Expression, newArg2: Expression): Expression =
      super.withNewChildrenInternal(newFirst, newArg1, newArg2)

  }

  val bytes: Array[Byte] = "POINT EMPTY".getBytes

  val mockExpression: DummyExpr = mock[DummyExpr]


  def doMock(): Unit = {
    mockExpressionConfig.getRasterAPI _ expects * returning mockRasterAPI anyNumberOfTimes()
    mockExpression.rasterTransform _ expects(mockRaster, 1, 2) returning mockRaster anyNumberOfTimes()
    mockRasterAPI.raster _ expects "path" returning mockRaster anyNumberOfTimes()
    mockRaster.cleanUp _ expects() returning null anyNumberOfTimes()
    mockFirstExpression.toString _ expects() returning "first" anyNumberOfTimes()
    mockSecondExpression.toString _ expects() returning "second" anyNumberOfTimes()
    mockThirdExpression.toString _ expects() returning "third" anyNumberOfTimes()
  }


  test("Raster2ArgExpression should implement accessor methods") {
    doMock()

    mockExpression.first shouldBe mockFirstExpression
    mockExpression.second shouldBe mockSecondExpression
    mockExpression.third shouldBe mockThirdExpression
    mockExpression.dataType shouldBe BinaryType
  }

  test("Raster2ArgExpression should evaluate") {
    doMock()

    val runtimePath = UTF8String.fromString("path")
    val result = mockExpression.nullSafeEval(runtimePath, 1, 2)

    result shouldBe mockRaster
  }

  test("Raster2ArgExpression should make copy") {
    doMock()

    mockExpression.makeCopy(Array[AnyRef](mockFirstExpression, mockSecondExpression, mockThirdExpression)) shouldBe a[Conv]
    mockExpression.withNewChildrenInternal(mockFirstExpression, mockSecondExpression, mockThirdExpression) shouldBe a[Conv]
  }


}
