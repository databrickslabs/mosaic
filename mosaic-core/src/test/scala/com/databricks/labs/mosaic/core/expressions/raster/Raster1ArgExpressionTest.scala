package com.databricks.labs.mosaic.core.expressions.raster

import com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfig
import com.databricks.labs.mosaic.core.raster.{MosaicRaster, RasterAPI}
import org.apache.spark.sql.catalyst.expressions.{Add, Expression}
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class Raster1ArgExpressionTest extends AnyFunSuite with MockFactory {

  val mockLeftExpression: Expression = mock[Expression]
  val mockRightExpression: Expression = mock[Expression]
  val mockRasterAPI: RasterAPI = mock[RasterAPI]
  val mockExpressionConfig: MosaicExpressionConfig = mock[MosaicExpressionConfig]
  mockExpressionConfig.getRasterAPI _ expects * returning mockRasterAPI anyNumberOfTimes()
  mockRasterAPI.enable _ expects() returning mockRasterAPI anyNumberOfTimes()
  val mockRaster: MosaicRaster = mock[MosaicRaster]

  // Mocking doesn't work well with templates, so we create a dummy class to extend the abstract class
  // We are using Abs as a template in order to test makeCopy which is linked to GenericExpressionFactory
  abstract class DummyExpr extends Raster1ArgExpression[Add](
    mockLeftExpression, mockRightExpression, BinaryType, mockExpressionConfig
  ) {
    // For partial mocking, make methods that are testable final, scalamock will not mock final methods
    override final def nullSafeEval(leftGeometryRow: Any, arg1Row: Any): Any =
      super.nullSafeEval(leftGeometryRow, arg1Row)

    override final def left: Expression = super.left

    override final def right: Expression = super.right

    override final def dataType: DataType = super.dataType

    override final val rasterAPI: RasterAPI = mockRasterAPI

    override final def makeCopy(newArgs: Array[AnyRef]): Expression = super.makeCopy(newArgs)

    override final def withNewChildrenInternal(newFirst: Expression, newArg1: Expression): Expression =
      super.withNewChildrenInternal(newFirst, newArg1)

  }

  val bytes: Array[Byte] = "POINT EMPTY".getBytes

  val mockExpression: DummyExpr = mock[DummyExpr]


  def doMock(): Unit = {
    mockExpressionConfig.getRasterAPI _ expects * returning mockRasterAPI anyNumberOfTimes()
    mockExpression.rasterTransform _ expects(mockRaster, 1) returning mockRaster anyNumberOfTimes()
    mockRasterAPI.raster _ expects "path" returning mockRaster anyNumberOfTimes()
    mockRaster.cleanUp _ expects() returning null anyNumberOfTimes()
    mockLeftExpression.toString _ expects() returning "left" anyNumberOfTimes()
    mockRightExpression.toString _ expects() returning "right" anyNumberOfTimes()
  }


  test("Raster1ArgExpression should implement accessor methods") {
    doMock()

    mockExpression.left shouldBe mockLeftExpression
    mockExpression.right shouldBe mockRightExpression
    mockExpression.dataType shouldBe BinaryType
  }

  test("Raster1ArgExpression should evaluate") {
    doMock()

    val runtimePath = UTF8String.fromString("path")
    val result = mockExpression.nullSafeEval(runtimePath, 1)

    result shouldBe mockRaster
  }

  test("Raster1ArgExpression should make copy") {
    doMock()

    mockExpression.makeCopy(Array[AnyRef](mockLeftExpression, mockRightExpression)) shouldBe a[Add]
    mockExpression.withNewChildrenInternal(mockLeftExpression, mockRightExpression) shouldBe a[Add]
  }


}
