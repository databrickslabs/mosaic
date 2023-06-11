package com.databricks.labs.mosaic.core.expressions.raster

import com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfig
import com.databricks.labs.mosaic.core.raster.{MosaicRaster, RasterAPI}
import org.apache.spark.sql.catalyst.expressions.{Abs, Expression}
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.{a, _}

class RasterExpressionTest extends AnyFunSuite with MockFactory {

  val mockChildExpression: Expression = mock[Expression]
  val mockRasterAPI: RasterAPI = mock[RasterAPI]
  val mockExpressionConfig: MosaicExpressionConfig = mock[MosaicExpressionConfig]
  mockExpressionConfig.getRasterAPI _ expects * returning mockRasterAPI anyNumberOfTimes()
  mockRasterAPI.enable _ expects() returning mockRasterAPI anyNumberOfTimes()
  val mockRaster: MosaicRaster = mock[MosaicRaster]

  // Mocking doesn't work well with templates, so we create a dummy class to extend the abstract class
  // We are using Abs as a template in order to test makeCopy which is linked to GenericExpressionFactory
  abstract class DummyExpr extends RasterExpression[Abs](
    mockChildExpression, BinaryType, mockExpressionConfig
  ) {
    // For partial mocking, make methods that are testable final, scalamock will not mock final methods
    override final def nullSafeEval(childRasterRow: Any): Any =
      super.nullSafeEval(childRasterRow)

    override final def child: Expression = super.child

    override final def dataType: DataType = super.dataType

    override final val rasterAPI: RasterAPI = mockRasterAPI

    override final def makeCopy(newArgs: Array[AnyRef]): Expression = super.makeCopy(newArgs)

    override final def withNewChildInternal(newChild: Expression): Expression =
      super.withNewChildInternal(newChild)

  }

  val bytes: Array[Byte] = "POINT EMPTY".getBytes

  val mockExpression: DummyExpr = mock[DummyExpr]


  def doMock(): Unit = {
    mockExpressionConfig.getRasterAPI _ expects * returning mockRasterAPI anyNumberOfTimes()
    mockRasterAPI.raster _ expects "path" returning mockRaster anyNumberOfTimes()
    mockRaster.cleanUp _ expects() returning null anyNumberOfTimes()
    mockChildExpression.toString _ expects() returning "child" anyNumberOfTimes()
    mockExpression.rasterTransform _ expects mockRaster returning mockRaster anyNumberOfTimes()
  }


  test("RasterExpression should implement accessor methods") {
    doMock()

    mockExpression.child shouldBe mockChildExpression
    mockExpression.dataType shouldBe BinaryType
  }

  test("RasterExpression should evaluate") {
    doMock()

    val runtimePath = UTF8String.fromString("path")
    val result = mockExpression.nullSafeEval(runtimePath)

    result shouldBe mockRaster
  }

  test("RasterExpression should make copy") {
    doMock()

    mockExpression.makeCopy(Array[AnyRef](mockChildExpression)) shouldBe a[Abs]
    mockExpression.withNewChildInternal(mockChildExpression) shouldBe a[Abs]
  }


}
