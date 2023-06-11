package com.databricks.labs.mosaic.core.expressions.raster

import com.databricks.labs.mosaic.core.expressions.MosaicExpressionConfig
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.{MosaicRaster, MosaicRasterBand, RasterAPI}
import org.apache.spark.sql.catalyst.expressions.{Add, Expression}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{BinaryType, LongType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.{a, _}

class RasterToGridExpressionTest extends AnyFunSuite with MockFactory {

  val mockPathExpression: Expression = mock[Expression]
  val mockResolutionExpression: Expression = mock[Expression]
  val mockRasterAPI: RasterAPI = mock[RasterAPI]
  val mockIndexSystem: IndexSystem = mock[IndexSystem]
  val mockExpressionConfig: MosaicExpressionConfig = mock[MosaicExpressionConfig]
  val mockRaster: MosaicRaster = mock[MosaicRaster]
  val mockRasterBand: MosaicRasterBand = mock[MosaicRasterBand]
  val geoTransform: Seq[Double] = Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
  val cellMeasurePairs: Seq[Map[Long, Int]] = Seq(Map(1L -> 1), Map(2L -> 2), Map(3L -> 3), Map(4L -> 4), Map(5L -> 5), Map(6L -> 6))

  def doMock(): Unit = {
    mockExpressionConfig.getRasterAPI _ expects * returning mockRasterAPI anyNumberOfTimes()
    mockExpressionConfig.getGeometryAPI _ expects * returning null anyNumberOfTimes()
    mockExpressionConfig.getIndexSystem _ expects * returning mockIndexSystem anyNumberOfTimes()
    mockExpressionConfig.getCellIdType _ expects() returning LongType anyNumberOfTimes()
    mockRasterAPI.raster _ expects "path" returning mockRaster anyNumberOfTimes()
    mockRasterAPI.enable _ expects() returning mockRasterAPI anyNumberOfTimes()
    mockRaster.cleanUp _ expects() returning null anyNumberOfTimes()
    mockRaster.getGeoTransform _ expects() returning geoTransform anyNumberOfTimes()
    mockRaster.transformBands[Map[Long, _]] _ expects * returning cellMeasurePairs anyNumberOfTimes()
    (mockRasterBand.transformValues[(Long, Double)](_: (Int, Int, Double) => (Long, Double), _: (Long, Double))) expects(*, *) returning Seq(
      Seq((1L, 1.0)), Seq((2L, 2.0)), Seq((3L, 3.0)), Seq((4L, 4.0)), Seq((5L, 5.0)), Seq((6L, 6.0))
    ) anyNumberOfTimes()
    mockPathExpression.toString _ expects() returning "left" anyNumberOfTimes()
    mockResolutionExpression.toString _ expects() returning "right" anyNumberOfTimes()
    mockIndexSystem.pointToIndex _ expects(*, *, *) returning 1L anyNumberOfTimes()
    cellMeasurePairs.foreach(
      pairs => pairs.foreach {
        case (cellId, _) => mockIndexSystem.serializeCellId _ expects cellId returning UTF8String.fromString("1") anyNumberOfTimes()
      }
    )
  }

  // DummyExpr needs mocks to be set up before it is instantiated, so we call doMock() before declaring the class
  doMock()

  // Mocking doesn't work well with templates, so we create a dummy class to extend the abstract class
  // We are using Abs as a template in order to test makeCopy which is linked to GenericExpressionFactory
  abstract class DummyExpr extends RasterToGridExpression[Add, Int](
    mockPathExpression, mockResolutionExpression, BinaryType, mockExpressionConfig
  ) {
    // For partial mocking, make methods that are testable final, scalamock will not mock final methods
    override final def rasterTransform(raster: MosaicRaster, arg1: Any): Any =
      super.rasterTransform(raster, arg1)

    override final def pixelTransformer(gt: Seq[Double], resolution: Int)(x: Int, y: Int, value: Double): (Long, Double) =
      super.pixelTransformer(gt, resolution)(x, y, value)

    override final def bandTransformer(band: MosaicRasterBand, resolution: Int, gt: Seq[Double]): Map[Long, Int] =
      super.bandTransformer(band, resolution, gt)

  }

  val mockExpression: DummyExpr = mock[DummyExpr]


  test("RasterToGridExpression should implement pixelTransformer") {
    doMock()

    val result = mockExpression.pixelTransformer(geoTransform, 1)(1, 1, 1.0)

    result shouldBe a[(Long, Double)]
    result shouldBe(1L, 1.0)
  }

  test("RasterToGridExpression should implement rasterTransform") {
    doMock()

    val result = mockExpression.rasterTransform(mockRaster, 1)

    result shouldBe a[GenericArrayData]
    result.asInstanceOf[GenericArrayData].array.length shouldBe 6
  }

  test("RasterToGridExpression should implement bandTransformer") {
    doMock()

    mockExpression.valuesCombiner _ expects * returning 1 anyNumberOfTimes()

    val result = mockExpression.bandTransformer(mockRasterBand, 1, geoTransform)

    result shouldBe a[Map[Long, Int]]
    result.keys.toSeq.length shouldBe 6
  }


}
