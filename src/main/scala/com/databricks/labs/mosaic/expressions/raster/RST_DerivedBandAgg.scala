package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.raster.operator.pixel.PixelCombineRasters
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpressionSerialization
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer

/**
  * Returns a new raster that is a result of combining an array of rasters using
  * average of pixels.
  */
//noinspection DuplicatedCode
case class RST_DerivedBandAgg(
    tileExpr: Expression,
    pythonFuncExpr: Expression,
    funcNameExpr: Expression,
    expressionConfig: MosaicExpressionConfig,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[ArrayBuffer[Any]]
      with TernaryLike[Expression]
      with RasterExpressionSerialization {

    override lazy val deterministic: Boolean = true
    override val nullable: Boolean = false
    override lazy val dataType: DataType = RasterTileType(expressionConfig.getCellIdType, tileExpr)
    override def prettyName: String = "rst_combine_avg_agg"

    private lazy val projection = UnsafeProjection.create(Array[DataType](ArrayType(elementType = dataType, containsNull = false)))
    private lazy val row = new UnsafeRow(1)

    override def first: Expression = tileExpr
    override def second: Expression = pythonFuncExpr
    override def third: Expression = funcNameExpr

    def update(buffer: ArrayBuffer[Any], input: InternalRow): ArrayBuffer[Any] = {
        val value = first.eval(input)
        buffer += InternalRow.copyValue(value)
        buffer
    }

    def merge(buffer: ArrayBuffer[Any], input: ArrayBuffer[Any]): ArrayBuffer[Any] = {
        buffer ++= input
    }

    override def createAggregationBuffer(): ArrayBuffer[Any] = ArrayBuffer.empty

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = newInputAggBufferOffset)

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = newMutableAggBufferOffset)

    override def eval(buffer: ArrayBuffer[Any]): Any = {
        GDAL.enable(expressionConfig)

        if (buffer.isEmpty) {
            null
        } else {

            // This works for Literals only
            val pythonFunc = pythonFuncExpr.eval(null).asInstanceOf[UTF8String].toString
            val funcName = funcNameExpr.eval(null).asInstanceOf[UTF8String].toString
            val rasterType = RasterTileType(tileExpr).rasterType

            // Do do move the expression
            var tiles = buffer.map(row =>
                MosaicRasterTile.deserialize(
                  row.asInstanceOf[InternalRow],
                  expressionConfig.getCellIdType,
                  rasterType
                )
            )

            // If merging multiple index rasters, the index value is dropped
            val idx = if (tiles.map(_.getIndex).groupBy(identity).size == 1) tiles.head.getIndex else null

            var combined = PixelCombineRasters.combine(tiles.map(_.getRaster), pythonFunc, funcName)

            val result = MosaicRasterTile(idx, combined)
                .formatCellId(IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem))
                .serialize(BinaryType)

            tiles.foreach(RasterCleaner.dispose(_))
            RasterCleaner.dispose(result)

            tiles = null
            combined = null

            result
        }
    }

    override def serialize(obj: ArrayBuffer[Any]): Array[Byte] = {
        val array = new GenericArrayData(obj.toArray)
        projection.apply(InternalRow.apply(array)).getBytes
    }

    override def deserialize(bytes: Array[Byte]): ArrayBuffer[Any] = {
        val buffer = createAggregationBuffer()
        row.pointTo(bytes, bytes.length)
        row.getArray(0).foreach(dataType, (_, x: Any) => buffer += x)
        buffer
    }

    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): RST_DerivedBandAgg =
        copy(tileExpr = newFirst, pythonFuncExpr = newSecond, funcNameExpr = newThird)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_DerivedBandAgg {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[RST_DerivedBandAgg].getCanonicalName,
          db.orNull,
          "rst_derived_band_agg",
          """
            |    _FUNC_(tiles)) - Aggregate which combines raster tiles using provided python function.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(raster_tile,
            |           'def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
            |              out_ar[:] = np.sum(in_ar, axis=0) / len(in_ar)
            |           ',
            |           'average'
            |       );
            |        {index_id, raster, parent_path, driver}
            |  """.stripMargin,
          "",
          "agg_funcs",
          "1.0",
          "",
          "built-in"
        )

}
