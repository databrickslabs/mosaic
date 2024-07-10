package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.pixel.PixelCombineRasters
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpressionSerialization
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer

/**
  * Returns a new tile that is a result of combining an array of rasters using
  * average of pixels.
  */
//noinspection DuplicatedCode
case class RST_DerivedBandAgg(
                                 rastersExpr: Expression,
                                 pythonFuncExpr: Expression,
                                 funcNameExpr: Expression,
                                 exprConfig: ExprConfig,
                                 mutableAggBufferOffset: Int = 0,
                                 inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[ArrayBuffer[Any]]
      with TernaryLike[Expression]
      with RasterExpressionSerialization {

    GDAL.enable(exprConfig)

    override lazy val deterministic: Boolean = true

    override val nullable: Boolean = false

    override lazy val dataType: DataType = {
        RasterTileType(exprConfig.getCellIdType, rastersExpr, exprConfig.isRasterUseCheckpoint)
    }

    private lazy val projection = UnsafeProjection.create(Array[DataType](ArrayType(elementType = dataType, containsNull = false)))

    private lazy val row = new UnsafeRow(1)

    override def prettyName: String = "rst_combine_avg_agg"

    override def first: Expression = rastersExpr

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
        GDAL.enable(exprConfig)
        if (buffer.isEmpty) {
            null
        } else {

            // This works for Literals only
            val pythonFunc = pythonFuncExpr.eval(null).asInstanceOf[UTF8String].toString
            val funcName = funcNameExpr.eval(null).asInstanceOf[UTF8String].toString

            // Do do move the expression
            var tiles = buffer.map(row =>
                RasterTile.deserialize(
                    row.asInstanceOf[InternalRow],
                    exprConfig.getCellIdType,
                    Option(exprConfig)
                )
            )

            // If merging multiple index rasters, the index value is dropped
            val idx = if (tiles.map(_.index).groupBy(identity).size == 1) tiles.head.index else null
            var combined = PixelCombineRasters.combine(tiles.map(_.raster), pythonFunc, funcName, Option(exprConfig))
            val resultType = RasterTile.getRasterType(dataType)
            var result = RasterTile(idx, combined, resultType)
                .formatCellId(IndexSystemFactory.getIndexSystem(exprConfig.getIndexSystem))

            // using serialize on the object vs from RasterExpressionSerialization
            val serialized = result.serialize(resultType, doDestroy = true, Option(exprConfig))

            tiles.foreach(_.flushAndDestroy())
            tiles = null
            combined = null
            result = null

            serialized
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
        copy(rastersExpr = newFirst, pythonFuncExpr = newSecond, funcNameExpr = newThird)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_DerivedBandAgg {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[RST_DerivedBandAgg].getCanonicalName,
          db.orNull,
          "rst_derived_band_agg",
          """
            |    _FUNC_(tiles)) - Aggregate which combines tile tiles using provided python function.
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
            |        {index_id, tile, parent_path, driver}
            |  """.stripMargin,
          "",
          "agg_funcs",
          "1.0",
          "",
          "built-in"
        )

}
