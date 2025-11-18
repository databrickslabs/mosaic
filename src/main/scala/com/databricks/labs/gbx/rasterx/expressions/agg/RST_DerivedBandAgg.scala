package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.PixelCombineRasters
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}
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
    exprConfExpr: Expression = ExpressionConfigExpr(),
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[ArrayBuffer[Any]]
      with TernaryLike[Expression] {

    override lazy val deterministic: Boolean = true
    override val nullable: Boolean = false
    lazy val rasterType: DataType = RST_ExpressionUtil.rasterType(tileExpr)
    override lazy val dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def prettyName: String = RST_DerivedBandAgg.name

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
        val exprConf = ExpressionConfig.fromExpr(exprConfExpr)
        RST_ExpressionUtil.init(exprConf)

        if (buffer.isEmpty) {
            null
        } else {

            // This works for Literals only
            // If we decide we want to support columnar value pass we should move this to update
            // in update we have access to the row
            // note: if we do so, all values need to be the same, this is an agg
            val pythonFunc = pythonFuncExpr.eval(null).asInstanceOf[UTF8String].toString
            val funcName = funcNameExpr.eval(null).asInstanceOf[UTF8String].toString

            val tiles = buffer.map(row => RasterSerializationUtil.rowToTile(row.asInstanceOf[InternalRow], rasterType))
            buffer.clear()

            // If merging multiple index rasters, the index value is dropped
            val idx: Long = if (tiles.map(_._1).groupBy(identity).size == 1) tiles.head._1 else -1L

            val (res, resMtd) = PixelCombineRasters.combine(tiles.map(_._2).toArray, tiles.head._3, pythonFunc, funcName)

            val resRow = RasterSerializationUtil.tileToRow((idx, res, resMtd), rasterType, exprConf.hConf)

            tiles.foreach(t => RasterDriver.releaseDataset(t._2))
            RasterDriver.releaseDataset(res)

            resRow
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
object RST_DerivedBandAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_derivedband_agg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => RST_DerivedBandAgg(c(0), c(1), c(2))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Combines a group by statement over aggregated raster tiles by using the provided python function."

    override def usageArgs: String = "tile, pyfunc, func_name"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT
           |        gbx_rst_derivedband_agg(tile, py_func1, func1_name) AS tile
           |        FROM SELECT (
           |          date, tile,
           |          \"\"\"
           |          import numpy as np
           |          def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize,
           |          buf_radius, gt, **kwargs):
           |            out_ar[:] = np.sum(in_ar, axis=0) / len(in_ar)
           |          \"\"\" as py_func1,
           |          "average" as func1_name
           |          FROM table
           |        )
           |        GROUP BY date, py_func1, func1_name;
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}, pyfunc: String, func_name: String"

}
