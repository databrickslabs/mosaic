package com.dblabs.spatial.rasterx.expressions.agg

import com.dblabs.spatial.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.dblabs.spatial.rasterx.gdal.RasterDriver
import com.dblabs.spatial.rasterx.operations.CombineAVG
import com.dblabs.spatial.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Returns a new raster that is a result of combining an array of rasters using
  * average of pixels.
  */
//noinspection DuplicatedCode
case class RST_CombineAvgAgg(
    tileExpr: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr(),
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[ArrayBuffer[Any]]
      with UnaryLike[Expression] {

    override lazy val deterministic: Boolean = true
    override val child: Expression = tileExpr
    override val nullable: Boolean = false
    lazy val rasterType: DataType = RST_ExpressionUtil.rasterType(tileExpr)
    override lazy val dataType: DataType = RST_ExpressionUtil.tileDataType(rasterType)
    override def prettyName: String = "rst_combine_avg_agg"
    val cellIDType: DataType = tileExpr.dataType.asInstanceOf[StructType].fields.head.dataType

    private lazy val projection = UnsafeProjection.create(Array[DataType](ArrayType(elementType = dataType, containsNull = false)))
    private lazy val row = new UnsafeRow(1)

    override def update(buffer: ArrayBuffer[Any], input: InternalRow): ArrayBuffer[Any] = {
        val value = child.eval(input)
        buffer += InternalRow.copyValue(value)
        buffer
    }

    override def merge(buffer: ArrayBuffer[Any], input: ArrayBuffer[Any]): ArrayBuffer[Any] = {
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
        } else if (buffer.size == 1) {
            val result = buffer.head
            buffer.clear()
            result
        } else {
            val tiles = buffer.map(row => RasterSerializationUtil.rowToTile(row.asInstanceOf[InternalRow], rasterType))
            buffer.clear()

            // If merging multiple index rasters, the index value is dropped
            val idx: Long = if (tiles.map(_._1).groupBy(identity).size == 1) tiles.head._1 else -1L
            val (res, resMtd) = CombineAVG.compute(tiles.map(_._2).toArray, tiles.head._3)

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

    override protected def withNewChildInternal(newChild: Expression): RST_CombineAvgAgg = copy(tileExpr = newChild)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_CombineAvgAgg extends WithExpressionInfo {

    override def name: String = "rst_combine_avg_agg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => RST_CombineAvgAgg(c(0))

}
