package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.MergeRasters
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.collection.mutable.ArrayBuffer

/** Merges rasters into a single raster. */
//noinspection DuplicatedCode
case class RST_MergeAgg(
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
    override def prettyName: String = RST_MergeAgg.name

    private lazy val projection = UnsafeProjection.create(Array[DataType](ArrayType(elementType = dataType, containsNull = false)))
    private lazy val row = new UnsafeRow(1)

    def update(buffer: ArrayBuffer[Any], input: InternalRow): ArrayBuffer[Any] = {
        val value = child.eval(input)
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
        } else if (buffer.size == 1) {
            buffer.head
        } else {

            // This is a trick to get the rasters sorted by their parent path to ensure more consistent results
            // when merging rasters with large overlaps
            val tiles = buffer
                .map(row => RasterSerializationUtil.rowToTile(row.asInstanceOf[InternalRow], rasterType))
                .sortBy(_._2.GetDescription())

            // If merging multiple index rasters, the index value is dropped
            val idx: Long = if (tiles.map(_._1).groupBy(identity).size == 1) tiles.head._1 else -1L
            val (res, resMtd) = MergeRasters.merge(tiles.map(_._2).toArray, tiles.head._3)

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

    override protected def withNewChildInternal(newChild: Expression): RST_MergeAgg = copy(tileExpr = newChild)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MergeAgg extends WithExpressionInfo {

    override def name: String = "gbx_rst_merge_agg"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => RST_MergeAgg(c(0))

    /* FOR `DESCRIBE FUNCTION EXTENDED <_FUNC_>` */
    override def description: String =
        "Aggregates raster tiles into a single raster."

    override def usageArgs: String = "tile"

    override def examples: String = {
        s"""
           |    Examples:
           |      > SELECT _FUNC_(_ARGS_) AS tile
           |        FROM table
           |        GROUP BY date;
           |      ${_TILE_RESULT_}
           |  """.stripMargin
    }

    override def extendedUsageArgs: String = s"${_TILE_TYPE_}"

}
