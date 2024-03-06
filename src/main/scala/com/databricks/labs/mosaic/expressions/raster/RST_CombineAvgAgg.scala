package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.raster.operator.CombineAVG
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile.{deserialize => deserializeTile}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpressionSerialization
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.collection.mutable.ArrayBuffer

/**
  * Returns a new raster that is a result of combining an array of rasters using
  * average of pixels.
  */
//noinspection DuplicatedCode
case class RST_CombineAvgAgg(
    tileExpr: Expression,
    expressionConfig: MosaicExpressionConfig,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[ArrayBuffer[Any]]
      with UnaryLike[Expression]
      with RasterExpressionSerialization {

    override lazy val deterministic: Boolean = true
    override val child: Expression = tileExpr
    override val nullable: Boolean = false
    override lazy val dataType: DataType = RasterTileType(expressionConfig.getCellIdType, tileExpr)
    lazy val tileType: DataType = dataType.asInstanceOf[RasterTileType].rasterType
    override def prettyName: String = "rst_combine_avg_agg"
    val cellIDType: DataType = expressionConfig.getCellIdType

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
        GDAL.enable(expressionConfig)

        if (buffer.isEmpty) {
            null
        } else if (buffer.size == 1) {
            val result = buffer.head
            buffer.clear()
            result
        } else {

            // Do do move the expression
            var tiles = buffer.map(row => deserializeTile(row.asInstanceOf[InternalRow], cellIDType, tileType))
            buffer.clear()

            // If merging multiple index rasters, the index value is dropped
            val idx = if (tiles.map(_.getIndex).groupBy(identity).size == 1) tiles.head.getIndex else null
            var combined = CombineAVG.compute(tiles.map(_.getRaster)).flushCache()

            val result = MosaicRasterTile(idx, combined)
                .formatCellId(IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem))
                .serialize(tileType)

            tiles.foreach(RasterCleaner.dispose)
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

    override protected def withNewChildInternal(newChild: Expression): RST_CombineAvgAgg = copy(tileExpr = newChild)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_CombineAvgAgg {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[RST_CombineAvgAgg].getCanonicalName,
          db.orNull,
          "rst_combine_avg_agg",
          """
            |    _FUNC_(tiles)) - Aggregate to combine raster tiles using an average of pixels.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(raster_tile);
            |        {index_id, raster, parent_path, driver}
            |  """.stripMargin,
          "",
          "agg_funcs",
          "1.0",
          "",
          "built-in"
        )

}
