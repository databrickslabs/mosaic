package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.raster.operator.merge.MergeRasters
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpressionSerialization
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType}

import scala.collection.mutable.ArrayBuffer

/** Merges rasters into a single raster. */
//noinspection DuplicatedCode
case class RST_MergeAgg(
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
    override lazy val dataType: DataType = {
        GDAL.enable(expressionConfig)
        RasterTileType(expressionConfig.getCellIdType, tileExpr, expressionConfig.isRasterUseCheckpoint)
    }
    override def prettyName: String = "rst_merge_agg"

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
        GDAL.enable(expressionConfig)

        if (buffer.isEmpty) {
            null
        } else if (buffer.size == 1) {
            buffer.head
        } else {

            // This is a trick to get the rasters sorted by their parent path to ensure more consistent results
            // when merging rasters with large overlaps
            val rasterType = RasterTileType(tileExpr, expressionConfig.isRasterUseCheckpoint).rasterType
            var tiles = buffer
                .map(row =>
                    MosaicRasterTile.deserialize(
                      row.asInstanceOf[InternalRow],
                      expressionConfig.getCellIdType,
                      rasterType
                    )
                )
                .sortBy(_.getParentPath)

            // If merging multiple index rasters, the index value is dropped
            val idx = if (tiles.map(_.getIndex).groupBy(identity).size == 1) tiles.head.getIndex else null
            var merged = MergeRasters.merge(tiles.map(_.getRaster)).flushCache()

            val result = MosaicRasterTile(idx, merged)
                .formatCellId(IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem))
                .serialize(rasterType)

            tiles.foreach(RasterCleaner.dispose(_))
            RasterCleaner.dispose(merged)

            tiles = null
            merged = null

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

    override protected def withNewChildInternal(newChild: Expression): RST_MergeAgg = copy(tileExpr = newChild)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MergeAgg {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[RST_MergeAgg].getCanonicalName,
          db.orNull,
          "rst_merge_agg",
          """
            |    _FUNC_(tiles)) - Aggregate merge of raster tiles.
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
