package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.NO_PATH_STRING
import com.databricks.labs.mosaic.core.index.IndexSystemFactory
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.merge.MergeRasters
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpressionSerialization
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, StringType}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/** Merges rasters into a single tile. */
//noinspection DuplicatedCode
case class RST_MergeAgg(
                           rastersExpr: Expression,
                           exprConfig: ExprConfig,
                           mutableAggBufferOffset: Int = 0,
                           inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[ArrayBuffer[Any]]
      with UnaryLike[Expression]
      with RasterExpressionSerialization {

    GDAL.enable(exprConfig)

    override lazy val deterministic: Boolean = true

    override val child: Expression = rastersExpr

    override val nullable: Boolean = false

    // serialize data type (keep as def)
    override def dataType: DataType = {
        RasterTileType(exprConfig.getCellIdType, rastersExpr, exprConfig.isRasterUseCheckpoint)
    }

    private lazy val row = new UnsafeRow(1)

    override def prettyName: String = "rst_merge_agg"

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
        GDAL.enable(exprConfig)

        if (buffer.isEmpty) {
            null
        } else if (buffer.size == 1) {
            buffer.head
        } else {
            // [1] Deserialize "as-provided" (may be BinaryType or StringType)
            var loadTiles = buffer
                .map(row => {
                    val tile = RasterTile.deserialize(
                        row.asInstanceOf[InternalRow],
                        exprConfig.getCellIdType,
                        Option(exprConfig)
                    )
                    val raster = tile.raster
                    if (raster.isEmptyRasterGDAL || raster.isEmpty) {
                        // empty raster
                        (false, tile)
                    } else {
                        // non-empty raster
                        (true, tile)
                    }
                })
            // [2] Filter out invalid tiles
            var (valid, invalid) = loadTiles.partition(_._1)                       // <- true goes to valid
            invalid.flatMap(t => Option(t._2.raster)).foreach(_.flushAndDestroy()) // <- destroy invalid

            // [3] Sort valid tiles by parent (or best path available)
            // - This is a trick to get the rasters sorted by their parent path to ensure
            //   more consistent results when merging rasters with large overlaps.
            var tiles = valid.map(_._2).sortBy(_.raster.identifyPseudoPathOpt().getOrElse(NO_PATH_STRING))

            // [4] Keep or drop index value
            // - If merging multiple index rasters, the index value is dropped
            val idx =
                if (Try(tiles.map(_.index).groupBy(identity).size).getOrElse(0) == 1) tiles.head.index
                else null

            // [5] Merge tiles
            // - specify the result type (binary or string) based on config.
            var merged = MergeRasters.merge(tiles.map(_.raster), Option(exprConfig))
            val resultType = RasterTile.getRasterType(dataType)
            var result = RasterTile(idx, merged, resultType).formatCellId(
                IndexSystemFactory.getIndexSystem(exprConfig.getIndexSystem))

            val serialized = result.serialize(resultType, doDestroy = true, Option(exprConfig))

            // [6] Cleanup
            tiles.foreach(_.flushAndDestroy())
            loadTiles = null
            tiles = null
            invalid = null
            merged = null
            result = null

            serialized
        }
    }

//    private def

    override def serialize(obj: ArrayBuffer[Any]): Array[Byte] = {
        // serialize the grouped data into a buffer for efficiency [happens first]
        val array = new GenericArrayData(obj.toArray)
        // projection was a lazy val; we need it to be more dynamic
        // since rasters may be binary or string
        val projection = UnsafeProjection.create(Array[DataType](ArrayType(elementType = this.dataType, containsNull = false)))
        projection.apply(InternalRow.apply(array)).getBytes
    }

    override def deserialize(bytes: Array[Byte]): ArrayBuffer[Any] = {
        // deserialize a single row of the buffer
        val buffer = createAggregationBuffer() // <- starts empty
        row.pointTo(bytes, bytes.length)
        row.getArray(0).foreach(this.dataType, (_, x: Any) => buffer += x)
        buffer
    }

    override protected def withNewChildInternal(newChild: Expression): RST_MergeAgg = copy(rastersExpr = newChild)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MergeAgg {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[RST_MergeAgg].getCanonicalName,
          db.orNull,
          "rst_merge_agg",
          """
            |    _FUNC_(tiles)) - Aggregate merge of tile tiles.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(raster_tile);
            |        {index_id, tile, parent_path, driver}
            |  """.stripMargin,
          "",
          "agg_funcs",
          "1.0",
          "",
          "built-in"
        )

}
