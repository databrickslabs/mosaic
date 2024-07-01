package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.CombineAVG
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.core.types.model.RasterTile.{deserialize => deserializeTile}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpressionSerialization
import com.databricks.labs.mosaic.functions.ExprConfig
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
                                rasterExpr: Expression,
                                exprConfig: ExprConfig,
                                mutableAggBufferOffset: Int = 0,
                                inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[ArrayBuffer[Any]]
      with UnaryLike[Expression]
      with RasterExpressionSerialization {

    GDAL.enable(exprConfig)

    override lazy val deterministic: Boolean = true

    override val child: Expression = rasterExpr

    override val nullable: Boolean = false

    protected val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(exprConfig.getIndexSystem)

    protected val cellIdDataType: DataType = indexSystem.getCellIdDataType

    // serialize data type
    override lazy val dataType: DataType = {
        RasterTileType(rasterExpr, exprConfig.isRasterUseCheckpoint)
    }

    private lazy val projection = UnsafeProjection.create(Array[DataType](ArrayType(elementType = dataType, containsNull = false)))

    private lazy val row = new UnsafeRow(1)

    override def prettyName: String = "rst_combine_avg_agg"

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
        GDAL.enable(exprConfig)

        if (buffer.isEmpty) {
            null
        } else if (buffer.size == 1) {
            val result = buffer.head
            buffer.clear()
            result
        } else {

            // Do do move the expression
            var tiles = buffer.map(row => deserializeTile(
                row.asInstanceOf[InternalRow], cellIdDataType, Option(exprConfig))
            )
            buffer.clear()

            // If merging multiple index rasters, the index value is dropped
            val idx = if (tiles.map(_.index).groupBy(identity).size == 1) tiles.head.index else null
            var combined = CombineAVG.compute(tiles.map(_.raster), Option(exprConfig))

            val resultType = RasterTile.getRasterType(dataType)
            var result = RasterTile(idx, combined, resultType).formatCellId(indexSystem)
            val serialized = result.serialize(resultType, doDestroy = true, Option(exprConfig))

            tiles.foreach(_.flushAndDestroy())
            result.flushAndDestroy()
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

    override protected def withNewChildInternal(newChild: Expression): RST_CombineAvgAgg = copy(rasterExpr = newChild)

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
