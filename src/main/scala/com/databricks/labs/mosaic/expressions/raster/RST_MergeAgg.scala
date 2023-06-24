package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.core.raster.gdal_raster.RasterCleaner
import com.databricks.labs.mosaic.core.raster.operator.merge.MergeRasters
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpressionSerialization
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

/**
  * Returns a set of new rasters with the specified tile size (tileWidth x
  * tileHeight).
  */
//noinspection DuplicatedCode
case class RST_MergeAgg(
    rasterExpr: Expression,
    expressionConfig: MosaicExpressionConfig,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends TypedImperativeAggregate[Array[Byte]]
      with UnaryLike[Expression]
      with RasterExpressionSerialization {

    override lazy val deterministic: Boolean = true
    override val child: Expression = rasterExpr
    override val nullable: Boolean = false
    override val dataType: DataType = BinaryType
    override def prettyName: String = "rst_merge_agg"

    val rasterAPI: RasterAPI = RasterAPI.apply(expressionConfig.getRasterAPI)

    override def update(accumulator: Array[Byte], inputRow: InternalRow): Array[Byte] = {
        val state = accumulator

        if (state.isEmpty) {
            val raster = rasterAPI.readRaster(rasterExpr.eval(inputRow), rasterExpr.dataType)
            val result = raster.writeToBytes()
            RasterCleaner.dispose(raster)
            result
        } else {
            val partialRaster = rasterAPI.readRaster(state, BinaryType)
            val newRaster = rasterAPI.readRaster(rasterExpr.eval(inputRow), rasterExpr.dataType)

            val mergedRaster = MergeRasters.merge(Seq(partialRaster, newRaster))
            val newState = serialize(mergedRaster, returnsRaster = true, BinaryType, rasterAPI, expressionConfig)

            RasterCleaner.dispose(partialRaster)
            RasterCleaner.dispose(newRaster)
            RasterCleaner.dispose(mergedRaster)

            newState.asInstanceOf[Array[Byte]]
        }
    }

    override def merge(accumulator: Array[Byte], input: Array[Byte]): Array[Byte] = {
        if (accumulator.isEmpty && input.isEmpty) {
            Array.empty[Byte]
        } else if (accumulator.isEmpty) {
            input
        } else {
            val leftPartial = rasterAPI.readRaster(accumulator, BinaryType)
            val rightPartial = rasterAPI.readRaster(input, BinaryType)
            val mergedRaster = MergeRasters.merge(Seq(leftPartial, rightPartial))
            val newState = serialize(mergedRaster, returnsRaster = true, BinaryType, rasterAPI, expressionConfig)

            RasterCleaner.dispose(leftPartial)
            RasterCleaner.dispose(rightPartial)
            RasterCleaner.dispose(mergedRaster)

            newState.asInstanceOf[Array[Byte]]
        }
    }

    override def createAggregationBuffer(): Array[Byte] = Array.empty[Byte]

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
        copy(inputAggBufferOffset = newInputAggBufferOffset)

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
        copy(mutableAggBufferOffset = newMutableAggBufferOffset)

    override def eval(buffer: Array[Byte]): Any = buffer

    override def serialize(buffer: Array[Byte]): Array[Byte] = buffer

    override def deserialize(storageFormat: Array[Byte]): Array[Byte] = storageFormat

    override protected def withNewChildInternal(newChild: Expression): RST_MergeAgg = copy(rasterExpr = newChild)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MergeAgg {

    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[RST_MergeAgg].getCanonicalName,
          db.orNull,
          "rst_merge_agg",
          """
            |    _FUNC_(tiles)) - Merges rasters into a single raster.
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(rasters);
            |        raster
            |  """.stripMargin,
          "",
          "agg_funcs",
          "1.0",
          "",
          "built-in"
        )

}
