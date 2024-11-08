package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

/**
  * Base class for all raster expressions that take no arguments. It provides
  * the boilerplate code needed to create a function builder for a given
  * expression. It minimises amount of code needed to create a new expression.
  * @param rasterExpr
  *   The expression for the raster. If the raster is stored on disc, the path
  *   to the raster is provided. If the raster is stored in memory, the bytes of
  *   the raster are provided.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterExpression[T <: Expression: ClassTag](
    rasterExpr: Expression,
    returnsRaster: Boolean,
    expressionConfig: MosaicExpressionConfig
) extends UnaryExpression
      with NullIntolerant
      with Serializable
      with RasterExpressionSerialization {

    protected val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem)

    protected val cellIdDataType: DataType = indexSystem.getCellIdDataType

    override def child: Expression = rasterExpr

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the raster to the expression.
      * It abstracts spark serialization from the caller.
      * @param raster
      *   The raster to be used.
      * @return
      *   The result of the expression.
      */
    def rasterTransform(raster: MosaicRasterTile): Any

    /**
      * Evaluation of the expression. It evaluates the raster path and the loads
      * the raster from the path. It handles the clean up of the raster before
      * returning the results.
      * @param input
      *   The input raster as either a path or bytes.
      *
      * @return
      *   The result of the expression.
      */
    override def nullSafeEval(input: Any): Any = {
        val rasterType = RasterTileType(rasterExpr, expressionConfig.isRasterUseCheckpoint).rasterType
        val tile = MosaicRasterTile.deserialize(
          input.asInstanceOf[InternalRow],
          cellIdDataType,
          rasterType
        )
        val result = rasterTransform(tile)
        val serialized = serialize(result, returnsRaster, rasterType, expressionConfig)
        RasterCleaner.dispose(tile)
        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 1, expressionConfig)

    override def withNewChildInternal(newFirst: Expression): Expression = makeCopy(Array(newFirst))

}
