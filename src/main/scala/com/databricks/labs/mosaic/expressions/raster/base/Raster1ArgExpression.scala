package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant}

import scala.reflect.ClassTag

/**
  * Base class for all raster expressions that take two arguments. It provides
  * the boilerplate code needed to create a function builder for a given
  * expression. It minimises amount of code needed to create a new expression.
  *
  * @param rasterExpr
  *   The raster expression. It can be a path to a raster file or a byte array
  *   containing the raster file content.
  * @param arg1Expr
  *   The expression for the first argument.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class Raster1ArgExpression[T <: Expression: ClassTag](
    rasterExpr: Expression,
    arg1Expr: Expression,
    returnsRaster: Boolean,
    expressionConfig: MosaicExpressionConfig
) extends BinaryExpression
      with NullIntolerant
      with Serializable
      with RasterExpressionSerialization {

    override def left: Expression = rasterExpr

    override def right: Expression = arg1Expr

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the raster and the arguments to
      * the expression. It abstracts spark serialization from the caller.
      * @param raster
      *   The raster to be used.
      * @param arg1
      *   The first argument.
      * @return
      *   A result of the expression.
      */
    def rasterTransform(raster: MosaicRasterTile, arg1: Any): Any

    /**
      * Evaluation of the expression. It evaluates the raster path and the loads
      * the raster from the path. It handles the clean up of the raster before
      * returning the results.
      *
      * @param input
      *   The input to the expression. It can be a path to a raster file or a
      *   byte array containing the raster file content.
      * @param arg1
      *   The first argument.
      * @return
      *   The result of the expression.
      */
    // noinspection DuplicatedCode
    override def nullSafeEval(input: Any, arg1: Any): Any = {
        GDAL.enable(expressionConfig)
        val rasterType = RasterTileType(rasterExpr).rasterType
        val tile = MosaicRasterTile.deserialize(
          input.asInstanceOf[InternalRow],
          expressionConfig.getCellIdType,
          rasterType
        )
        val raster = tile.getRaster
        val result = rasterTransform(tile, arg1)
        val serialized = serialize(result, returnsRaster, rasterType, expressionConfig)
        RasterCleaner.dispose(raster)
        RasterCleaner.dispose(result)
        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 2, expressionConfig)

    override def withNewChildrenInternal(
        newFirst: Expression,
        newArg1: Expression
    ): Expression = makeCopy(Array(newFirst, newArg1))

}
