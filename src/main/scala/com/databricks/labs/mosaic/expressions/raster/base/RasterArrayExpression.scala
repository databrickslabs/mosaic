package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}

import scala.reflect.ClassTag

/**
  * Base class for all tile expressions that take two arguments. It provides
  * the boilerplate code needed to create a function builder for a given
  * expression. It minimises amount of code needed to create a new expression.
  *
  * @param rastersExpr
  *   The rasters expression. It is an array column containing rasters as either
  *   paths or as content byte arrays.
  * @param returnsRaster
  *   Whether tile is returned.
  * @param exprConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterArrayExpression[T <: Expression: ClassTag](
                                                                   rastersExpr: Expression,
                                                                   returnsRaster: Boolean,
                                                                   exprConfig: ExprConfig
) extends UnaryExpression
      with NullIntolerant
      with Serializable
      with RasterExpressionSerialization {

    override def child: Expression = rastersExpr

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the rasters to the expression.
      * It abstracts spark serialization from the caller.
      * @param rasters
      *   The sequence of rasters to be used.
      * @return
      *   A result of the expression.
      */
    def rasterTransform(rasters: Seq[RasterTile]): Any

    /**
      * Evaluation of the expression. It evaluates the tile path and the loads
      * the tile from the path. It handles the clean up of the tile before
      * returning the results.
      * @param input
      *   The InternalRow of the expression. It contains an array containing
      *   tile tiles. It may be used for other argument expressions so it is
      *   passed to rasterTransform.
      *
      * @return
      *   The result of the expression.
      */
    override def nullSafeEval(input: Any): Any = {
        GDAL.enable(exprConfig)
        var tiles = RasterArrayUtils.getTiles(input, rastersExpr, exprConfig)
        var result = rasterTransform(tiles)
        val resultType = {
            if (returnsRaster) RasterTile.getRasterType(dataType)
            else dataType
        }
        val serialized = serialize(result, returnsRaster, resultType, doDestroy = true, exprConfig)

        tiles.foreach(_.raster.flushAndDestroy())
        tiles = null
        result = null

        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 1, exprConfig)

    override def withNewChildInternal(
        newFirst: Expression
    ): Expression = makeCopy(Array(newFirst))

}
