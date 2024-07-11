package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant}

import scala.reflect.ClassTag

/**
  * Base class for all tile expressions that take two arguments. It provides
  * the boilerplate code needed to create a function builder for a given
  * expression. It minimises amount of code needed to create a new expression.
  *
  * @param rasterExpr
  *   The tile expression. It can be a path to a tile file or a byte array
  *   containing the tile file content.
  * @param arg1Expr
  *   The expression for the first argument.
  * @param returnsRaster
  *   for serialization handling.
  * @param exprConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class Raster1ArgExpression[T <: Expression: ClassTag](
                                                                  rasterExpr: Expression,
                                                                  arg1Expr: Expression,
                                                                  returnsRaster: Boolean,
                                                                  exprConfig: ExprConfig
) extends BinaryExpression
      with NullIntolerant
      with Serializable
      with RasterExpressionSerialization {

    override def left: Expression = rasterExpr

    override def right: Expression = arg1Expr

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the tile and the arguments to
      * the expression. It abstracts spark serialization from the caller.
      * @param raster
      *   The tile to be used.
      * @param arg1
      *   The first argument.
      * @return
      *   A result of the expression.
      */
    def rasterTransform(raster: RasterTile, arg1: Any): Any

    /**
      * Evaluation of the expression. It evaluates the tile path and the loads
      * the tile from the path. It handles the clean up of the tile before
      * returning the results.
      *
      * @param input
      *   The input to the expression. It can be a path to a tile file or a
      *   byte array containing the tile file content.
      * @param arg1
      *   The first argument.
      * @return
      *   The result of the expression.
      */
    // noinspection DuplicatedCode
    override def nullSafeEval(input: Any, arg1: Any): Any = {
        GDAL.enable(exprConfig)
        var tile = RasterTile.deserialize(
            input.asInstanceOf[InternalRow],
            exprConfig.getCellIdType,
            Option(exprConfig)
        )
        var result = rasterTransform(tile, arg1)
        val resultType = {
            if (returnsRaster) RasterTile.getRasterType(dataType)
            else dataType
        }
        val serialized = serialize(result, returnsRaster, resultType, doDestroy = true, exprConfig)

        tile.flushAndDestroy()
        tile = null
        result = null

        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 2, exprConfig)

    override def withNewChildrenInternal(
        newFirst: Expression,
        newArg1: Expression
    ): Expression = makeCopy(Array(newFirst, newArg1))

}
