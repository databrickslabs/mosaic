package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.RasterBandGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO.flushAndDestroy
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant}

import scala.reflect.ClassTag

/**
  * Base class for all tile band expressions that take no arguments. It
  * provides the boilerplate code needed to create a function builder for a
  * given expression. It minimises amount of code needed to create a new
  * expression.
  * @param rasterExpr
  *   The path to the tile if MOSAIC_RASTER_STORAGE is set to
  *   MOSAIC_RASTER_STORAGE_DISK. The bytes of the tile if
  *   MOSAIC_RASTER_STORAGE is set to MOSAIC_RASTER_STORAGE_BYTE.
  * @param bandExpr
  *   The expression for the band index.
  * @param returnsRaster
  *   for serialization handling.
  * @param exprConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterBandExpression[T <: Expression: ClassTag](
                                                                  rasterExpr: Expression,
                                                                  bandExpr: Expression,
                                                                  returnsRaster: Boolean,
                                                                  exprConfig: ExprConfig
) extends BinaryExpression
      with NullIntolerant
      with Serializable
      with RasterExpressionSerialization {

    override def left: Expression = rasterExpr

    override def right: Expression = bandExpr

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the tile band to the
      * expression. It abstracts spark serialization from the caller.
      * @param raster
      *   The tile to be used.
      * @param band
      *   The band to be used.
      * @return
      *   The result of the expression.
      */
    def bandTransform(raster: RasterTile, band: RasterBandGDAL): Any

    /**
      * Evaluation of the expression. It evaluates the tile path and the loads
      * the tile from the path. It evaluates the band index and loads the
      * specified band. It handles the clean up of the tile before returning
      * the results.
      *
      * @param inputRaster
      *   The path to the tile if MOSAIC_RASTER_STORAGE is set to
      *   MOSAIC_RASTER_STORAGE_DISK. The bytes of the tile if
      *   MOSAIC_RASTER_STORAGE is set to MOSAIC_RASTER_STORAGE_BYTE.
      * @param inputBand
      *   The band index to be used. It is an Int.
      * @return
      *   The result of the expression.
      */
    // noinspection DuplicatedCode
    override def nullSafeEval(inputRaster: Any, inputBand: Any): Any = {
        GDAL.enable(exprConfig)
        var tile = RasterTile.deserialize(
            inputRaster.asInstanceOf[InternalRow],
            exprConfig.getCellIdType,
            Option(exprConfig)
        )
        val bandIndex = inputBand.asInstanceOf[Int]

        tile.initAndHydrateTile() // <- required

        val band = tile.raster.getBand(bandIndex)
        var result = bandTransform(tile, band)
        val resultType = {
            if (returnsRaster) RasterTile.getRasterType(dataType)
            else dataType
        }
        val serialized = serialize(result, returnsRaster, resultType, doDestroy = true, exprConfig)

        tile.flushAndDestroy()

        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 2, exprConfig)

    override def withNewChildrenInternal(newFirst: Expression, newSecond: Expression): Expression =
        makeCopy(Array[AnyRef](newFirst, newSecond))

}
