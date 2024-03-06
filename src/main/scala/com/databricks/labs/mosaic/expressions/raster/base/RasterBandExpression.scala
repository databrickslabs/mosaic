package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterBandGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant}

import scala.reflect.ClassTag

/**
  * Base class for all raster band expressions that take no arguments. It
  * provides the boilerplate code needed to create a function builder for a
  * given expression. It minimises amount of code needed to create a new
  * expression.
  * @param rasterExpr
  *   The path to the raster if MOSAIC_RASTER_STORAGE is set to
  *   MOSAIC_RASTER_STORAGE_DISK. The bytes of the raster if
  *   MOSAIC_RASTER_STORAGE is set to MOSAIC_RASTER_STORAGE_BYTE.
  * @param bandExpr
  *   The expression for the band index.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterBandExpression[T <: Expression: ClassTag](
    rasterExpr: Expression,
    bandExpr: Expression,
    returnsRaster: Boolean,
    expressionConfig: MosaicExpressionConfig
) extends BinaryExpression
      with NullIntolerant
      with Serializable
      with RasterExpressionSerialization {

    override def left: Expression = rasterExpr

    override def right: Expression = bandExpr

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the raster band to the
      * expression. It abstracts spark serialization from the caller.
      * @param raster
      *   The raster to be used.
      * @param band
      *   The band to be used.
      * @return
      *   The result of the expression.
      */
    def bandTransform(raster: MosaicRasterTile, band: MosaicRasterBandGDAL): Any

    /**
      * Evaluation of the expression. It evaluates the raster path and the loads
      * the raster from the path. It evaluates the band index and loads the
      * specified band. It handles the clean up of the raster before returning
      * the results.
      *
      * @param inputRaster
      *   The path to the raster if MOSAIC_RASTER_STORAGE is set to
      *   MOSAIC_RASTER_STORAGE_DISK. The bytes of the raster if
      *   MOSAIC_RASTER_STORAGE is set to MOSAIC_RASTER_STORAGE_BYTE.
      * @param inputBand
      *   The band index to be used. It is an Int.
      * @return
      *   The result of the expression.
      */
    // noinspection DuplicatedCode
    override def nullSafeEval(inputRaster: Any, inputBand: Any): Any = {
        GDAL.enable(expressionConfig)
        val rasterType = RasterTileType(rasterExpr).rasterType
        val tile = MosaicRasterTile.deserialize(
            inputRaster.asInstanceOf[InternalRow],
            expressionConfig.getCellIdType,
            rasterType
        )
        val bandIndex = inputBand.asInstanceOf[Int]

        val band = tile.getRaster.getBand(bandIndex)
        val result = bandTransform(tile, band)

        val serialized = serialize(result, returnsRaster, rasterType, expressionConfig)
        RasterCleaner.dispose(tile)
        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 2, expressionConfig)

    override def withNewChildrenInternal(newFirst: Expression, newSecond: Expression): Expression =
        makeCopy(Array[AnyRef](newFirst, newSecond))

}
