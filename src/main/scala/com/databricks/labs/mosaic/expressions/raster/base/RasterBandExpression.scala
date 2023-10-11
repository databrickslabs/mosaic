package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.MosaicRasterBand
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.core.raster.gdal_raster.RasterCleaner
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType

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
  * @param outputType
  *   The output type of the result.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterBandExpression[T <: Expression: ClassTag](
    rasterExpr: Expression,
    bandExpr: Expression,
    outputType: DataType,
    returnsRaster: Boolean,
    expressionConfig: MosaicExpressionConfig
) extends BinaryExpression
      with NullIntolerant
      with Serializable
      with RasterExpressionSerialization {

    /**
      * The raster API to be used. Enable the raster so that subclasses dont
      * need to worry about this.
      */
    protected val rasterAPI: RasterAPI = RasterAPI(expressionConfig.getRasterAPI)
    rasterAPI.enable()

    override def left: Expression = rasterExpr

    override def right: Expression = bandExpr

    /** Output Data Type */
    override def dataType: DataType = outputType

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
    def bandTransform(raster: MosaicRasterTile, band: MosaicRasterBand): Any

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
        val tile = MosaicRasterTile.deserialize(inputRaster.asInstanceOf[InternalRow], expressionConfig.getCellIdType, rasterAPI)
        val bandIndex = inputBand.asInstanceOf[Int]

        val band = tile.raster.getBand(bandIndex)
        val result = bandTransform(tile, band)

        val serialized = serialize(result, returnsRaster, dataType, rasterAPI, expressionConfig)
        RasterCleaner.dispose(tile)
        RasterCleaner.dispose(result)
        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 2, expressionConfig)

    override def withNewChildrenInternal(newFirst: Expression, newSecond: Expression): Expression =
        makeCopy(Array[AnyRef](newFirst, newSecond))

}
