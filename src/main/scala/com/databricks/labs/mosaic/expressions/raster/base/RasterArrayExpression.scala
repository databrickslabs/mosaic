package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.core.raster.gdal_raster.RasterCleaner
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.reflect.ClassTag

/**
  * Base class for all raster expressions that take two arguments. It provides
  * the boilerplate code needed to create a function builder for a given
  * expression. It minimises amount of code needed to create a new expression.
  *
  * @param rastersExpr
  *   The rasters expression. It is an array column containing rasters as either
  *   paths or as content byte arrays.
  * @param outputType
  *   The output type of the result.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterArrayExpression[T <: Expression: ClassTag](
    rastersExpr: Expression,
    outputType: DataType,
    returnsRaster: Boolean,
    expressionConfig: MosaicExpressionConfig
) extends UnaryExpression
      with NullIntolerant
      with Serializable
      with RasterExpressionSerialization {

    /**
      * The raster API to be used. Enable the raster so that subclasses dont
      * need to worry about this.
      */
    protected val rasterAPI: RasterAPI = RasterAPI(expressionConfig.getRasterAPI)
    rasterAPI.enable()

    override def child: Expression = rastersExpr

    /** Output Data Type */
    override def dataType: DataType = if (returnsRaster) rastersExpr.dataType.asInstanceOf[ArrayType].elementType else outputType

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the rasters to the expression.
      * It abstracts spark serialization from the caller.
      * @param rasters
      *   The sequence of rasters to be used.
      * @return
      *   A result of the expression.
      */
    def rasterTransform(rasters: Seq[MosaicRaster]): Any

    /**
      * Evaluation of the expression. It evaluates the raster path and the loads
      * the raster from the path. It handles the clean up of the raster before
      * returning the results.
      * @param input
      *   The input to the expression. It is an array containing paths to raster
      *   files or byte arrays containing the raster files contents.
      *
      * @return
      *   The result of the expression.
      */
    override def nullSafeEval(input: Any): Any = {
        val rasterDT = rastersExpr.dataType.asInstanceOf[ArrayType].elementType
        val arrayData = input.asInstanceOf[ArrayData]
        val n = arrayData.numElements()
        val rasters = (0 until n).map(i => rasterAPI.readRaster(arrayData.get(i, rasterDT), rasterDT))
        val result = rasterTransform(rasters)
        val serialized = serialize(result, returnsRaster, dataType, rasterAPI, expressionConfig)
        rasters.foreach(RasterCleaner.dispose)
        RasterCleaner.dispose(result)
        serialized
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 1, expressionConfig)

    override def withNewChildInternal(
        newFirst: Expression
    ): Expression = makeCopy(Array(newFirst))

}
