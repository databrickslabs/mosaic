package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.ClassTag

/**
  * Base class for all raster expressions that take no arguments. It provides
  * the boilerplate code needed to create a function builder for a given
  * expression. It minimises amount of code needed to create a new expression.
  * @param pathExpr
  *   The expression for the raster path.
  * @param outputType
  *   The output type of the result.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterExpression[T <: Expression: ClassTag](
    pathExpr: Expression,
    outputType: DataType,
    expressionConfig: MosaicExpressionConfig
) extends UnaryExpression
      with NullIntolerant
      with Serializable {

    /**
      * The raster API to be used. Enable the raster so that subclasses dont
      * need to worry about this.
      */
    protected val rasterAPI: RasterAPI = RasterAPI(expressionConfig.getRasterAPI)
    rasterAPI.enable()

    override def child: Expression = pathExpr

    /** Output Data Type */
    override def dataType: DataType = outputType

    /**
      * The function to be overriden by the extending class. It is called when
      * the expression is evaluated. It provides the raster to the expression.
      * It abstracts spark serialization from the caller.
      * @param raster
      *   The raster to be used.
      * @return
      *   The result of the expression.
      */
    def rasterTransform(raster: MosaicRaster): Any

    /**
      * Evaluation of the expression. It evaluates the raster path and the loads
      * the raster from the path. It handles the clean up of the raster before
      * returning the results.
      * @param inputPath
      *   The path to the raster. It is a UTF8String.
      *
      * @return
      *   The result of the expression.
      */
    override def nullSafeEval(inputPath: Any): Any = {
        val path = inputPath.asInstanceOf[UTF8String].toString

        val raster = rasterAPI.raster(path)
        val result = rasterTransform(raster)

        raster.cleanUp()
        result
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 1, expressionConfig)

    override def withNewChildInternal(newFirst: Expression): Expression = makeCopy(Array(newFirst))

}
