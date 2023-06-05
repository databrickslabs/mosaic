package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType

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
  * @param outputType
  *   The output type of the result.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class Raster1ArgExpression[T <: Expression: ClassTag](
    rasterExpr: Expression,
    arg1Expr: Expression,
    outputType: DataType,
    expressionConfig: MosaicExpressionConfig
) extends BinaryExpression
      with NullIntolerant
      with Serializable {

    /**
      * The raster API to be used. Enable the raster so that subclasses dont
      * need to worry about this.
      */
    protected val rasterAPI: RasterAPI = RasterAPI(expressionConfig.getRasterAPI)
    rasterAPI.enable()

    override def left: Expression = rasterExpr

    override def right: Expression = arg1Expr

    /** Output Data Type */
    override def dataType: DataType = outputType

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
    def rasterTransform(raster: MosaicRaster, arg1: Any): Any

    /**
      * Evaluation of the expression. It evaluates the raster path and the loads
      * the raster from the path. It handles the clean up of the raster before
      * returning the results.
      * @param input
      *   The input to the expression. It can be a path to a raster file or a
      *   byte array containing the raster file content.
      *
      * @param arg1
      *   The first argument.
      *
      * @return
      *   The result of the expression.
      */
    override def nullSafeEval(input: Any, arg1: Any): Any = {
        val raster = rasterAPI.readRaster(input, rasterExpr.dataType)
        val result = rasterTransform(raster, arg1)
        raster.cleanUp()
        result
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 2, expressionConfig)

    override def withNewChildrenInternal(
        newFirst: Expression,
        newArg1: Expression
    ): Expression = makeCopy(Array(newFirst, newArg1))

}
