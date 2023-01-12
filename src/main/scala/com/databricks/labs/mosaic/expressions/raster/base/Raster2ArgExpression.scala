package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.ClassTag

/**
  * Base class for all raster expressions that take two arguments. It provides
  * the boilerplate code needed to create a function builder for a given
  * expression. It minimises amount of code needed to create a new expression.
  * @param pathExpr
  *   The expression for the raster path.
  * @param arg1Expr
  *   The expression for the first argument.
  * @param arg2Expr
  *   The expression for the second argument.
  * @param outputType
  *   The output type of the result.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class Raster2ArgExpression[T <: Expression: ClassTag](
    pathExpr: Expression,
    arg1Expr: Expression,
    arg2Expr: Expression,
    outputType: DataType,
    expressionConfig: MosaicExpressionConfig
) extends TernaryExpression
      with NullIntolerant
      with Serializable {

    /**
      * The raster API to be used. Enable the raster so that subclasses dont
      * need to worry about this.
      */
    protected val rasterAPI: RasterAPI = RasterAPI(expressionConfig.getRasterAPI)
    rasterAPI.enable()

    override def first: Expression = pathExpr

    override def second: Expression = arg1Expr

    override def third: Expression = arg2Expr

    /** Output Data Type */
    override def dataType: DataType = outputType

    /**
      * The function to be overriden by the extending class. It is called when
      * the expression is evaluated. It provides the raster and the arguments to
      * the expression. It abstracts spark serialization from the caller.
      * @param raster
      *   The raster to be used.
      * @param arg1
      *   The first argument.
      * @param arg2
      *   The second argument.
      * @return
      *   A result of the expression.
      */
    def rasterTransform(raster: MosaicRaster, arg1: Any, arg2: Any): Any

    /**
      * Evaluation of the expression. It evaluates the raster path and the loads
      * the raster from the path. It handles the clean up of the raster before
      * returning the results.
      * @param inputPath
      *   The path to the raster. It is a UTF8String.
      *
      * @param arg1
      *   The first argument.
      * @param arg2
      *   The second argument.
      *
      * @return
      *   The result of the expression.
      */
    override def nullSafeEval(inputPath: Any, arg1: Any, arg2: Any): Any = {
        val path = inputPath.asInstanceOf[UTF8String].toString

        val raster = rasterAPI.raster(path)
        val result = rasterTransform(raster, arg1, arg2)

        raster.cleanUp()
        result
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 3, expressionConfig)

    override def withNewChildrenInternal(
        newFirst: Expression,
        newArg1: Expression,
        newArg2: Expression
    ): Expression = makeCopy(Array(newFirst, newArg1, newArg2))

}
