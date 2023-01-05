package com.databricks.labs.mosaic.expressions.base

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.ClassTag

abstract class RasterExpression[T <: Expression: ClassTag](
    rasterExpr: Expression,
    pathExpr: Expression,
    outputType: DataType,
    rasterAPI: RasterAPI
) extends BinaryExpression
      with NullIntolerant
      with Serializable {

    rasterAPI.enable()

    override def left: Expression = rasterExpr

    override def right: Expression = pathExpr

    /** Output Data Type */
    override def dataType: DataType = outputType

    def rasterTransform(raster: MosaicRaster): Any

    override def nullSafeEval(inputRaster: Any, inputPath: Any): Any = {
        val path = inputPath.asInstanceOf[UTF8String].toString

        val baseRaster = rasterAPI.raster(inputRaster)
        val raster = rasterAPI.raster(inputRaster, path)

        val result = rasterTransform(raster)

        raster.cleanUp()
        baseRaster.cleanUp()

        result
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 2, rasterAPI.name)

    override def withNewChildrenInternal(newFirst: Expression, newSecond: Expression): Expression = makeCopy(Array(newFirst, newSecond))

}
