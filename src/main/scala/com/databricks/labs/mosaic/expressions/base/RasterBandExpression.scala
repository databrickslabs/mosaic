package com.databricks.labs.mosaic.expressions.base

import com.databricks.labs.mosaic.core.raster.{MosaicRaster, MosaicRasterBand}
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.ClassTag

abstract class RasterBandExpression[T <: Expression: ClassTag](
    rasterExpr: Expression,
    bandExpr: Expression,
    pathExpr: Expression,
    outputType: DataType,
    rasterAPI: RasterAPI
) extends TernaryExpression
      with NullIntolerant
      with Serializable {

    override def first: Expression = rasterExpr

    override def second: Expression = bandExpr

    override def third: Expression = pathExpr

    /** Output Data Type */
    override def dataType: DataType = outputType

    def bandTransform(raster: MosaicRaster, band: MosaicRasterBand): Any

    override def nullSafeEval(inputRaster: Any, inputBand: Any, inputPath: Any): Any = {
        val path = inputPath.asInstanceOf[UTF8String].toString
        val bandIndex = inputBand.asInstanceOf[Int]

        val baseRaster = rasterAPI.raster(inputRaster)
        val raster = rasterAPI.raster(inputRaster, path)

        val band = raster.getBand(bandIndex)

        val result = bandTransform(raster, band)

        raster.cleanUp()
        baseRaster.cleanUp()

        result
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression = GenericExpressionFactory.makeCopyImpl[T](this, newArgs, 3)

    override def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
        makeCopy(Array[AnyRef](newFirst, newSecond, newThird))

}
