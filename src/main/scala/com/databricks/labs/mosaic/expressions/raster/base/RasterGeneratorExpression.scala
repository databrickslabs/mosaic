package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.ClassTag

/**
  * Base class for all raster generator expressions that take no arguments. It
  * provides the boilerplate code needed to create a function builder for a
  * given expression. It minimises amount of code needed to create a new
  * expression. These expressions are used to generate a collection of new
  * rasters based on the input raster. The new rasters are written in the
  * checkpoint directory. The files are written as GeoTiffs. Subdatasets are not
  * supported, please flatten beforehand.
  * @param inPathExpr
  *   The expression for the raster path.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterGeneratorExpression[T <: Expression: ClassTag](
    inPathExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends CollectionGenerator
      with NullIntolerant
      with Serializable {

    val uuid: String = java.util.UUID.randomUUID().toString.replace("-", "_")

    /**
      * The raster API to be used. Enable the raster so that subclasses dont
      * need to worry about this.
      */
    protected val rasterAPI: RasterAPI = RasterAPI(expressionConfig.getRasterAPI)
    rasterAPI.enable()

    override def position: Boolean = false

    override def inline: Boolean = false

    /**
      * Generators expressions require an abstraction for element type. Always
      * needs to be wrapped in a StructType. The actually type is that of the
      * structs element.
      */
    override def elementSchema: StructType = StructType(Array(StructField("path", StringType)))

    /**
      * The function to be overriden by the extending class. It is called when
      * the expression is evaluated. It provides the raster band to the
      * expression. It abstracts spark serialization from the caller.
      * @param raster
      *   The raster to be used.
      * @return
      *   Sequence of subrasters = (id, reference to the input raster, extent of
      *   the output raster, unified mask for all bands).
      */
    def rasterGenerator(raster: MosaicRaster): Seq[(Long, (Int, Int, Int, Int))]

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        val inPath = inPathExpr.eval(input).asInstanceOf[UTF8String].toString
        val checkpointPath = expressionConfig.getRasterCheckpoint

        val raster = rasterAPI.raster(inPath)
        val result = rasterGenerator(raster)

        for ((id, extent) <- result) yield {
            val outPath = raster.saveCheckpoint(uuid, id, extent, checkpointPath)
            InternalRow.fromSeq(Seq(UTF8String.fromString(outPath)))
        }

    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        GenericExpressionFactory.makeCopyImpl[T](this, newArgs, children.length, expressionConfig)

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = makeCopy(newChildren.toArray)

}
