package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, NullIntolerant}
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

/**
  * Base class for all raster generator expressions that take no arguments. It
  * provides the boilerplate code needed to create a function builder for a
  * given expression. It minimises amount of code needed to create a new
  * expression. These expressions are used to generate a collection of new
  * rasters based on the input raster. The new rasters are written in the
  * checkpoint directory. The files are written as GeoTiffs. Subdatasets are not
  * supported, please flatten beforehand.
  *
  * @param tileExpr
  *   The expression for the raster. If the raster is stored on disc, the path
  *   to the raster is provided. If the raster is stored in memory, the bytes of
  *   the raster are provided.
  * @param resolutionExpr
  *   The resolution of the index system to use for tessellation.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterTessellateGeneratorExpression[T <: Expression: ClassTag](
    tileExpr: Expression,
    resolutionExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends CollectionGenerator
      with NullIntolerant
      with Serializable {

    val uuid: String = java.util.UUID.randomUUID().toString.replace("-", "_")

    val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem)

    protected val geometryAPI: GeometryAPI = GeometryAPI.apply(expressionConfig.getGeometryAPI)

    override def position: Boolean = false

    override def inline: Boolean = false

    /**
      * Generators expressions require an abstraction for element type. Always
      * needs to be wrapped in a StructType. The actually type is that of the
      * structs element.
      */
    override def elementSchema: StructType = {
        StructType(
            Array(StructField(
                "element",
                RasterTileType(expressionConfig.getCellIdType, tileExpr, expressionConfig.isRasterUseCheckpoint))
            )
        )
    }

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the raster band to the
      * expression. It abstracts spark serialization from the caller.
      * @param raster
      *   The raster to be used.
      * @return
      *   Sequence of generated new rasters to be written.
      */
    def rasterGenerator(raster: MosaicRasterTile, resolution: Int): Seq[MosaicRasterTile]

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        GDAL.enable(expressionConfig)
        val rasterType = RasterTileType(tileExpr, expressionConfig.isRasterUseCheckpoint).rasterType
        val tile = MosaicRasterTile
            .deserialize(tileExpr.eval(input).asInstanceOf[InternalRow], indexSystem.getCellIdDataType, rasterType)
        val inResolution: Int = indexSystem.getResolution(resolutionExpr.eval(input))
        val generatedChips = rasterGenerator(tile, inResolution)
            .map(chip => chip.formatCellId(indexSystem))

        val rows = generatedChips
            .map(chip => InternalRow.fromSeq(Seq(chip.formatCellId(indexSystem).serialize(rasterType))))

        RasterCleaner.dispose(tile)
        generatedChips.foreach(chip => RasterCleaner.dispose(chip))
        generatedChips.foreach(chip => RasterCleaner.dispose(chip.getRaster))

        rows.iterator
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        GenericExpressionFactory.makeCopyImpl[T](this, newArgs, children.length, expressionConfig)

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = makeCopy(newChildren.toArray)

}
