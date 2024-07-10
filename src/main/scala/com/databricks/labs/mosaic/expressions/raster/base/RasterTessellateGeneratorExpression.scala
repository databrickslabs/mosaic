package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, NullIntolerant}
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

/**
  * Base class for all tile generator expressions that take no arguments. It
  * provides the boilerplate code needed to create a function builder for a
  * given expression. It minimises amount of code needed to create a new
  * expression. These expressions are used to generate a collection of new
  * rasters based on the input tile. The new rasters are written in the
  * checkpoint directory. The files are written as GeoTiffs. Subdatasets are not
  * supported, please flatten beforehand.
  *
  * @param rasterExpr
  *   The expression for the tile. If the tile is stored on disc, the path
  *   to the tile is provided. If the tile is stored in memory, the bytes of
  *   the tile are provided.
  * @param resolutionExpr
  *   The resolution of the index system to use for tessellation.
  * @param exprConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterTessellateGeneratorExpression[T <: Expression: ClassTag](
                                                                                 rasterExpr: Expression,
                                                                                 resolutionExpr: Expression,
                                                                                 exprConfig: ExprConfig
) extends CollectionGenerator
      with NullIntolerant
      with Serializable {

    val uuid: String = java.util.UUID.randomUUID().toString.replace("-", "_")

    val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(exprConfig.getIndexSystem)

    protected val geometryAPI: GeometryAPI = GeometryAPI.apply(exprConfig.getGeometryAPI)

    override def position: Boolean = false

    override def inline: Boolean = false

    /**
      * Generators expressions require an abstraction for element type. Always
      * needs to be wrapped in a StructType. The actually type is that of the
      * structs element.
      * - we want to use checkpointing always for tessellate generator.
      */
    override def elementSchema: StructType = {
        StructType(
            Array(StructField(
                "element",
                RasterTileType(exprConfig.getCellIdType, rasterExpr, useCheckpoint = true)) // always use checkpoint
            )
        )
    }

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the tile band to the
      * expression. It abstracts spark serialization from the caller.
      * - always uses checkpoint dir.
      * @param raster
      *   The tile to be used.
      * @return
      *   Sequence of generated new rasters to be written.
      */
    def rasterGenerator(raster: RasterTile, resolution: Int): Seq[RasterTile]

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        GDAL.enable(exprConfig)
        var tile = RasterTile.deserialize(
            rasterExpr.eval(input).asInstanceOf[InternalRow],
            indexSystem.getCellIdDataType,
            Option(exprConfig)
        )
        val inResolution: Int = indexSystem.getResolution(resolutionExpr.eval(input))
        var genTiles = rasterGenerator(tile, inResolution).map(_.formatCellId(indexSystem))
        val resultType = RasterTile.getRasterType(RasterTileType(rasterExpr, useCheckpoint = true)) // always use checkpoint
        val rows = genTiles.map(t => InternalRow.fromSeq(Seq(t.formatCellId(indexSystem)
            .serialize(resultType, doDestroy = true, Option(exprConfig)))))

        tile.flushAndDestroy()
        tile = null
        genTiles = null

        rows.iterator
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        GenericExpressionFactory.makeCopyImpl[T](this, newArgs, children.length, exprConfig)

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = makeCopy(newChildren.toArray)

}
