package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO.flushAndDestroy
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.ExprConfig
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
  * @param rasterExpr
  *   The expression for the raster. If the raster is stored on disc, the path
  *   to the raster is provided. If the raster is stored in memory, the bytes of
  *   the raster are provided.
  * @param exprConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterGeneratorExpression[T <: Expression: ClassTag](
                                                                       rasterExpr: Expression,
                                                                       exprConfig: ExprConfig
) extends CollectionGenerator
      with NullIntolerant
      with Serializable {

    GDAL.enable(exprConfig)

    override def dataType: DataType = {
        RasterTileType(exprConfig.getCellIdType, rasterExpr, useCheckpoint = true) // always checkpoint
    }

    val uuid: String = java.util.UUID.randomUUID().toString.replace("-", "_")

    protected val geometryAPI: GeometryAPI = GeometryAPI.apply(exprConfig.getGeometryAPI)

    protected val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(exprConfig.getIndexSystem)

    protected val cellIdDataType: DataType = indexSystem.getCellIdDataType

    override def position: Boolean = false

    override def inline: Boolean = false

    /**
      * Generators expressions require an abstraction for element type. Always
      * needs to be wrapped in a StructType. The actually type is that of the
      * structs element.
      */
    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the raster band to the
      * expression. It abstracts spark serialization from the caller.
      * @param raster
      *   The raster to be used.
      * @return
      *   Sequence of generated new rasters to be written.
      */
    def rasterGenerator(raster: RasterTile): Seq[RasterTile]

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        GDAL.enable(exprConfig)
        var tile = RasterTile.deserialize(
            rasterExpr.eval(input).asInstanceOf[InternalRow],
            cellIdDataType,
            Option(exprConfig)
        )
        var genTiles = rasterGenerator(tile).map(_.formatCellId(indexSystem))
        val resultType = RasterTile.getRasterType(dataType)
        val rows = genTiles.map(_.serialize(resultType, doDestroy = true, Option(exprConfig)))

        tile.flushAndDestroy()
        tile = null
        genTiles = null

        rows.map(row => InternalRow.fromSeq(Seq(row)))
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        GenericExpressionFactory.makeCopyImpl[T](this, newArgs, children.length, exprConfig)

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = makeCopy(newChildren.toArray)

}
