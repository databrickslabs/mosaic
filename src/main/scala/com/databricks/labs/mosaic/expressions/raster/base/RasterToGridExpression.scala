package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.raster.RasterToGridType
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

/**
  * Base class for all raster to grid expressions that take no arguments. It
  * provides the boilerplate code needed to create a function builder for a
  * given expression. It minimises amount of code needed to create a new
  * expression. These expressions project rasters to grid index system of
  * Mosaic. All cells are projected to spatial coordinates and then to grid
  * index system. The pixels are grouped by cell ids and then combined to form a
  * grid -> value/measure collection per band of the raster.
  * @param rasterExpr
  *   The raster expression. It can be a path to a raster file or a byte array
  *   containing the raster file content.
  * @param resolutionExpr
  *    The resolution of the index system to use.
  * @param measureType
  *   The output type of the result.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterToGridExpression[T <: Expression: ClassTag, P](
    rasterExpr: Expression,
    resolutionExpr: Expression,
    measureType: DataType,
    expressionConfig: MosaicExpressionConfig
) extends Raster1ArgExpression[T](rasterExpr, resolutionExpr, returnsRaster = false, expressionConfig)
      with RasterGridExpression
      with NullIntolerant
      with Serializable {

    override def dataType: DataType = RasterToGridType(expressionConfig.getCellIdType, measureType)

    /** The index system to be used. */
    val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem)
    val geometryAPI: GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    /**
      * It projects the pixels to the grid and groups by the results so that the
      * result is a Sequence of (cellId, measure) of each band of the raster. It
      * applies the values combiner on the measures of each cell. For no
      * combine, use the identity function.
      * @param tile
      *   The raster to be used.
      * @return
      *   Sequence of (cellId, measure) of each band of the raster.
      */
    override def rasterTransform(tile: MosaicRasterTile, arg1: Any): Any = {
        GDAL.enable(expressionConfig)
        val resolution = arg1.asInstanceOf[Int]
        val transformed = griddedPixels(tile.getRaster, indexSystem, resolution)
        val results = transformed.map(_.mapValues(valuesCombiner))
        RasterCleaner.dispose(tile)
        serialize(results)
    }

    /**
      * The method to be overridden to specify how the pixel values are combined
      * within a cell.
      * @param values
      *   The values to be combined.
      * @return
      *   The combined value/values.
      */
    def valuesCombiner(values: Seq[Double]): P

    /**
      * Serializes the result of the raster transform to the desired output
      * type.
      * @param cellsWithMeasure
      *   The result of the raster transform to be serialized to spark internal
      *   types.
      * @return
      *   The serialized result.
      */
    private def serialize(cellsWithMeasure: Traversable[Traversable[(Any, P)]]) = {
        val serialized = ArrayData.toArrayData(
          cellsWithMeasure.map(result =>
              ArrayData.toArrayData(
                result.map { case (cellID, value) => InternalRow.fromSeq(Seq(indexSystem.serializeCellId(cellID), value)) }
              )
          )
        )
        serialized
    }

}
