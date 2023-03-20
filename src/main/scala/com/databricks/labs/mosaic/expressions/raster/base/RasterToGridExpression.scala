package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.{MosaicRaster, MosaicRasterBand}
import com.databricks.labs.mosaic.expressions.raster.RasterToGridType
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.InternalRow
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
  * @param pathExpr
  *   The expression for the raster path.
  * @param measureType
  *   The output type of the result.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterToGridExpression[T <: Expression: ClassTag, P](
    pathExpr: Expression,
    resolution: Expression,
    measureType: DataType,
    expressionConfig: MosaicExpressionConfig
) extends Raster1ArgExpression[T](pathExpr, resolution, RasterToGridType(expressionConfig.getCellIdType, measureType), expressionConfig)
      with NullIntolerant
      with Serializable {

    /** The index system to be used. */
    val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem)
    val geometryAPI: GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    /**
      * It projects the pixels to the grid and groups by the results so that the
      * result is a Sequence of (cellId, measure) of each band of the raster. It
      * applies the values combiner on the measures of each cell. For no
      * combine, use the identity function.
      * @param raster
      *   The raster to be used.
      * @return
      *   Sequence of (cellId, measure) of each band of the raster.
      */
    override def rasterTransform(raster: MosaicRaster, arg1: Any): Any = {
        val gt = raster.getRaster.GetGeoTransform()
        val resolution = arg1.asInstanceOf[Int]
        val bandTransform = (band: MosaicRasterBand) => {
            val results = band.transformValues[(Long, Double)](pixelTransformer(gt, resolution), (0L, -1.0))
            results
                // Filter out default cells. We don't want to return them since they are masked in original raster.
                // We use 0L as a dummy cell ID for default cells.
                .map(row => row.filter(_._1 != 0L))
                .filterNot(_.isEmpty)
                .flatten
                .groupBy(_._1) // Group by cell ID.
                .mapValues(values => valuesCombiner(values.map(_._2))) // Apply combiner that is overridden in subclasses.
        }
        val transformed = raster.transformBands(bandTransform)

        serialize(transformed)
    }

    /**
      * The method to be overriden to specify how the pixel values are combined
      * within a cell.
      * @param values
      *   The values to be combined.
      * @return
      *   The combined value/values.
      */
    def valuesCombiner(values: Seq[Double]): P

    private def pixelTransformer(gt: Seq[Double], resolution: Int)(x: Int, y: Int, value: Double): (Long, Double) = {
        val offset = 0.5 // This centers the point to the pixel centroid
        val xOffset = offset + x
        val yOffset = offset + y
        val xGeo = gt(0) + xOffset * gt(1) + yOffset * gt(2)
        val yGeo = gt(3) + xOffset * gt(4) + yOffset * gt(5)
        val cellID = indexSystem.pointToIndex(xGeo, yGeo, resolution)
        (cellID, value)
    }

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
