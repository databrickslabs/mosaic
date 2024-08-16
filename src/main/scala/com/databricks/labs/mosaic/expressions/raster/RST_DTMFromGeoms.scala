package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.multipoint.MosaicMultiPoint
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.rasterize.GDALRasterize
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.MULTIPOINT
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterExpressionSerialization
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale

case class RST_DTMFromGeoms(
                          pointsArray: Expression,
                          linesArray: Expression,
                          tolerance: Expression,
                          gridOrigin: Expression,
                          gridWidthX: Expression,
                          gridWidthY: Expression,
                          gridSizeX: Expression,
                          gridSizeY: Expression,
                          expressionConfig: MosaicExpressionConfig
                      ) extends Expression with Serializable with RasterExpressionSerialization with CodegenFallback
{
    GDAL.enable(expressionConfig)

    override def nullable: Boolean = false

    def firstElementType: DataType = pointsArray.dataType.asInstanceOf[ArrayType].elementType
    def secondElementType: DataType = linesArray.dataType.asInstanceOf[ArrayType].elementType

    def getGeometryAPI(expressionConfig: MosaicExpressionConfig): GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    def geometryAPI: GeometryAPI = getGeometryAPI(expressionConfig)

    override def eval(input: InternalRow): Any = {
        val pointsGeom =
            pointsArray
                .eval(input)
                .asInstanceOf[ArrayData]
                .toObjectArray(firstElementType)
                .map({
                    obj =>
                        val g = geometryAPI.geometry(obj, firstElementType)
                        g.getGeometryType.toUpperCase(Locale.ROOT) match {
                            case "POINT" => g.asInstanceOf[MosaicPoint]
                            case _ => throw new UnsupportedOperationException("RST_DTMFromGeoms requires Point geometry as masspoints input")
                        }
                })

        val multiPointGeom = geometryAPI.fromSeq(pointsGeom, MULTIPOINT).asInstanceOf[MosaicMultiPoint]
        val linesArrayData = linesArray
            .eval(input)
            .asInstanceOf[ArrayData]


        val linesGeom = if (linesArrayData == null) {
            Array(geometryAPI.geometry(UTF8String.fromString("LINESTRING EMPTY"), StringType).asInstanceOf[MosaicLineString])
        } else {
            linesArrayData
                .toObjectArray(secondElementType)
                .map({
                    obj =>
                        val g = geometryAPI.geometry(obj, secondElementType)
                        g.getGeometryType.toUpperCase(Locale.ROOT) match {
                            case "LINESTRING" => g.asInstanceOf[MosaicLineString]
                            case _ => throw new UnsupportedOperationException("RST_DTMFromGeoms requires LineString geometry as breaklines input")
                        }
                })
        }

        val origin = geometryAPI.geometry(gridOrigin.eval(input), gridOrigin.dataType).asInstanceOf[MosaicPoint]
        val gridWidthXValue = gridWidthX.eval(input).asInstanceOf[Int]
        val gridWidthYValue = gridWidthY.eval(input).asInstanceOf[Int]
        val gridSizeXValue = gridSizeX.eval(input).asInstanceOf[Double]
        val gridSizeYValue = gridSizeY.eval(input).asInstanceOf[Double]
        val toleranceValue = tolerance.eval(input).asInstanceOf[Double]

        val gridPoints = multiPointGeom.pointGrid(origin, gridWidthXValue, gridWidthYValue, gridSizeXValue, gridSizeYValue)

        val interpolatedPoints = multiPointGeom
            .interpolateElevation(linesGeom, gridPoints, toleranceValue)
            .asSeq

        val outputRaster = GDALRasterize.executeRasterize(
          interpolatedPoints, None, origin, gridWidthXValue, gridWidthYValue, gridSizeXValue, gridSizeYValue
        )

        val outputRow = MosaicRasterTile(null, outputRaster).serialize(StringType)
        outputRow
    }

    override def dataType: DataType = RasterTileType(
        expressionConfig.getCellIdType, StringType, expressionConfig.isRasterUseCheckpoint)

    override def children: Seq[Expression] = Seq(pointsArray, linesArray, tolerance, gridOrigin, gridWidthX, gridWidthY, gridSizeX, gridSizeY)

    override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
        copy(
            pointsArray = newChildren(0),
            linesArray = newChildren(1),
            tolerance = newChildren(2),
            gridOrigin = newChildren(3),
            gridWidthX = newChildren(4),
            gridWidthY = newChildren(5),
            gridSizeX = newChildren(6),
            gridSizeY = newChildren(7)
        )
    }

    override def canEqual(that: Any): Boolean = false
}

object RST_DTMFromGeoms extends WithExpressionInfo {

    override def name: String = "rst_dtmfromgeoms"

    override def usage: String = {
        "_FUNC_(expr1, expr2, expr3, expr4, expr5, expr6, expr7, expr8) - Returns the interpolated heights " +
            "of the points in the grid defined by `expr4`, `expr5`, `expr6`, `expr7` and `expr8`" +
            "in the triangulated irregular network formed from the points in `expr1` " +
            "including `expr2` as breaklines with tolerance `expr3` as a raster in GeoTIFF format."
    }

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b, c, d, e, f, g, h);
          |        {index_id, raster_tile, parentPath, driver}
          |  """.stripMargin


    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_DTMFromGeoms](8, expressionConfig)
    }

}