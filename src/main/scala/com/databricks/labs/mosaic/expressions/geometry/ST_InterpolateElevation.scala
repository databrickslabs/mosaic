package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.geometry.multipoint.MosaicMultiPoint
import com.databricks.labs.mosaic.core.geometry.point.MosaicPoint
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import java.util.Locale

case class ST_InterpolateElevation(
    pointsArray: Expression,
    linesArray: Expression,
    mergeTolerance: Expression,
    snapTolerance: Expression,
    gridOrigin: Expression,
    gridWidthX: Expression,
    gridWidthY: Expression,
    gridSizeX: Expression,
    gridSizeY: Expression,
    expressionConfig: MosaicExpressionConfig
) extends CollectionGenerator with Serializable with CodegenFallback {
    override def position: Boolean = false

    override def inline: Boolean = false

    override def elementSchema: StructType = StructType(Seq(StructField("geom", firstElementType)))

    def firstElementType: DataType = pointsArray.dataType.asInstanceOf[ArrayType].elementType
    def secondElementType: DataType = linesArray.dataType.asInstanceOf[ArrayType].elementType

    def getGeometryAPI(expressionConfig: MosaicExpressionConfig): GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    def geometryAPI: GeometryAPI = getGeometryAPI(expressionConfig)

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
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
                            case _ => throw new UnsupportedOperationException("ST_InterpolateElevation requires Point geometry as masspoints input")
                        }
                })

        val multiPointGeom = geometryAPI.fromSeq(pointsGeom, MULTIPOINT).asInstanceOf[MosaicMultiPoint]
        val linesGeom =
            linesArray
                .eval(input)
                .asInstanceOf[ArrayData]
                .toObjectArray(firstElementType)
                .map({
                    obj =>
                        val g = geometryAPI.geometry(obj, firstElementType)
                        g.getGeometryType.toUpperCase(Locale.ROOT) match {
                            case "LINESTRING" => g.asInstanceOf[MosaicLineString]
                            case _ => throw new UnsupportedOperationException("ST_InterpolateElevation requires LineString geometry as breaklines input")
                        }
                })

        val origin = geometryAPI.geometry(gridOrigin.eval(input), gridOrigin.dataType).asInstanceOf[MosaicPoint]
        val gridWidthXValue = gridWidthX.eval(input).asInstanceOf[Int]
        val gridWidthYValue = gridWidthY.eval(input).asInstanceOf[Int]
        val gridSizeXValue = gridSizeX.eval(input).asInstanceOf[Double]
        val gridSizeYValue = gridSizeY.eval(input).asInstanceOf[Double]
        val mergeToleranceValue = mergeTolerance.eval(input).asInstanceOf[Double]
        val snapToleranceValue = snapTolerance.eval(input).asInstanceOf[Double]

        val gridPoints = multiPointGeom.pointGrid(origin, gridWidthXValue, gridWidthYValue, gridSizeXValue, gridSizeYValue)

        val interpolatedPoints = multiPointGeom
            .interpolateElevation(linesGeom, gridPoints, mergeToleranceValue, snapToleranceValue)
            .asSeq

        val serializedPoints = interpolatedPoints
            .map(geometryAPI.serialize(_, firstElementType))

        val outputRows = serializedPoints
            .map(g => InternalRow.fromSeq(Seq(g)))

        outputRows
    }

    override def children: Seq[Expression] = Seq(pointsArray, linesArray, mergeTolerance, snapTolerance, gridOrigin, gridWidthX, gridWidthY, gridSizeX, gridSizeY)

    override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
        copy(
            pointsArray = newChildren(0),
            linesArray = newChildren(1),
            mergeTolerance = newChildren(2),
            snapTolerance = newChildren(3),
            gridOrigin = newChildren(4),
            gridWidthX = newChildren(5),
            gridWidthY = newChildren(6),
            gridSizeX = newChildren(7),
            gridSizeY = newChildren(8)
        )
    }
}

object ST_InterpolateElevation extends WithExpressionInfo {

    override def name: String = "st_interpolateelevation"

    override def usage: String = {
        "_FUNC_(expr1, expr2, expr3, expr4, expr5, expr6, expr7, expr8, expr9) - Returns the interpolated heights " +
            "of the points in the grid defined by `expr5`, `expr6`, `expr7`, `expr8` and `expr9`" +
            "in the triangulated irregular network formed from the points in `expr1` " +
            "including `expr2` as breaklines with tolerance parameters `expr3` and `expr4`."
    }

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b, c, d, e, f, g, h, i);
          |        Point Z (...)
          |        Point Z (...)
          |        ...
          |        Point Z (...)
          |  """.stripMargin


    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_InterpolateElevation](9, expressionConfig)
    }

}