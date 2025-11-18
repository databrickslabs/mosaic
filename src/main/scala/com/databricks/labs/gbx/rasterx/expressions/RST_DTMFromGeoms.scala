package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.{GDALRasterize, InterpolateElevation}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.LineString

case class RST_DTMFromGeoms(
    pointsArray: Expression,
    linesArray: Expression,
    mergeTolerance: Expression,
    snapTolerance: Expression,
    splitPointFinder: Expression,
    gridOrigin: Expression,
    gridWidthX: Expression,
    gridWidthY: Expression,
    gridSizeX: Expression,
    gridSizeY: Expression,
    noData: Expression
) extends InvokedExpression {

    def firstElementType: DataType = pointsArray.dataType.asInstanceOf[ArrayType].elementType
    def secondElementType: DataType = linesArray.dataType.asInstanceOf[ArrayType].elementType

    override def children: Seq[Expression] =
        Seq(
          pointsArray,
          linesArray,
          mergeTolerance,
          snapTolerance,
          splitPointFinder,
          gridOrigin,
          gridWidthX,
          gridWidthY,
          gridSizeX,
          gridSizeY,
          noData,
          ExpressionConfigExpr()
        )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_DTMFromGeoms.name
    override def replacement: Expression = invoke(RST_DTMFromGeoms)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10))

}

object RST_DTMFromGeoms extends WithExpressionInfo {

    def eval(
        pointsArray: ArrayData,
        linesArray: ArrayData,
        mergeTolerance: Double,
        snapTolerance: Double,
        splitPointFinder: UTF8String,
        gridOrigin: Any,
        gridWindow: (Int, Int, Double, Double),
        noData: Double,
        conf: UTF8String,
        dts: (DataType, DataType, DataType)
    ): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (pdt, ldt, odt) = dts
              val (gridWidthX, gridWidthY, gridSizeX, gridSizeY) = gridWindow
              val geomPoints = JTS.fromArrayData(pointsArray, pdt)
              val geomLines = JTS.fromArrayData(linesArray, ldt).map(_.asInstanceOf[LineString])
              val multiPointGeom = JTS.multiPoint(geomPoints)
              val origin = (odt match {
                  case StringType => JTS.fromWKT(gridOrigin.asInstanceOf[UTF8String].toString)
                  case BinaryType => JTS.fromWKB(gridOrigin.asInstanceOf[Array[Byte]])
              }).getCentroid

              val gridPoints = InterpolateElevation.pointGrid(origin, gridWidthX, gridWidthY, gridSizeX, gridSizeY)
              val interpolatedPoints = InterpolateElevation
                  .interpolate(multiPointGeom, geomLines, gridPoints, mergeTolerance, snapTolerance)

              val outputRaster = GDALRasterize.executeRasterize(
                interpolatedPoints,
                None,
                origin,
                gridWidthX,
                gridWidthY,
                gridSizeX,
                gridSizeY,
                noData,
                Map.empty
              )

              val res = RasterSerializationUtil.tileToRow((0L, outputRaster._1, outputRaster._2), BinaryType, exprConf.hConf)
              RasterDriver.releaseDataset(outputRaster._1)
              res
          },
          pointsArray, // TODO: this will need fixing
          StringType
        )

    override def name: String = "gbx_rst_dtmfromgeoms"

    override def builder(): FunctionBuilder =
        (c: Seq[Expression]) => new RST_DTMFromGeoms(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10))

    //TODO: ADD EXPRESSION INFO
    override def usageArgs: String = ""

    override def description: String = ""

    override def extendedUsageArgs: String = ""

    override def examples: String = ""
}
