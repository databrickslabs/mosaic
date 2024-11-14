package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.WithExpressionInfo
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, NullIntolerant}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

/**
  * Writes raster tiles from the input column to a specified directory.
  *   - expects the driver to already have been set on the inputExpr ("tile"
  *     column).
  * @param inputExpr
  *   The expression for the raster. If the raster is stored on disc, the path
  *   to the raster is provided. If the raster is stored in memory, the bytes of
  *   the raster are provided.
  * @param dirExpr
  *   Write to directory.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class RST_Write(
    inputExpr: Expression,
    dirExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends Raster1ArgExpression[RST_Write](
      inputExpr,
      dirExpr,
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    // serialize data type
    override def dataType: DataType = {
        require(dirExpr.isInstanceOf[Literal])
        RasterTileType(expressionConfig.getCellIdType, inputExpr, expressionConfig.isRasterUseCheckpoint)
    }

    /**
      * write a raster to dir.
      *
      * @param tile
      *   The raster to be used.
      * @param arg1
      *   The dir.
      * @return
      *   tile using the new path
      */
    override def rasterTransform(tile: MosaicRasterTile, arg1: Any): Any = {
        tile.copy(
          raster = copyToArg1Dir(tile, arg1)
        )
    }

    private def copyToArg1Dir(inTile: MosaicRasterTile, arg1: Any): MosaicRasterGDAL = {
        val inRaster = inTile.getRaster
        val inPath = inRaster.createInfo("path")
        val inDriver = inRaster.createInfo("driver")
        val outPath = GDAL.writeRasterString(
                inRaster,
                Some(arg1.asInstanceOf[UTF8String].toString)
            )
            .toString

        MosaicRasterGDAL.readRaster(
          Map("path" -> outPath, "driver" -> inDriver, "parentPath" -> inPath)
        )
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Write extends WithExpressionInfo {

    override def name: String = "rst_write"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a new raster written to the specified directory.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, fuse_dir);
          |        {index_id, raster, parent_path, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (children: Seq[Expression]) =>
        {
            def checkDir(dir: Expression) = Try(dir.eval().asInstanceOf[String]).isSuccess

            children match {
                // Note type checking only works for literals
                case Seq(input, dir) if checkDir(dir) => RST_Write(input, dir, expressionConfig)
                case _                                => RST_Write(children.head, children(1), expressionConfig)
            }
        }
    }

}
