package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.operator.TranslateToGTiff
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.expressions.base.WithExpressionInfo
import com.databricks.labs.mosaic.expressions.raster.base.Raster1ArgExpression
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, NullIntolerant}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

/**
 *   Converts the provided raster tile to tif if possible.
 *   - Writes raster tiles from the input column to a specified directory.
 *   - expects the driver to already have been set on the inputExpr ("tile"
 *     column).
 *
 * @param inputExpr
 *   The expression for the tile with the raster to write.
 * @param dirExpr
 *   Write to directory.
 * @param exprConfig
 *   Additional arguments for the expression (expressionConfigs).
 */
case class RST_ToTif(
                        inputExpr: Expression,
                        dirExpr: Expression,
                        exprConfig: ExprConfig
                    ) extends Raster1ArgExpression[RST_ToTif](
    inputExpr,
    dirExpr,
    returnsRaster = true,
    exprConfig = exprConfig
)
    with NullIntolerant
    with CodegenFallback {

    // serialize data type
    // - don't use checkpoint because we are writing to a different location
    // - type is StringType
    override def dataType: DataType = {
        RasterTileType(exprConfig.getCellIdType, StringType, useCheckpoint = false)
    }

    /**
     * write a tile to dir.
     *
     * @param tile
     *   The tile to be used.
     * @param arg1
     *   The dir.
     * @return
     *   tile using the new path
     */
    override def rasterTransform(tile: RasterTile, arg1: Any): Any = {
        // [1] convert current raster to tif
        val outRaster = TranslateToGTiff.compute(tile.raster, Some(exprConfig))

        // [2] generate a tile copy with the copied raster
        tile.copy(
            raster = copyToArg1Dir(outRaster, arg1)
        )
    }

    private def copyToArg1Dir(raster: RasterGDAL, arg1: Any): RasterGDAL = {
        require(dirExpr.isInstanceOf[Literal])

        // (1) new [[RasterGDAL]]
        // - from createInfo of existing
        val result = RasterGDAL(
            createInfoInit = raster.getCreateInfo(includeExtras = true),
            exprConfigOpt = Option(exprConfig)
        )
        // (2) just update the FuseDirOpt
        // - actual write will be during serialize
        // - aka `raster.finalizeAndDestroy`
        val toDir = arg1.asInstanceOf[UTF8String].toString
        result.setFuseDirOpt(Some(toDir))

        result
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_ToTif extends WithExpressionInfo {

    override def name: String = "rst_totif"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a new tile of type "tif" written to the specified directory.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tile, fuse_dir);
          |        {index_id, tile, parent_path, driver}
          |        ...
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = { (children: Seq[Expression]) =>
    {
        def checkDir(dir: Expression) = Try(dir.eval().asInstanceOf[String]).isSuccess

        children match {
            // Note type checking only works for literals
            case Seq(input, dir) if checkDir(dir) => RST_ToTif(input, dir, exprConfig)
            case _                                => RST_ToTif(children.head, children(1), exprConfig)
        }
    }
    }

}

