package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.operator.gdal.GDALCalc
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterArray1ArgExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

/** The expression for map algebra. */
case class RST_MapAlgebra(
    tileExpr: Expression,
    jsonSpecExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends RasterArray1ArgExpression[RST_MapAlgebra](
      tileExpr,
      jsonSpecExpr,
      returnsRaster = true,
      expressionConfig = expressionConfig
    )
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = {
        GDAL.enable(expressionConfig)
        RasterTileType(expressionConfig.getCellIdType, tileExpr, expressionConfig.isRasterUseCheckpoint)
    }

    /**
      * Map Algebra.
      * @param tiles
      *   The raster to be used.
      * @param arg1
      *   The red band index.
      * @return
      *   The raster (tile) from the calculation.
      */
    override def rasterTransform(tiles: Seq[MosaicRasterTile], arg1: Any): Any = {
        val jsonSpec = arg1.asInstanceOf[UTF8String].toString
        val extension = GDAL.getExtension(tiles.head.getDriver)
        val resultPath = PathUtils.createTmpFilePath(extension)
        val command = parseSpec(jsonSpec, resultPath, tiles)
        val index = if (tiles.map(_.getIndex).groupBy(identity).size == 1) tiles.head.getIndex else null
        val result = GDALCalc.executeCalc(command, resultPath)
        MosaicRasterTile(index, result)
    }

    def parseSpec(jsonSpec: String, resultPath: String, tiles: Seq[MosaicRasterTile]): String = {
        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

        val AZRasters = ('A' to 'Z').toList.map(l => s"${l}_index")
        val AZBands = ('A' to 'Z').toList.map(l => s"${l}_band")
        val json = parse(jsonSpec)

        val namedRasters = AZRasters
            .map(raster => (raster, (json \ raster).toOption))
            .filter(_._2.isDefined)
            .map(raster => (raster._1, raster._2.get.extract[Int]))
            .map { case (raster, index) => (raster, tiles(index).getRaster.getPath) }

        val paramRasters = (if (namedRasters.isEmpty) {
                                tiles.zipWithIndex.map { case (tile, index) => (s"${('A' + index).toChar}", tile.getRaster.getPath) }
                            } else {
                                namedRasters
                            })
            .map(raster => s" -${raster._1.split("_").head} ${raster._2}")
            .mkString

        val namedBands = AZBands
            .map(band => (band, (json \ band).toOption))
            .filter(_._2.isDefined)
            .map(band => (band._1, band._2.get.extract[Int]))
            .map(band => s" --${band._1}=${band._2}")
            .mkString

        val calc = (json \ "calc").toOption
            .map(_.extract[String])
            .getOrElse(
              throw new IllegalArgumentException("Calc parameter is required")
            )
        val extraOptions = (json \ "extra_options").toOption.map(_.extract[String]).getOrElse("")

        "gdal_calc" + paramRasters +
            namedBands +
            s" --outfile=$resultPath" +
            s" --calc=$calc" + s" $extraOptions"
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MapAlgebra extends WithExpressionInfo {

    override def name: String = "rst_mapalgebra"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2) - Performs map algebra on the raster tiles.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_tiles, "{calc: 'A+B', A_index: 0, B_index: 1}");
          |        {index_id, raster, parent_path, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_MapAlgebra](2, expressionConfig)
    }

}
