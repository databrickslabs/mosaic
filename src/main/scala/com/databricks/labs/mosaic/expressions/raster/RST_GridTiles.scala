package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.Mosaic
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.expressions.raster.base.RasterGridExpression
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, NullIntolerant}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Returns a set of new rasters with the specified tile size (tileWidth x
  * tileHeight).
  */
case class RST_GridTiles(
    pathExpr: Expression,
    resolutionExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends CollectionGenerator
      with RasterGridExpression
      with NullIntolerant
      with CodegenFallback {

    /** The index system to be used. */
    val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem)
    val geometryAPI: GeometryAPI = GeometryAPI(expressionConfig.getGeometryAPI)

    /**
      * The raster API to be used. Enable the raster so that subclasses dont
      * need to worry about this.
      */
    protected val rasterAPI: RasterAPI = RasterAPI(expressionConfig.getRasterAPI)
    rasterAPI.enable()

    override def position: Boolean = false

    override def inline: Boolean = false

    /**
      * Generators expressions require an abstraction for element type. Always
      * needs to be wrapped in a StructType. The actually type is that of the
      * structs element.
      */
    override def elementSchema: StructType = StructType(Array(StructField("path", StringType)))

    /**
      * Returns a set of new rasters with the specified tile size (tileWidth x
      * tileHeight).
      */
    def rasterGenerator(raster: MosaicRaster, resolution: Int): Seq[MosaicRaster] = {
        val indexCRS = indexSystem.osrSpatialRef
        val bbox = raster.bbox(geometryAPI, indexCRS)

        val cells = Mosaic
            .mosaicFill(bbox, resolution, keepCoreGeom = false, indexSystem, geometryAPI)
            .map(_.indexAsLong(indexSystem))

        val rasters = cells.map(cellID => raster.getRasterForCell(cellID, indexSystem, geometryAPI))

        rasters
    }

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        val inPath = pathExpr.eval(input).asInstanceOf[UTF8String].toString
        val checkpointPath = expressionConfig.getRasterCheckpoint
        val resolution = resolutionExpr.eval(input).asInstanceOf[Int]

        val raster = rasterAPI.raster(inPath)
        val tiles = rasterGenerator(raster, resolution)
        val rasterStorage = expressionConfig.getRasterStorage

        rasterAPI.writeRasters(tiles, checkpointPath, rasterStorage)
    }

    override def children: Seq[Expression] = Seq(pathExpr, resolutionExpr)

    override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
        copy(pathExpr = newChildren(0), resolutionExpr = newChildren(1))
    }

}

/** Expression info required for the expression registration for spark SQL. */
object RST_GridTiles extends WithExpressionInfo {

    override def name: String = "rst_gridtiles"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a set of new rasters with the specified tile size (tileWidth x tileHeight).
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b);
          |        /path/to/raster_tile_1.tif
          |        /path/to/raster_tile_2.tif
          |        /path/to/raster_tile_3.tif
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[RST_GridTiles](2, expressionConfig)
    }

}
