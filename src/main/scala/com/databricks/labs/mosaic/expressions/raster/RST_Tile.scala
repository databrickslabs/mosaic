package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.core.raster.gdal_raster.RasterCleaner
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.datasource.gdal.ReTileOnRead
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, NullIntolerant}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * Returns a set of new rasters with the specified tile size (tileWidth x
  * tileHeight).
  */
case class RST_Tile(
    rasterPathExpr: Expression,
    sizeInMB: Expression,
    expressionConfig: MosaicExpressionConfig
) extends CollectionGenerator
      with Serializable
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = RasterTileType(expressionConfig.getCellIdType)

    val uuid: String = java.util.UUID.randomUUID().toString.replace("-", "_")

    protected val rasterAPI: RasterAPI = RasterAPI(expressionConfig.getRasterAPI)
    rasterAPI.enable()
    protected val geometryAPI: GeometryAPI = GeometryAPI.apply(expressionConfig.getGeometryAPI)

    protected val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem)

    protected val cellIdDataType: DataType = indexSystem.getCellIdDataType

    override def position: Boolean = false

    override def inline: Boolean = false

    override def children: Seq[Expression] = Seq(rasterPathExpr, sizeInMB)

    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        val path = rasterPathExpr.eval(input).asInstanceOf[UTF8String].toString
        val targetSize = sizeInMB.eval(input).asInstanceOf[Int]
        val (raster, tiles) = ReTileOnRead.localSubdivide(path, targetSize)
        val rows = tiles.map(_.formatCellId(indexSystem).serialize(rasterAPI))
        tiles.foreach(RasterCleaner.dispose)
        RasterCleaner.dispose(raster)
        rows.map(row => InternalRow.fromSeq(Seq(row)))
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        GenericExpressionFactory.makeCopyImpl[RST_Tile](this, newArgs, children.length, expressionConfig)

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = makeCopy(newChildren.toArray)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_Tile extends WithExpressionInfo {

    override def name: String = "rst_tile"

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
        GenericExpressionFactory.getBaseBuilder[RST_ReTile](3, expressionConfig)
    }

}
