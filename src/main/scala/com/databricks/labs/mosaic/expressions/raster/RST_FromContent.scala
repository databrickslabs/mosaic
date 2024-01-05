package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.datasource.gdal.ReTileOnRead
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal, NullIntolerant}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.nio.file.{Files, Paths}

/**
  * The raster for construction of a raster tile. This should be the first
  * expression in the expression tree for a raster tile.
  */
case class RST_FromContent(
    rasterExpr: Expression,
    driverExpr: Expression,
    sizeInMB: Expression,
    parentPathExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends CollectionGenerator
      with Serializable
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = RasterTileType(expressionConfig.getCellIdType)

    protected val geometryAPI: GeometryAPI = GeometryAPI.apply(expressionConfig.getGeometryAPI)

    protected val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem)

    protected val cellIdDataType: DataType = indexSystem.getCellIdDataType

    override def position: Boolean = false

    override def inline: Boolean = false

    override def children: Seq[Expression] = Seq(rasterExpr, driverExpr, sizeInMB, parentPathExpr)

    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))

    /**
      * subdivides raster binary content into tiles of the specified size (in MB).
      * @param input
      *   The input file path.
      * @return
      *   The tiles.
      */
    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        GDAL.enable(expressionConfig)
        //parentPath may be null (it is not used here as content may be different)
        val parentPath = {
            try {
                parentPathExpr.eval(input).asInstanceOf[UTF8String].toString
            } catch {
                case _: Any => null
            }
        }
        val driver = driverExpr.eval(input).asInstanceOf[UTF8String].toString
        val ext = GDAL.getExtension(driver)
        var rasterArr = rasterExpr.eval(input).asInstanceOf[Array[Byte]]
        val targetSize = sizeInMB.eval(input).asInstanceOf[Int]
        if (targetSize <= 0 && rasterArr.length <= Integer.MAX_VALUE) {
            var raster = MosaicRasterGDAL.readRaster(rasterArr, parentPath, driver)
            var tile = MosaicRasterTile(null, raster, parentPath, driver)
            val row = tile.formatCellId(indexSystem).serialize()
            RasterCleaner.dispose(raster)
            RasterCleaner.dispose(tile)
            rasterArr = null
            raster = null
            tile = null
            Seq(InternalRow.fromSeq(Seq(row)))
        } else {
            // If target size is <0 and we are here that means the file is too big to fit in memory
            // - write the initial raster to file (unsplit)
            // - repeating the createDirectories for context isolation
            val rasterPath = PathUtils.createTmpFilePath(ext)
            Files.createDirectories(Paths.get(rasterPath).getParent)
            Files.write(Paths.get(rasterPath), rasterArr)

            // We split to tiles of size 64MB
            val size = if (targetSize <= 0) 64 else targetSize
            var tiles = ReTileOnRead.localSubdivide(rasterPath, parentPath, size)
            val rows = tiles.map(_.formatCellId(indexSystem).serialize())
            tiles.foreach(RasterCleaner.dispose(_))
            Files.deleteIfExists(Paths.get(rasterPath))
            rasterArr = null
            tiles = null
            rows.map(row => InternalRow.fromSeq(Seq(row)))
        }
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        GenericExpressionFactory.makeCopyImpl[RST_FromContent](this, newArgs, children.length, expressionConfig)

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = makeCopy(newChildren.toArray)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_FromContent extends WithExpressionInfo {

    override def name: String = "rst_fromcontent"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2, expr3, expr4) - Returns raster tiles from binary content within threshold in MBs.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster, driver, sizeInMB, parentPath);
          |        {index_id, raster, parentPath, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        (children: Seq[Expression]) => {
            val sizeExpr = if (children.length < 3) new Literal(-1, IntegerType) else children(2)
            val pathExpr = if (children.length < 4) new Literal(null, StringType) else children(3)
            RST_FromContent(children(0), children(1), sizeExpr, pathExpr, expressionConfig)
        }
    }

}
