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
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.nio.file.{Files, Paths, StandardCopyOption}

/**
  * The raster for construction of a raster tile. This should be the first
  * expression in the expression tree for a raster tile.
  */
case class RST_FromFile(
    rasterPathExpr: Expression,
    sizeInMB: Expression,
    expressionConfig: MosaicExpressionConfig
) extends CollectionGenerator
      with Serializable
      with NullIntolerant
      with CodegenFallback {

    override def dataType: DataType = {
        GDAL.enable(expressionConfig)
        RasterTileType(expressionConfig.getCellIdType, BinaryType, expressionConfig.isRasterUseCheckpoint)
    }

    protected val geometryAPI: GeometryAPI = GeometryAPI.apply(expressionConfig.getGeometryAPI)

    protected val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem)

    protected val cellIdDataType: DataType = indexSystem.getCellIdDataType

    override def position: Boolean = false

    override def inline: Boolean = false

    override def children: Seq[Expression] = Seq(rasterPathExpr, sizeInMB)

    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))

    /**
      * Loads a raster from a file and subdivides it into tiles of the specified
      * size (in MB).
      * @param input
      *   The input file path.
      * @return
      *   The tiles.
      */
    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        GDAL.enable(expressionConfig)
        val rasterType = dataType.asInstanceOf[RasterTileType].rasterType
        val path = rasterPathExpr.eval(input).asInstanceOf[UTF8String].toString
        val readPath = PathUtils.getCleanPath(path)
        val driver = MosaicRasterGDAL.identifyDriver(path)
        val targetSize = sizeInMB.eval(input).asInstanceOf[Int]
        val currentSize = Files.size(Paths.get(PathUtils.replaceDBFSTokens(readPath)))
        if (targetSize <= 0 && currentSize <= Integer.MAX_VALUE) {
            val createInfo = Map("path" -> readPath, "parentPath" -> path)
            var raster = MosaicRasterGDAL.readRaster(createInfo)
            var tile = MosaicRasterTile(null, raster)
            val row = tile.formatCellId(indexSystem).serialize(rasterType)
            RasterCleaner.dispose(raster)
            RasterCleaner.dispose(tile)
            raster = null
            tile = null
            Seq(InternalRow.fromSeq(Seq(row)))
        } else {
            // If target size is <0 and we are here that means the file is too big to fit in memory
            // We split to tiles of size 64MB
            val tmpPath = PathUtils.createTmpFilePath(GDAL.getExtension(driver))
            Files.copy(Paths.get(readPath), Paths.get(tmpPath), StandardCopyOption.REPLACE_EXISTING)
            val size = if (targetSize <= 0) 64 else targetSize
            var tiles = ReTileOnRead.localSubdivide(tmpPath, path, size)
            val rows = tiles.map(_.formatCellId(indexSystem).serialize(rasterType))
            tiles.foreach(RasterCleaner.dispose(_))
            Files.deleteIfExists(Paths.get(tmpPath))
            tiles = null
            rows.map(row => InternalRow.fromSeq(Seq(row)))
        }
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        GenericExpressionFactory.makeCopyImpl[RST_FromFile](this, newArgs, children.length, expressionConfig)

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = makeCopy(newChildren.toArray)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_FromFile extends WithExpressionInfo {

    override def name: String = "rst_fromfile"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a set of new raster tiles within threshold in MBs.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_path);
          |        {index_id, raster, parent_path, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        (children: Seq[Expression]) => {
            val sizeExpr = if (children.length == 1) new Literal(-1, IntegerType) else children(1)
            RST_FromFile(children.head, sizeExpr, expressionConfig)
        }
    }

}
