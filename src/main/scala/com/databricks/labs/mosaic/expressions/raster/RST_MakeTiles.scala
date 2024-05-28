package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.MOSAIC_NO_DRIVER
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

import java.nio.file.{Files, Paths}
import scala.util.Try

/**
  * Creates raster tiles from the input column.
  * - spark config to turn checkpointing on for all functions in 0.4.2
  * - this is the only function able to write raster to
  *    checkpoint (even if the spark config is set to false).
  * - can be useful when you want to start from the configured checkpoint
  *   but work with binary payloads from there.
 *  - more at [[com.databricks.labs.mosaic.gdal.MosaicGDAL]].
  * @param inputExpr
  *   The expression for the raster. If the raster is stored on disc, the path
  *   to the raster is provided. If the raster is stored in memory, the bytes of
  *   the raster are provided.
  * @param sizeInMBExpr
  *   The size of the tiles in MB. If set to -1, the file is loaded and returned
  *   as a single tile. If set to 0, the file is loaded and subdivided into
  *   tiles of size 64MB. If set to a positive value, the file is loaded and
  *   subdivided into tiles of the specified size. If the file is too big to fit
  *   in memory, it is subdivided into tiles of size 64MB.
  * @param driverExpr
  *   The driver to use for reading the raster. If not specified, the driver is
  *   inferred from the file extension. If the input is a byte array, the driver
  *   has to be specified.
  * @param withCheckpointExpr
  *   If set to true, the tiles are written to the checkpoint directory. If set
  *   to false, the tiles are returned as a in-memory byte arrays.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class RST_MakeTiles(
    inputExpr: Expression,
    driverExpr: Expression,
    sizeInMBExpr: Expression,
    withCheckpointExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends CollectionGenerator
      with Serializable
      with NullIntolerant
      with CodegenFallback {

    /** @return Returns StringType if either  */
    override def dataType: DataType = {
        GDAL.enable(expressionConfig)
        require(withCheckpointExpr.isInstanceOf[Literal])

        if (withCheckpointExpr.eval().asInstanceOf[Boolean] || expressionConfig.isRasterUseCheckpoint) {
            // Raster is referenced via a path
            RasterTileType(expressionConfig.getCellIdType, StringType, useCheckpoint = true)
        } else {
            // Raster is referenced via a byte array
            RasterTileType(expressionConfig.getCellIdType, BinaryType, useCheckpoint = false)
        }
    }

    protected val geometryAPI: GeometryAPI = GeometryAPI.apply(expressionConfig.getGeometryAPI)

    protected val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(expressionConfig.getIndexSystem)

    protected val cellIdDataType: DataType = indexSystem.getCellIdDataType

    override def position: Boolean = false

    override def inline: Boolean = false

    override def children: Seq[Expression] = Seq(inputExpr, driverExpr, sizeInMBExpr, withCheckpointExpr)

    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))

    private def getDriver(rawInput: Any, rawDriver: String): String = {
        if (rawDriver == MOSAIC_NO_DRIVER) {
            if (inputExpr.dataType == StringType) {
                val path = rawInput.asInstanceOf[UTF8String].toString
                MosaicRasterGDAL.identifyDriver(path)
            } else {
                throw new IllegalArgumentException("Driver has to be specified for byte array input")
            }
        } else {
            rawDriver
        }
    }

    private def getInputSize(rawInput: Any): Long = {
        if (inputExpr.dataType == StringType) {
            val path = rawInput.asInstanceOf[UTF8String].toString
            val cleanPath = PathUtils.replaceDBFSTokens(path)
            Files.size(Paths.get(cleanPath))
        } else {
            val bytes = rawInput.asInstanceOf[Array[Byte]]
            bytes.length
        }
    }

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

        val rawDriver = driverExpr.eval(input).asInstanceOf[UTF8String].toString
        val rawInput = inputExpr.eval(input)
        val driver = getDriver(rawInput, rawDriver)
        val targetSize = sizeInMBExpr.eval(input).asInstanceOf[Int]
        val inputSize = getInputSize(rawInput)
        val path = if (inputExpr.dataType == StringType) rawInput.asInstanceOf[UTF8String].toString else PathUtils.NO_PATH_STRING

        if (targetSize <= 0 && inputSize <= Integer.MAX_VALUE) {
            // - no split required
            val createInfo = Map("parentPath" -> PathUtils.NO_PATH_STRING, "driver" -> driver, "path" -> path)
            val raster = GDAL.readRaster(rawInput, createInfo, inputExpr.dataType)
            val tile = MosaicRasterTile(null, raster)
            val row = tile.formatCellId(indexSystem).serialize(rasterType)
            RasterCleaner.dispose(raster)
            RasterCleaner.dispose(tile)
            Seq(InternalRow.fromSeq(Seq(row)))
        } else {
            // target size is > 0 and raster size > target size
            // - write the initial raster to file (unsplit)
            // - createDirectories in case of context isolation
            val readPath =
                if (inputExpr.dataType == StringType) {
                    PathUtils.copyToTmpWithRetry(path, 5)
                } else {
                    val tmpPath = PathUtils.createTmpFilePath(GDAL.getExtension(driver))
                    Files.createDirectories(Paths.get(tmpPath).getParent)
                    Files.write(Paths.get(tmpPath), rawInput.asInstanceOf[Array[Byte]])
                    tmpPath
                }
            val size = if (targetSize <= 0) 64 else targetSize
            var tiles = ReTileOnRead.localSubdivide(readPath, PathUtils.NO_PATH_STRING, size)
            val rows = tiles.map(_.formatCellId(indexSystem).serialize(rasterType))
            tiles.foreach(RasterCleaner.dispose(_))
            Files.deleteIfExists(Paths.get(readPath))
            tiles = null
            rows.map(row => InternalRow.fromSeq(Seq(row)))
        }
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        GenericExpressionFactory.makeCopyImpl[RST_MakeTiles](this, newArgs, children.length, expressionConfig)

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = makeCopy(newChildren.toArray)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_MakeTiles extends WithExpressionInfo {

    override def name: String = "rst_maketiles"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a set of new rasters with the specified tile size (tileWidth x tileHeight).
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_path);
          |        {index_id, raster, parent_path, driver}
          |        ...
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = { (children: Seq[Expression]) =>
        {
            def checkSize(size: Expression) = Try(size.eval().asInstanceOf[Int]).isSuccess
            def checkChkpnt(chkpnt: Expression) = Try(chkpnt.eval().asInstanceOf[Boolean]).isSuccess
            def checkDriver(driver: Expression) = Try(driver.eval().asInstanceOf[UTF8String].toString).isSuccess
            val noSize = new Literal(-1, IntegerType)
            val noDriver = new Literal(UTF8String.fromString(MOSAIC_NO_DRIVER), StringType)
            val noCheckpoint = new Literal(false, BooleanType)

            children match {
                // Note type checking only works for literals
                case Seq(input)                                => RST_MakeTiles(input, noDriver, noSize, noCheckpoint, expressionConfig)
                case Seq(input, driver) if checkDriver(driver) => RST_MakeTiles(input, driver, noSize, noCheckpoint, expressionConfig)
                case Seq(input, size) if checkSize(size)       => RST_MakeTiles(input, noDriver, size, noCheckpoint, expressionConfig)
                case Seq(input, checkpoint) if checkChkpnt(checkpoint)                                                         =>
                    RST_MakeTiles(input, noDriver, noSize, checkpoint, expressionConfig)
                case Seq(input, size, checkpoint) if checkSize(size) && checkChkpnt(checkpoint)                                =>
                    RST_MakeTiles(input, noDriver, size, checkpoint, expressionConfig)
                case Seq(input, driver, size) if checkDriver(driver) && checkSize(size)                                        =>
                    RST_MakeTiles(input, driver, size, noCheckpoint, expressionConfig)
                case Seq(input, driver, checkpoint) if checkDriver(driver) && checkChkpnt(checkpoint)                          =>
                    RST_MakeTiles(input, driver, noSize, checkpoint, expressionConfig)
                case Seq(input, driver, size, checkpoint) if checkDriver(driver) && checkSize(size) && checkChkpnt(checkpoint) =>
                    RST_MakeTiles(input, driver, size, checkpoint, expressionConfig)
                case _ => RST_MakeTiles(children.head, children(1), children(2), children(3), expressionConfig)
            }
        }
    }

}
