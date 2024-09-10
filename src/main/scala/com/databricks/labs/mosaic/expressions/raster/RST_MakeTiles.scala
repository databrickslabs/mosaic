package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.{NO_DRIVER, NO_PATH_STRING, RASTER_DRIVER_KEY, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO.{createTmpFileFromDriver, identifyDriverNameFromRawPath}
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.datasource.gdal.SubdivideOnRead
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.functions.ExprConfig
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
  * Creates tile tiles from the input column.
  * - spark config to turn checkpointing on for all functions in 0.4.3
  * - this is the only function able to write tile to
  *    checkpoint (even if the spark config is set to false).
  * - can be useful when you want to start from the configured checkpoint
  *   but work with binary payloads from there.
 *  - more at [[com.databricks.labs.mosaic.gdal.MosaicGDAL]].
  * @param inputExpr
  *   The expression for the tile. If the tile is stored on disc, the path
  *   to the tile is provided. If the tile is stored in memory, the bytes of
  *   the tile are provided.
  * @param sizeInMBExpr
  *   The size of the tiles in MB. If set to -1, the file is loaded and returned
  *   as a single tile. If set to 0, the file is loaded and subdivided into
  *   tiles of size 64MB. If set to a positive value, the file is loaded and
  *   subdivided into tiles of the specified size. If the file is too big to fit
  *   in memory, it is subdivided into tiles of size 64MB.
  * @param driverExpr
  *   The driver to use for reading the tile. If not specified, the driver is
  *   inferred from the file extension. If the input is a byte array, the driver
  *   has to be specified.
  * @param withCheckpointExpr
  *   If set to true, the tiles are written to the checkpoint directory. If set
  *   to false, the tiles are returned as a in-memory byte arrays.
  * @param exprConfig
  *   Additional arguments for the expression (expressionConfigs).
  */
case class RST_MakeTiles(
                            inputExpr: Expression,
                            driverExpr: Expression,
                            sizeInMBExpr: Expression,
                            withCheckpointExpr: Expression,
                            exprConfig: ExprConfig
) extends CollectionGenerator
      with Serializable
      with NullIntolerant
      with CodegenFallback {

    GDAL.enable(exprConfig)

    // serialize data type
    override def dataType: DataType = {
        require(withCheckpointExpr.isInstanceOf[Literal])
        if (withCheckpointExpr.eval().asInstanceOf[Boolean] || exprConfig.isRasterUseCheckpoint) {
            // Raster will be serialized as a path
            RasterTileType(exprConfig.getCellIdType, StringType, useCheckpoint = true)
        } else {
            // Raster will be serialized as a byte array
            RasterTileType(exprConfig.getCellIdType, BinaryType, useCheckpoint = false)
        }
    }

    protected val geometryAPI: GeometryAPI = GeometryAPI.apply(exprConfig.getGeometryAPI)

    protected val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(exprConfig.getIndexSystem)

    protected val cellIdDataType: DataType = indexSystem.getCellIdDataType

    override def position: Boolean = false

    override def inline: Boolean = false

    override def children: Seq[Expression] = Seq(inputExpr, driverExpr, sizeInMBExpr, withCheckpointExpr)

    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))

    private def getUriPartOpt(path: String): Option[String] = {
        val uriDeepCheck = Try(exprConfig.isUriDeepCheck).getOrElse(false)
        PathUtils.parseGdalUriOpt(path, uriDeepCheck)
    }

    private def getDriver(rawInput: Any, rawDriver: String): String = {
        if (rawDriver == NO_DRIVER) {
            if (inputExpr.dataType == StringType) {
                val path = rawInput.asInstanceOf[UTF8String].toString
                identifyDriverNameFromRawPath(path, getUriPartOpt(path))
            } else {
                throw new IllegalArgumentException("Driver has to be specified for byte array input")
            }
        } else {
            rawDriver
        }
    }

    private def getInputSize(rawInput: Any, uriDeepCheck: Boolean): Long = {
        if (inputExpr.dataType == StringType) {
            val path = rawInput.asInstanceOf[UTF8String].toString
            val uriGdalOpt = PathUtils.parseGdalUriOpt(path, uriDeepCheck)
            val fsPath = PathUtils.asFileSystemPath(path, uriGdalOpt)
            Files.size(Paths.get(fsPath))
        } else {
            val bytes = rawInput.asInstanceOf[Array[Byte]]
            bytes.length
        }
    }

    /**
      * Loads a tile from a file and subdivides it into tiles of the specified
      * size (in MB).
      * @param input
      *   The input file path or content.
      * @return
      *   The tiles.
      */
    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        GDAL.enable(exprConfig)
        val resultType = RasterTile.getRasterType(dataType)

        val rawDriver = driverExpr.eval(input).asInstanceOf[UTF8String].toString
        val rawInput = inputExpr.eval(input) // <- path or content
        val driverShortName = getDriver(rawInput, rawDriver)
        val targetSize = sizeInMBExpr.eval(input).asInstanceOf[Int]
        val inputSize = getInputSize(rawInput, uriDeepCheck = false) // <- this can become a config
        val path = if (inputExpr.dataType == StringType) rawInput.asInstanceOf[UTF8String].toString else NO_PATH_STRING

        val createInfo = Map(
            RASTER_PATH_KEY -> path,
            RASTER_DRIVER_KEY -> driverShortName
        )

        if (targetSize <= 0 && inputSize <= Integer.MAX_VALUE) {
            // - no split required

            var raster = GDAL.readRasterExpr(
                rawInput,
                createInfo,
                inputExpr.dataType,
                Option(exprConfig)
            )
            var result = RasterTile(null, raster, inputExpr.dataType).formatCellId(indexSystem)
            val row = result.serialize(resultType, doDestroy = true, Option(exprConfig))

            result.flushAndDestroy()
            raster = null
            result = null

            // do this for TraversableOnce[InternalRow]
            Seq(InternalRow.fromSeq(Seq(row)))
        } else {
            // target size is > 0 and tile size > target size
            // - write the initial tile to file (unsplit)
            // - createDirectories in case of context isolation
            val readPath =
                if (inputExpr.dataType == StringType) {
                    PathUtils.copyCleanPathToTmpWithRetry(path, Option(exprConfig), retries = 5)
                } else {
                    val tmpPath = createTmpFileFromDriver(driverShortName, Option(exprConfig))
                    Files.createDirectories(Paths.get(tmpPath).getParent)
                    Files.write(Paths.get(tmpPath), rawInput.asInstanceOf[Array[Byte]])
                    tmpPath
                }
            val size = if (targetSize <= 0) 64 else targetSize
            var results = SubdivideOnRead
                .localSubdivide(
                    createInfo + (RASTER_PATH_KEY -> readPath, RASTER_PARENT_PATH_KEY -> path),
                    size,
                    Option(exprConfig)
                )
                .map(_.formatCellId(indexSystem))
            val rows = results.map(_.serialize(resultType, doDestroy = true, Option(exprConfig)))

            results.foreach(_.flushAndDestroy())

            results = null

            // do this for TraversableOnce[InternalRow]
            rows.map(row => InternalRow.fromSeq(Seq(row)))
        }
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        GenericExpressionFactory.makeCopyImpl[RST_MakeTiles](this, newArgs, children.length, exprConfig)

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
          |        {index_id, tile, parent_path, driver}
          |        ...
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = { (children: Seq[Expression]) =>
        {
            def checkSize(size: Expression) = Try(size.eval().asInstanceOf[Int]).isSuccess
            def checkChkpnt(chkpnt: Expression) = Try(chkpnt.eval().asInstanceOf[Boolean]).isSuccess
            def checkDriver(driver: Expression) = Try(driver.eval().asInstanceOf[UTF8String].toString).isSuccess
            val noSize = new Literal(-1, IntegerType)
            val noDriver = new Literal(UTF8String.fromString(NO_DRIVER), StringType)
            val noCheckpoint = new Literal(false, BooleanType)

            children match {
                // Note type checking only works for literals
                case Seq(input)                                => RST_MakeTiles(input, noDriver, noSize, noCheckpoint, exprConfig)
                case Seq(input, driver) if checkDriver(driver) => RST_MakeTiles(input, driver, noSize, noCheckpoint, exprConfig)
                case Seq(input, size) if checkSize(size)       => RST_MakeTiles(input, noDriver, size, noCheckpoint, exprConfig)
                case Seq(input, checkpoint) if checkChkpnt(checkpoint)                                                         =>
                    RST_MakeTiles(input, noDriver, noSize, checkpoint, exprConfig)
                case Seq(input, size, checkpoint) if checkSize(size) && checkChkpnt(checkpoint)                                =>
                    RST_MakeTiles(input, noDriver, size, checkpoint, exprConfig)
                case Seq(input, driver, size) if checkDriver(driver) && checkSize(size)                                        =>
                    RST_MakeTiles(input, driver, size, noCheckpoint, exprConfig)
                case Seq(input, driver, checkpoint) if checkDriver(driver) && checkChkpnt(checkpoint)                          =>
                    RST_MakeTiles(input, driver, noSize, checkpoint, exprConfig)
                case Seq(input, driver, size, checkpoint) if checkDriver(driver) && checkSize(size) && checkChkpnt(checkpoint) =>
                    RST_MakeTiles(input, driver, size, checkpoint, exprConfig)
                case _ => RST_MakeTiles(children.head, children(1), children(2), children(3), exprConfig)
            }
        }
    }

}
