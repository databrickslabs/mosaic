package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.{RASTER_DRIVER_KEY, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO.{createTmpFileFromDriver, identifyDriverNameFromRawPath}
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.datasource.gdal.ReTileOnRead
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.utils.PathUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal, NullIntolerant}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.nio.file.{Files, Paths, StandardCopyOption}

/**
  * The tile for construction of a tile tile. This should be the first
  * expression in the expression tree for a tile tile.
  */
case class RST_FromFile(
                           rasterPathExpr: Expression,
                           sizeInMB: Expression,
                           exprConfig: ExprConfig
) extends CollectionGenerator
      with Serializable
      with NullIntolerant
      with CodegenFallback {

    GDAL.enable(exprConfig)

    protected val geometryAPI: GeometryAPI = GeometryAPI.apply(exprConfig.getGeometryAPI)

    protected val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(exprConfig.getIndexSystem)

    protected val cellIdDataType: DataType = indexSystem.getCellIdDataType

    override def dataType: DataType = {
        RasterTileType(cellIdDataType, BinaryType, exprConfig.isRasterUseCheckpoint)
    }

    override def position: Boolean = false

    override def inline: Boolean = false

    override def children: Seq[Expression] = Seq(rasterPathExpr, sizeInMB)

    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))

    /**
      * Loads a tile from a file and subdivides it into tiles of the specified
      * size (in MB).
      * @param input
      *   The input file path.
      * @return
      *   The tiles.
      */
    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        GDAL.enable(exprConfig)
        val resultType = RasterTile.getRasterType(dataType)
        val toFuse = resultType == StringType
        val path = rasterPathExpr.eval(input).asInstanceOf[UTF8String].toString
        val uriGdalOpt = PathUtils.parseGdalUriOpt(path, uriDeepCheck = exprConfig.isUriDeepCheck)
        val fsPath = PathUtils.asFileSystemPath(path, uriGdalOpt) // removes fuse tokens
        val driverShortName = identifyDriverNameFromRawPath(path, uriGdalOpt)
        val targetSize = sizeInMB.eval(input).asInstanceOf[Int]
        val currentSize = Files.size(Paths.get(fsPath))

        val createInfo = Map(
            RASTER_PATH_KEY -> path,
            RASTER_DRIVER_KEY -> driverShortName
        )

        if (targetSize <= 0 && currentSize <= Integer.MAX_VALUE) {
            // since this will be serialized want it initialized
            var raster = RasterGDAL(createInfo, Option(exprConfig))
            raster.finalizeRaster(toFuse) // <- this will also destroy
            var result = RasterTile(null, raster, resultType).formatCellId(indexSystem)
            val row = result.serialize(resultType, doDestroy = true, Option(exprConfig))

            raster = null
            result = null

            // do this for TraversableOnce[InternalRow]
            Seq(InternalRow.fromSeq(Seq(row)))
        } else {
            // If target size is <0 and we are here that means the file is too big to fit in memory
            // We split to tiles of size 64MB
            val tmpPath = createTmpFileFromDriver(driverShortName, Option(exprConfig))
            Files.copy(Paths.get(fsPath), Paths.get(tmpPath), StandardCopyOption.REPLACE_EXISTING)
            val size = if (targetSize <= 0) 64 else targetSize

            var results = ReTileOnRead.localSubdivide(
                createInfo + (RASTER_PATH_KEY -> tmpPath),
                size,
                Option(exprConfig)
            ).map(_.formatCellId(indexSystem))
            val rows = results.map(_.finalizeTile(toFuse).serialize(resultType, doDestroy = true, Option(exprConfig)))

            results = null

            // do this for TraversableOnce[InternalRow]
            rows.map(row => InternalRow.fromSeq(Seq(row)))
        }
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        GenericExpressionFactory.makeCopyImpl[RST_FromFile](this, newArgs, children.length, exprConfig)

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = makeCopy(newChildren.toArray)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_FromFile extends WithExpressionInfo {

    override def name: String = "rst_fromfile"

    override def usage: String =
        """
          |_FUNC_(expr1) - Returns a set of new tile tiles within threshold in MBs.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_path);
          |        {index_id, tile, parent_path, driver}
          |        ...
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = {
        (children: Seq[Expression]) => {
            val sizeExpr = if (children.length == 1) new Literal(-1, IntegerType) else children(1)
            RST_FromFile(children.head, sizeExpr, exprConfig)
        }
    }

}
