package com.databricks.labs.mosaic.expressions.raster

import com.databricks.labs.mosaic.{NO_PATH_STRING, RASTER_DRIVER_KEY, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.index.{IndexSystem, IndexSystemFactory}
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO.{createTmpFileFromDriver, readRasterHydratedFromContent}
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.RasterTile
import com.databricks.labs.mosaic.datasource.gdal.SubdivideOnRead
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal, NullIntolerant}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.nio.file.{Files, Paths}

/**
  * The tile for construction of a tile tile. This should be the first
  * expression in the expression tree for a tile tile.
  */
case class RST_FromContent(
                              contentExpr: Expression,
                              driverExpr: Expression,
                              sizeInMB: Expression,
                              exprConfig: ExprConfig
) extends CollectionGenerator
      with Serializable
      with NullIntolerant
      with CodegenFallback {

    GDAL.enable(exprConfig)

    override def dataType: DataType = {
        RasterTileType(exprConfig.getCellIdType, BinaryType, exprConfig.isRasterUseCheckpoint)
    }

    protected val geometryAPI: GeometryAPI = GeometryAPI.apply(exprConfig.getGeometryAPI)

    protected val indexSystem: IndexSystem = IndexSystemFactory.getIndexSystem(exprConfig.getIndexSystem)

    protected val cellIdDataType: DataType = indexSystem.getCellIdDataType

    override def position: Boolean = false

    override def inline: Boolean = false

    override def children: Seq[Expression] = Seq(contentExpr, driverExpr, sizeInMB)

    override def elementSchema: StructType = StructType(Array(StructField("tile", dataType)))

    /**
      * subdivides tile binary content into tiles of the specified size (in
      * MB).
      * @param input
      *   The input file path.
      * @return
      *   The tiles.
      */
    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        GDAL.enable(exprConfig)
        val resultType = RasterTile.getRasterType(
            RasterTileType(exprConfig.getCellIdType, BinaryType, exprConfig.isRasterUseCheckpoint))

        val driverName = driverExpr.eval(input).asInstanceOf[UTF8String].toString
        var rasterArr = contentExpr.eval(input).asInstanceOf[Array[Byte]]
        val targetSize = sizeInMB.eval(input).asInstanceOf[Int]

        if (targetSize <= 0 || rasterArr.length <= targetSize) {
            // - no split required
            var raster = readRasterHydratedFromContent(
                rasterArr,
                Map(RASTER_DRIVER_KEY -> driverName),
                Option(exprConfig)
            )

            var result = RasterTile(null, raster, resultType).formatCellId(indexSystem)
            val row = result.serialize(resultType, doDestroy = true, Option(exprConfig))

            raster.flushAndDestroy()

            rasterArr = null
            raster = null
            result = null

            // do this for TraversableOnce[InternalRow]
            Seq(InternalRow.fromSeq(Seq(row)))
        } else {
            // target size is > 0 and tile size > target size
            // - write the initial tile to file (unsplit)
            // - createDirectories in case of context isolation
            val tmpPath = createTmpFileFromDriver(driverName, Option(exprConfig))
            Files.createDirectories(Paths.get(tmpPath).getParent)
            Files.write(Paths.get(tmpPath), rasterArr)

            // split to tiles up to specified threshold
            var results = SubdivideOnRead
                .localSubdivide(
                    Map(
                        RASTER_PATH_KEY -> tmpPath,
                        RASTER_DRIVER_KEY -> driverName
                    ),
                    targetSize,
                    Option(exprConfig)
                )
                .map(_.formatCellId(indexSystem))
            val rows = results.map(_.serialize(resultType, doDestroy = true, Option(exprConfig)))

            results.foreach(_.flushAndDestroy())

            rasterArr = null
            results = null

            // do this for TraversableOnce[InternalRow]
            rows.map(row => InternalRow.fromSeq(Seq(row)))
        }
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        GenericExpressionFactory.makeCopyImpl[RST_FromContent](this, newArgs, children.length, exprConfig)

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = makeCopy(newChildren.toArray)

}

/** Expression info required for the expression registration for spark SQL. */
object RST_FromContent extends WithExpressionInfo {

    override def name: String = "rst_fromcontent"

    override def usage: String =
        """
          |_FUNC_(expr1, expr2, expr3) - Returns tile tiles from binary content within threshold in MBs.
          |""".stripMargin

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(raster_bin, driver, size_in_mb);
          |        {index_id, tile, parent_path, driver}
          |        ...
          |  """.stripMargin

    override def builder(exprConfig: ExprConfig): FunctionBuilder = { (children: Seq[Expression]) =>
        {
            val sizeExpr = if (children.length < 3) new Literal(-1, IntegerType) else children(2)
            RST_FromContent(children.head, children(1), sizeExpr, exprConfig)
        }
    }

}
