package com.databricks.labs.mosaic.core.types.model

import com.databricks.labs.mosaic.{NO_PATH_STRING, RASTER_PARENT_PATH_KEY, RASTER_PATH_KEY}
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.expressions.raster.{buildMapString, extractMap}
import com.databricks.labs.mosaic.functions.ExprConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, DataType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.{Failure, Success, Try}

/**
  * A case class modeling an instance of a mosaic tile tile.
  *
  * @param index
  *   Index ID.
  * @param raster
  *   Raster instance corresponding to the tile.
  * @param rasterType
  *   Preserve the type of the tile payload from deserialization,
  *   will be [[StringType]] or [[BinaryType]].
  */
case class RasterTile(
    index: Either[Long, String],
    raster: RasterGDAL,
    rasterType: DataType
) {

    /**
      * Indicates whether the tile is present.
      *
      * @return
      *   True if the tile is present, false otherwise.
      */
    def isEmpty: Boolean = Option(raster).forall(_.isEmpty)

    /**
     * Finalize the tile.
     * - essentially calls `tile.finalizeRaster()`.
     * @param toFuse
     *   Whether to write to fuse during finalize; if [[RASTER_PATH_KEY]] not already under the specified fuse dir.
     * @param overrideFuseDirOpt
     *   Option to specify the fuse dir location, None means use checkpoint dir;
     *   only relevant if 'toFuse' is true, default is None.
     *
     * @return
     *   [[RasterTile]] `this` (fluent).
     */
    def finalizeTile(toFuse: Boolean, overrideFuseDirOpt: Option[String] = None): RasterTile = {
        Try{
            if (overrideFuseDirOpt.isDefined) this.raster.setFuseDirOpt(overrideFuseDirOpt)
            this.raster.finalizeRaster(toFuse)
        }
        this
    }

    /**
     * Destroys the tile [[Dataset]] object.
     * @return
     *   [[RasterTile]] `this` (fluent).
     */
    def flushAndDestroy(): RasterTile = {
        Try(this.raster.flushAndDestroy())
        this
    }

    /**
      * Formats the index ID as the data type supplied by the index system.
      *
      * @param indexSystem
      *   Index system to use for formatting.
      * @return
      *   [[RasterTile]] with formatted index ID.
      */
    def formatCellId(indexSystem: IndexSystem): RasterTile = {
        if (Option(index).isEmpty) return this
        (indexSystem.getCellIdDataType, index) match {
            case (_: LongType, Left(_))       => this
            case (_: StringType, Right(_))    => this
            case (_: LongType, Right(value))  => this.copy(index = Left(indexSystem.parse(value)))
            case (_: StringType, Left(value)) => this.copy(index = Right(indexSystem.format(value)))
            case _                            => throw new IllegalArgumentException("Invalid cell id data type")
        }
    }

    /**
     * Attempt to initialize and hydrate the tile.
     * - essentially calls `tile.initAndHydrate()`.
     *
     * @param forceInit
     *   Whether to force an init, regardless of internal state of tile.
     * @return
     *   [[RasterTile]] `this` (fluent).
     */
    def initAndHydrateTile(forceInit: Boolean = false): RasterTile = {
        Try{
            this.raster.initAndHydrate(forceInit = forceInit)
        }
        this
    }

    /**
      * Formats the index ID as the long type.
      *
      * @param indexSystem
      *   Index system to use for formatting.
      * @return
      *   number variation of index id.
      */
    def cellIdAsLong(indexSystem: IndexSystem): Long =
        index match {
            case Left(value) => value
            case _           => indexSystem.parse(index.right.get)
        }

    /**
      * Formats the index ID as the string type.
      *
      * @param indexSystem
      *   Index system to use for formatting.
      * @return
      *   string variation of index id.
      */
    def cellIdAsStr(indexSystem: IndexSystem): String =
        index match {
            case Right(value) => value
            case _            => indexSystem.format(index.left.get)
        }

    /**
      * Serialize to spark internal representation.
      *
      * @param rasterDT
      *    How to encode the tile.
      *    - Options are [[StringType]] or [[BinaryType]]
      *    - If checkpointing is used, [[StringType]] will be forced
      *    - call finalize on tiles when serializing them.
      * @param doDestroy
      *   Whether to destroy the internal object after serializing.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @param overrideFuseDirOpt
      *   Option to override where to write [[StringType]], default is None (checkpoint dir).
      * @return
      *   An instance of [[InternalRow]].
      */
    def serialize(
                     rasterDT: DataType,
                     doDestroy: Boolean,
                     exprConfigOpt: Option[ExprConfig],
                     overrideFuseDirOpt: Option[String] = None
                 ): InternalRow = {

        // (1) finalize the tile's tile
        // - write to fuse if [[StringType]]
        val toFuse = rasterDT == StringType
        this.finalizeTile(toFuse, overrideFuseDirOpt = overrideFuseDirOpt)

        // (2) serialize the tile according to the specified serialization type
        val encodedRaster = GDAL.writeRasters(
            Seq(raster), rasterDT, doDestroy, exprConfigOpt, overrideFuseDirOpt).head

        val path = encodedRaster match {
                case uStr: UTF8String => uStr.toString
                case _ => this.raster.getRawPath // <- we want raw path here
        }

        // (3) update createInfo
        // - safety net for parent path
        val parentPath = this.raster.identifyPseudoPathOpt().getOrElse(NO_PATH_STRING)
        val newCreateInfo = raster.getCreateInfo + (RASTER_PATH_KEY -> path, RASTER_PARENT_PATH_KEY -> parentPath)
        val mapData = buildMapString(newCreateInfo)

        // (4) actual serialization
        if (Option(index).isDefined) {
            if (index.isLeft) InternalRow.fromSeq(
              Seq(index.left.get, encodedRaster, mapData)
            )
            else {
                InternalRow.fromSeq(
                  Seq(UTF8String.fromString(index.right.get), encodedRaster, mapData)
                )
            }
        } else {
            InternalRow.fromSeq(Seq(null, encodedRaster, mapData))
        }
    }

    def getSequenceNumber: Int = Try {
        this.raster
            .withDatasetHydratedOpt().get
            .GetMetadataItem("BAND_INDEX", "DATABRICKS_MOSAIC").toInt
    }.getOrElse(-1)

}

/** singleton static object. */
object RasterTile {

    /**
     * Smart constructor based on Spark internal instance.
     * - Must infer tile data type
     *
     * @param row
     *   An instance of [[InternalRow]].
     * @param idDataType
     *   The data type of the index ID.
     * @param exprConfigOpt
     *   Option [[ExprConfig]]
     * @return
     *   An instance of [[RasterTile]].
     */
    def deserialize(row: InternalRow, idDataType: DataType, exprConfigOpt: Option[ExprConfig]): RasterTile = {
        val index = row.get(0, idDataType)
        val rawRaster = Try(row.get(1, StringType)) match {
            case Success(value) => value
            case Failure(_) => row.get(1, BinaryType)
        }
        val rawRasterDataType = rawRaster match {
            case _: UTF8String => StringType
            case _ => BinaryType
        }

        val createInfo = extractMap(row.getMap(2))
        val raster = GDAL.readRasterExpr(rawRaster, createInfo, rawRasterDataType, exprConfigOpt)

        // noinspection TypeCheckCanBeMatch
        if (Option(index).isDefined) {
            if (index.isInstanceOf[Long]) {
                new RasterTile(Left(index.asInstanceOf[Long]), raster, rawRasterDataType)
            } else {
                new RasterTile(Right(index.asInstanceOf[UTF8String].toString), raster, rawRasterDataType)
            }
        } else {
            new RasterTile(null, raster, rawRasterDataType)
        }
    }

    /** returns rasterType from a passed DataType, handling RasterTileType as well as string + binary. */
    def getRasterType(dataType: DataType): DataType = {
        dataType match {
            case tile: RasterTileType  => tile.rasterType
            case _ => dataType
        }
    }

}
