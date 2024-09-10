package com.databricks.labs.mosaic.core.types.model

import com.databricks.labs.mosaic.NO_PATH_STRING
import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.gdal.RasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterIO
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.expressions.raster.{buildMapString, extractMap}
import com.databricks.labs.mosaic.functions.ExprConfig
import com.databricks.labs.mosaic.utils.FileUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, DataType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.control.Exception.allCatch
import scala.util.Try

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
    def isEmpty: Boolean = {
        raster.isEmptyRasterGDAL || raster.isEmpty
    }

    /**
     * Finalize the tile.
     * - essentially calls `raster.finalizeRaster()`.
     *
     * @param overrideFuseDirOpt
     *   Option to specify the fuse dir location, None means use checkpoint dir;
     *   only relevant if 'toFuse' is true, default is None.
     *
     * @return
     *   [[RasterTile]] `this` (fluent).
     */
    def finalizeTile(overrideFuseDirOpt: Option[String] = None): RasterTile = {
        Try{
            if (overrideFuseDirOpt.isDefined) this.raster.setFuseDirOpt(overrideFuseDirOpt)
            this.raster.finalizeRaster()
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
     * @return
     *   [[RasterTile]] `this` (fluent).
     */
    def tryInitAndHydrateTile(): RasterTile = {
        Try(this.raster.tryInitAndHydrate())
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
      *    - Options are [[StringType]] or [[BinaryType]].
      *    - Checkpointing is used regardless.
     *     - For [[BinaryType]] the binary payload also set on `raster`.
     *     - For [[StringType]] the binary payload is null.
      *    - Calls finalize on tiles when serializing them.
      * @param doDestroy
      *   Whether to destroy the internal object after serializing.
      * @param exprConfigOpt
      *   Option [[ExprConfig]]
      * @return
      *   An instance of [[InternalRow]].
      */
    def serialize(
                     rasterDT: DataType,
                     doDestroy: Boolean,
                     exprConfigOpt: Option[ExprConfig]
                 ): InternalRow = {
        // [1] handle the finalization of the raster
        // - writes to fuse for the path (if not already)
        // - expects any fuse dir customization to have already been set on raster
        // - this updates the createInfo 'path'
        this.finalizeTile()

        // [2] update the raw parent path with the "best"
        this.raster.updateRawParentPath(this.raster.identifyPseudoPathOpt()
            .getOrElse(NO_PATH_STRING))

        // [3] if `rasterDT` is [[BinaryType]],
        //     store the byte array as `raster`
        // - uses FS Path
        val binaryPayload =
            if (rasterDT == BinaryType) {
                Try(FileUtils.readBytes(
                    this.raster.getPathGDAL.asFileSystemPath,
                    uriDeepCheck = false)
                )
                    .getOrElse(Array.empty[Byte])
            } else null

        // [4] actual serialization
        // cell, raster, and map data
        val mapData = buildMapString(this.raster.getCreateInfo(includeExtras = true))
        val encodedTile = if (Option(index).isDefined) {
            if (index.isLeft) InternalRow.fromSeq(
              Seq(index.left.get, binaryPayload, mapData)
            )
            else {
                InternalRow.fromSeq(
                  Seq(UTF8String.fromString(index.right.get), binaryPayload, mapData)
                )
            }
        } else {
            InternalRow.fromSeq(Seq(null, binaryPayload, mapData))
        }
        // [5] handle destroy
        if (doDestroy) this.flushAndDestroy()

        encodedTile
    }

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

        // [0] read the cellid
        // - may be null
        val index = row.get(0, idDataType)

        // [1] read the binary payload
        // - either null or Array[Byte]
        val binaryPayload = getBinaryPayloadOrNull(row)

        // [2] read the `createInfo` map
        val createInfo = extractMap(row.getMap(2))

        // [3] read the rastersExpr
        val raster: RasterGDAL =
            Try {
                if (binaryPayload == null) {
                    // try to load from "path"
                    RasterIO.readRasterHydratedFromPath(
                        createInfo,
                        exprConfigOpt
                    )
                } else {
                    // try to load from binary
                    RasterIO.readRasterHydratedFromContent(
                        binaryPayload,
                        createInfo,
                        exprConfigOpt
                    )
                }
            }.getOrElse {
                // empty tile
                RasterGDAL()
            }

        val rasterDataType =
            if (Option(binaryPayload).isDefined) BinaryType
            else StringType

        // noinspection TypeCheckCanBeMatch
        if (Option(index).isDefined) {
            if (index.isInstanceOf[Long]) {
                new RasterTile(Left(index.asInstanceOf[Long]), raster, rasterDataType)
            } else {
                new RasterTile(Right(index.asInstanceOf[UTF8String].toString), raster, rasterDataType)
            }
        } else {
            new RasterTile(null, raster, rasterDataType)
        }
    }

    def getBinaryPayloadOrNull(tileInternalRow: InternalRow): Array[Byte] = {
        val binaryPayload: Array[Byte] = allCatch.opt(tileInternalRow.get(1, StringType)) match {
            case Some(_) => null // <- might be StringType prior to 0.4.3
            case _ => allCatch.opt(tileInternalRow.get(1, BinaryType)) match {
                case Some(binVal) => binVal.asInstanceOf[Array[Byte]]
                case _ => null
            }
        }
        binaryPayload
    }

    /** returns rasterType from a passed DataType, handling RasterTileType as well as string + binary. */
    def getRasterType(dataType: DataType): DataType = {
        dataType match {
            case tile: RasterTileType  => tile.rasterType
            case _ => dataType
        }
    }

}
