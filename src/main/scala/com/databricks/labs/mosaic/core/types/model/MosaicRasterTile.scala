package com.databricks.labs.mosaic.core.types.model

import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, DataType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class MosaicRasterTile(
    index: Either[Long, String],
    raster: MosaicRaster,
    parentPath: String,
    driver: String
) {

    def isEmpty: Boolean = Option(raster).forall(_.isEmpty)

    def formatCellId(indexSystem: IndexSystem): MosaicRasterTile = {
        (indexSystem.getCellIdDataType, index) match {
            case (_: LongType, Left(_))       => this
            case (_: StringType, Right(_))    => this
            case (_: LongType, Right(value))  => this.copy(index = Left(indexSystem.parse(value)))
            case (_: StringType, Left(value)) => this.copy(index = Right(indexSystem.format(value)))
            case _                            => throw new IllegalArgumentException("Invalid cell id data type")
        }
    }

    def cellIdAsLong(indexSystem: IndexSystem): Long =
        index match {
            case Left(value) => value
            case _           => indexSystem.parse(index.right.get)
        }

    def cellIdAsStr(indexSystem: IndexSystem): String =
        index match {
            case Right(value) => value
            case _            => indexSystem.format(index.left.get)
        }

    /**
      * Serialise to spark internal representation.
      *
      * @return
      *   An instance of [[InternalRow]].
      */
    def serialize(
        rasterAPI: RasterAPI,
        rasterDataType: DataType = BinaryType,
        checkpointLocation: String = ""
    ): InternalRow = {
        val parentPathUTF8 = UTF8String.fromString(parentPath)
        val driverUTF8 = UTF8String.fromString(driver)
        val encodedRaster = encodeRaster(rasterAPI, rasterDataType, checkpointLocation)
        if (Option(index).isDefined) {
            if (index.isLeft) InternalRow.fromSeq(
              Seq(index.left.get, encodedRaster, parentPathUTF8, driverUTF8)
            )
            else InternalRow.fromSeq(
              Seq(UTF8String.fromString(index.right.get), encodedRaster, parentPathUTF8, driverUTF8)
            )
        } else {
            InternalRow.fromSeq(Seq(null, encodedRaster, parentPathUTF8, driverUTF8))
        }

    }

    /**
      * Encodes the chip geometry as WKB.
      *
      * @return
      *   An instance of [[Array]] of [[Byte]] representing WKB.
      */
    private def encodeRaster(
        rasterAPI: RasterAPI,
        rasterDataType: DataType = BinaryType,
        checkpointLocation: String = ""
    ): Any = {
        rasterAPI.writeRasters(Seq(raster), checkpointLocation, rasterDataType).head
    }

    def indexAsLong(indexSystem: IndexSystem): Long = {
        if (index.isLeft) index.left.get
        else indexSystem.formatCellId(index.right.get, LongType).asInstanceOf[Long]
    }

}

object MosaicRasterTile {

    def deserialize(row: InternalRow, idDataType: DataType, rasterAPI: RasterAPI): MosaicRasterTile = {
        val index = row.get(0, idDataType)
        val rasterBytes = row.get(1, BinaryType)
        val parentPath = row.get(2, StringType).toString
        val driver = row.get(3, StringType).toString
        val raster = rasterAPI.readRaster(rasterBytes, parentPath, driver, BinaryType)
        // noinspection TypeCheckCanBeMatch
        if (Option(index).isDefined) {
            if (index.isInstanceOf[Long]) {
                MosaicRasterTile(Left(index.asInstanceOf[Long]), raster, parentPath, driver)
            } else {
                MosaicRasterTile(Right(index.asInstanceOf[UTF8String].toString), raster, parentPath, driver)
            }
        } else {
            MosaicRasterTile(null, raster, parentPath, driver)
        }

    }

}
