package com.databricks.labs.mosaic.core.types.model

import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, DataType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.{Failure, Success, Try}

/**
  * A case class modeling an instance of a mosaic raster tile.
  *
  * @param index
  *   Index ID.
  * @param raster
  *   Raster instance corresponding to the tile.
  * @param parentPath
  *   Parent path of the raster.
  * @param driver
  *   Driver used to read the raster.
  */
case class MosaicRasterTile(
    index: Either[Long, String],
    raster: MosaicRasterGDAL,
    parentPath: String,
    driver: String
) {

    def getIndex: Either[Long, String] = index

    def getParentPath: String = parentPath

    def getDriver: String = driver

    def getRaster: MosaicRasterGDAL = raster

    /**
      * Indicates whether the raster is present.
      *
      * @return
      *   True if the raster is present, false otherwise.
      */
    def isEmpty: Boolean = Option(raster).forall(_.isEmpty)

    /**
      * Formats the index ID as the data type supplied by the index system.
      *
      * @param indexSystem
      *   Index system to use for formatting.
      * @return
      *   MosaicChip with formatted index ID.
      */
    def formatCellId(indexSystem: IndexSystem): MosaicRasterTile = {
        if (Option(index).isEmpty) return this
        (indexSystem.getCellIdDataType, index) match {
            case (_: LongType, Left(_))       => this
            case (_: StringType, Right(_))    => this
            case (_: LongType, Right(value))  => new MosaicRasterTile(
                  index = Left(indexSystem.parse(value)),
                  raster = raster,
                  parentPath = parentPath,
                  driver = driver
                )
            case (_: StringType, Left(value)) => new MosaicRasterTile(
                  index = Right(indexSystem.format(value)),
                  raster = raster,
                  parentPath = parentPath,
                  driver = driver
                )
            case _                            => throw new IllegalArgumentException("Invalid cell id data type")
        }
    }

    /**
      * Formats the index ID as the long type.
      *
      * @param indexSystem
      *   Index system to use for formatting.
      * @return
      *   MosaicChip with formatted index ID.
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
      *   MosaicChip with formatted index ID.
      */
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
        rasterDataType: DataType = BinaryType,
        checkpointLocation: String = ""
    ): InternalRow = {
        val parentPathUTF8 = UTF8String.fromString(parentPath)
        val driverUTF8 = UTF8String.fromString(driver)
        val encodedRaster = encodeRaster(rasterDataType, checkpointLocation)
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
        rasterDataType: DataType = BinaryType,
        checkpointLocation: String = ""
    ): Any = {
        GDAL.writeRasters(Seq(raster), checkpointLocation, rasterDataType).head
    }

    def getSequenceNumber: Int =
        Try(raster.getRaster.GetMetadataItem("BAND_INDEX", "DATABRICKS_MOSAIC")) match {
            case Success(value) => value.toInt
            case Failure(_)     => -1
        }
}

/** Companion object. */
object MosaicRasterTile {

    /**
      * Smart constructor based on Spark internal instance.
      *
      * @param row
      *   An instance of [[InternalRow]].
      * @param idDataType
      *   The data type of the index ID.
      * @return
      *   An instance of [[MosaicRasterTile]].
      */
    def deserialize(row: InternalRow, idDataType: DataType): MosaicRasterTile = {
        val index = row.get(0, idDataType)
        val rasterBytes = row.get(1, BinaryType)
        val parentPath = row.get(2, StringType).toString
        val driver = row.get(3, StringType).toString
        val raster = GDAL.readRaster(rasterBytes, parentPath, driver, BinaryType)
        // noinspection TypeCheckCanBeMatch
        if (Option(index).isDefined) {
            if (index.isInstanceOf[Long]) {
                new MosaicRasterTile(Left(index.asInstanceOf[Long]), raster, parentPath, driver)
            } else {
                new MosaicRasterTile(Right(index.asInstanceOf[UTF8String].toString), raster, parentPath, driver)
            }
        } else {
            new MosaicRasterTile(null, raster, parentPath, driver)
        }

    }

}
