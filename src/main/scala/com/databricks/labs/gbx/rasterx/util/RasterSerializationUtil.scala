package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.rasterx.gdal.{CheckpointManager, GDAL, RasterDriver}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.Dataset

import scala.util.Try

object RasterSerializationUtil {

    def rowToTile(row: InternalRow, rasterDT: DataType): (Long, Dataset, Map[String, String]) = {
        val cellID = row.getLong(0)
        val metadataRow = row.getMap(2)
        val metadata = SerializationUtil.createMap[String, String](metadataRow)
        rasterDT match {
            case StringType =>
                val path = row.getString(1)
                val ds = RasterDriver.read(path, metadata)
                (cellID, ds, metadata)
            case BinaryType =>
                val buffer = row.getBinary(1)
                val ds = RasterDriver.readFromBytes(buffer, metadata)
                (cellID, ds, metadata)
        }
    }

    def rowToDS(row: InternalRow, rasterDT: DataType, shared: Boolean = false): Dataset = {
        val metadataRow = row.getMap(2)
        val metadata = SerializationUtil.createMap[String, String](metadataRow)
        rasterDT match {
            case StringType =>
                val path = row.getString(1)
                RasterDriver.read(path, metadata, shared)
            case BinaryType =>
                val buffer = row.getBinary(1)
                RasterDriver.readFromBytes(buffer, metadata)
        }
    }

    def tileToRow(tuple: (Long, Dataset, Map[String, String]), dataType: DataType, hconf: SerializableConfiguration): InternalRow = {
        dataType match {
            case BinaryType =>
                val metadata = SerializationUtil.toMapData[String, String](tuple._3)
                val bytes =
                    if (tuple._2 == null) {
                        Array.emptyByteArray
                    } else {
                        RasterDriver.writeToBytes(tuple._2, tuple._3)
                    }
                InternalRow.fromSeq(
                  Seq(
                    tuple._1, // cellid
                    bytes, // binary
                    metadata // metadata
                  )
                )
            case StringType =>
                val cpPath = CheckpointManager.getCheckpointPath
                val stagePrefix = Try {
                    val tc = TaskContext.get()
                    val sid = tc.stageId()
                    val san = tc.stageAttemptNumber()
                    s"stage_${sid}_$san"
                }.getOrElse("non_task")
                val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
                val extension = GDAL.getExtension(tuple._2.GetDriver().getShortName)
                // better keep the output in a directory to manage for sidecar files
                val outPath = s"$cpPath/$stagePrefix/raster_$uuid.$extension"
                val mtd = tuple._3 ++ Map(
                  "driver" -> tuple._2.GetDriver().getShortName,
                  "extension" -> extension,
                  "path" -> outPath,
                  "all_parents" -> Seq(
                    tuple._3.getOrElse("all_parents", ""),
                    tuple._3.getOrElse("path", "")
                  ).mkString(";")
                )
                val metadata = SerializationUtil.toMapData[String, String](mtd)
                if (tuple._2 != null) RasterDriver.write(tuple._2, outPath, tuple._3, hconf) // path
                InternalRow.fromSeq(
                  Seq(
                    tuple._1, // cellid
                    UTF8String.fromString(outPath), // path
                    metadata // metadata
                  )
                )
        }
    }

    def arrayToTiles(array: ArrayData, dataType: DataType): Seq[(Long, Dataset, Map[String, String])] = {
        val n = array.numElements()
        (0 until n).map { i =>
            val row = array.getStruct(i, 3)
            rowToTile(row, dataType)
        }
    }

}
