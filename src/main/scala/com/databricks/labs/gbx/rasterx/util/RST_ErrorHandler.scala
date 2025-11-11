package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

object RST_ErrorHandler {

    private def hasError(metadata: Map[String, String]): Boolean = {
        metadata.contains("error_message")
    }

    private def createErrorMetadata(error: Throwable): Map[String, String] = {
        Map(
          "error_message" -> error.getMessage,
          "error_detail" -> error.getStackTrace.mkString("\n"),
          "gdal_error" -> org.gdal.gdal.gdal.GetLastErrorMsg()
        )
    }

    // Just wraps eval methods to catch exceptions and check for error propagation
    def safeEval(eval: () => InternalRow, row: InternalRow, rasterType: DataType): InternalRow = {
        try {
            eval()
        } catch {
            case e: Throwable =>
                // Check if input already had error
                val (cellId, metadata) = Try { // just in case of malformed rows and unexpected errors
                    val (cellId, ds, metadata) = RasterSerializationUtil.rowToTile(row, rasterType)
                    RasterDriver.releaseDataset(ds)
                    (cellId, metadata)
                }.getOrElse((-1L, Map.empty[String, String]))
                if (hasError(metadata)) {
                    // Return input as-is since it already had error
                    row
                } else {
                    // Create new error row
                    val errorMetadata = createErrorMetadata(e)
                    RasterSerializationUtil.tileToRow((cellId, null, errorMetadata), rasterType, null)
                }
        }
    }

    def safeEval(eval: () => InternalRow, rows: ArrayData, rasterType: DataType): InternalRow = {
        try {
            eval()
        } catch {
            case e: Throwable =>
                // Check if input already had error
                val errorIdx = (0 until rows.numElements()).find { i =>
                    val row = rows.getStruct(i, 3)
                    val metadata = Try { // just in case of malformed rows and unexpected errors
                        val (_, ds, metadata) = RasterSerializationUtil.rowToTile(row, rasterType)
                        RasterDriver.releaseDataset(ds)
                        metadata
                    }.getOrElse(Map.empty[String, String])
                    hasError(metadata)
                }
                if (errorIdx.nonEmpty) {
                    // Rethrow since no input had error
                    rows.getStruct(errorIdx.get, 3)
                } else {
                    // Create new error row
                    val errorMetadata = createErrorMetadata(e)
                    RasterSerializationUtil.tileToRow((-1, null, errorMetadata), rasterType, null)
                }
        }
    }

    def safeEval(eval: () => Any, row: InternalRow, rasterType: DataType, conf: UTF8String): Any = {
        try {
            eval()
        } catch {
            case _: Throwable =>
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                if (exprConf.crashExpressions) {
                    val (cellId, metadata) = Try { // just in case of malformed rows and unexpected errors
                        val (cellId, ds, metadata) = RasterSerializationUtil.rowToTile(row, rasterType)
                        RasterDriver.releaseDataset(ds)
                        (cellId, metadata)
                    }.getOrElse((-1L, Map.empty[String, String]))
                    throw new Error(s"""
                                       |Error during expression evaluation. Cell ID: $cellId
                                       |Metadata: $metadata
                                       |""".stripMargin)
                }
                null // swallow the error and return null for any eval
        }
    }

    def safeEval(eval: () => IterableOnce[InternalRow], row: InternalRow, rasterType: DataType): IterableOnce[InternalRow] = {
        try {
            eval()
        } catch {
            case e: Throwable =>
                val cellId = Try { // just in case of malformed rows and unexpected errors
                    val (cellId, ds, _) = RasterSerializationUtil.rowToTile(row, rasterType)
                    RasterDriver.releaseDataset(ds)
                    cellId
                }.getOrElse(-1L)
                val errorMetadata = createErrorMetadata(e)
                Seq(RasterSerializationUtil.tileToRow((cellId, null, errorMetadata), rasterType, null))
        }
    }

}
