package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.{CheckpointManager, GDALManager}
import com.databricks.labs.gbx.util.NodeFileManager
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.apache.spark.util.TaskFailureListener

import scala.util.Try

object RST_ExpressionUtil {

    def rasterType(tileExpr: Expression): DataType = tileExpr.dataType.asInstanceOf[StructType].fields(1).dataType

    def tileDataType(tileExpr: Expression): DataType = {
        val rasterDataType = rasterType(tileExpr)
        StructType(
          Seq(
            StructField("cellid", LongType, nullable = false),
            StructField("raster", rasterDataType, nullable = false),
            StructField("metadata", MapType(StringType, StringType), nullable = true)
          )
        )
    }

    def tileDataType(rdt: DataType): DataType = {
        StructType(
          Seq(
            StructField("cellid", LongType, nullable = false),
            StructField("raster", rdt, nullable = false),
            StructField("metadata", MapType(StringType, StringType), nullable = true)
          )
        )
    }

    def init(expressionConfig: ExpressionConfig): Unit = {
        NodeFileManager.init(expressionConfig.hConf)
        GDALManager.init(expressionConfig)
        CheckpointManager.init(expressionConfig)
    }

    def addCleanupListener(it: Iterator[_]): Unit = {
        val iter = it.asInstanceOf[AutoCloseable]
        Try {
            val tc = org.apache.spark.TaskContext.get()
            tc.addTaskCompletionListener[Unit](_ => iter.close())
            tc.addTaskFailureListener(new TaskFailureListener() {
                override def onTaskFailure(context: TaskContext, error: Throwable): Unit = iter.close()
            })
        }
    }

}
