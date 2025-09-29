package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.rasterx.gdal.CheckpointManager
import com.databricks.labs.gbx.util.HadoopUtils
import org.apache.spark.util.SerializableConfiguration

object CheckpointCleaner {

    def getStageDirs(hconf: SerializableConfiguration): Seq[(Int, String)] = {
        val cpPath = CheckpointManager.getCheckpointPath
        HadoopUtils
            .listHadoopDirs(cpPath, hconf)
            .filter(path => path.contains("stage_"))
            .map { path =>
                val stagePart = path.split("/").last
                val stageId = stagePart.split("_")(1).toInt
                (stageId, path)
            }
    }

    def deleteStages(stages: Seq[Int], hconf: SerializableConfiguration): Unit = {
        val existingStageDirs = getStageDirs(hconf)
        existingStageDirs.foreach { case (sid, path) =>
            if (stages.contains(sid)) {
                // Delete the directory
                HadoopUtils.deleteIfExists(path, hconf)
            }
        }
    }

}
