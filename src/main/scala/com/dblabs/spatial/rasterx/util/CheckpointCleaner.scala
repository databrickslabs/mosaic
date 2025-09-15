package com.dblabs.spatial.rasterx.util

import com.dblabs.spatial.rasterx.gdal.CheckpointManager
import com.dblabs.spatial.util.HadoopUtils
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
