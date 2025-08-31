package com.dblabs.spatial.rasterx.util

import com.dblabs.spatial.rasterx.gdal.CheckpointManager
import com.dblabs.spatial.util.HadoopUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable

class CheckpointCleanerListener(spark: SparkSession) extends SparkListener {

    private val stagesMap = mutable.Map[Int, Seq[Int]]()

    private def getStageDirs(hconf: SerializableConfiguration): Seq[(Int, String)] = {
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

    private def deleteStages(stages: Seq[Int], hconf: SerializableConfiguration): Unit = {
        val existingStageDirs = getStageDirs(hconf)
        existingStageDirs.foreach { case (sid, path) =>
            if (stages.contains(sid)) {
                // Delete the directory
                HadoopUtils.deleteIfExists(path, hconf)
            }
        }
    }

    override def onJobStart(js: SparkListenerJobStart): Unit = {
        val jid = js.jobId
        val stages = js.stageIds
        stagesMap(jid) = stages
    }

    override def onJobEnd(je: SparkListenerJobEnd): Unit = {
        val stages = stagesMap.getOrElse(je.jobId, Seq.empty)
        val hconf = new SerializableConfiguration(spark.sessionState.newHadoopConf)
        deleteStages(stages, hconf)
        stagesMap.remove(je.jobId)
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        val stages = stagesMap.values.flatten
        val hconf = new SerializableConfiguration(spark.sessionState.newHadoopConf)
        deleteStages(stages.toSeq, hconf)
    }

}
