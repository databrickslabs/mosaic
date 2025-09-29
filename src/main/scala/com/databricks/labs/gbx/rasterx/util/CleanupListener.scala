package com.databricks.labs.gbx.rasterx.util

import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession

import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListSet}
import scala.jdk.CollectionConverters.CollectionHasAsScala

class CleanupListener(spark: SparkSession) extends SparkListener {

    private def cleanup(executionId: Long): Unit = {
        val stages = stageIdsForExec(executionId).toSeq
        val hconf = new org.apache.spark.util.SerializableConfiguration(spark.sessionState.newHadoopConf)
        CheckpointCleaner.deleteStages(stages, hconf)
    }

    // execId -> active/all stages
    private val exec2active = new ConcurrentHashMap[Long, ConcurrentSkipListSet[Int]]()
    private val exec2all = new ConcurrentHashMap[Long, ConcurrentSkipListSet[Int]]()
    // stageId -> execId
    private val stage2exec = new ConcurrentHashMap[Int, Long]()
    // execs that have received SQLExecutionEnd (finish signal)
    private val finished = java.util.Collections.newSetFromMap(new ConcurrentHashMap[Long, java.lang.Boolean]())
    // execId -> RDD ids used by this execution (to respect cache/unpersist)
    private val exec2rdds = new ConcurrentHashMap[Long, ConcurrentSkipListSet[Int]]()

    // ---- Queries ----
    private def stageIdsForExec(execId: Long): Set[Int] = Option(exec2all.get(execId)).map(_.asScala.toSet).getOrElse(Set.empty)

    // detect mappings
    override def onOtherEvent(e: SparkListenerEvent): Unit =
        e match {
            case s: org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart =>
                exec2active.computeIfAbsent(s.executionId, _ => new ConcurrentSkipListSet[Int]())
                exec2all.computeIfAbsent(s.executionId, _ => new ConcurrentSkipListSet[Int]())
                exec2rdds.computeIfAbsent(s.executionId, _ => new ConcurrentSkipListSet[Int]())
            case s: org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd   =>
                finished.add(s.executionId); maybeCleanup(s.executionId)
            case _                                                                   => ()
        }

    override def onStageSubmitted(e: SparkListenerStageSubmitted): Unit = {
        val execIdOpt = Option(e.properties)
            .flatMap(p => Option(p.getProperty("spark.sql.execution.id")).map(_.toLong))
        execIdOpt.foreach { execId =>
            val stg = e.stageInfo.stageId
            stage2exec.put(stg, execId)
            exec2active.computeIfAbsent(execId, _ => new ConcurrentSkipListSet[Int]()).add(stg)
            exec2all.computeIfAbsent(execId, _ => new ConcurrentSkipListSet[Int]()).add(stg)
            // track RDDs used by this execution
            val rddSet = exec2rdds.computeIfAbsent(execId, _ => new ConcurrentSkipListSet[Int]())
            e.stageInfo.rddInfos.foreach(ri => rddSet.add(ri.id))
        }
    }

    override def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
        val stg = e.stageInfo.stageId
        Option(stage2exec.get(stg)).foreach { execId =>
            Option(exec2active.get(execId)).foreach(_.remove(stg))
            maybeCleanup(execId) // if SQL end already seen, this may trigger cleanup
        }
    }

    private def hasPersistedRDDs(execId: Long): Boolean = {
        val sc = spark.sparkContext
        val rdds = Option(exec2rdds.get(execId)).map(_.asScala.toSet).getOrElse(Set.empty[Int])
        if (rdds.isEmpty) false
        else sc.getPersistentRDDs.keys.exists(rdds.contains)
    }

    private def maybeCleanup(execId: Long): Unit = {
        val noActive = Option(exec2active.get(execId)).forall(_.isEmpty)
        if (finished.contains(execId) && noActive && !hasPersistedRDDs(execId)) {
            try cleanup(execId)
            finally {
                exec2active.remove(execId); exec2all.remove(execId); finished.remove(execId)
                exec2rdds.remove(execId)
                stage2exec.entrySet().removeIf(_.getValue == execId)
            }
        }
    }

}
