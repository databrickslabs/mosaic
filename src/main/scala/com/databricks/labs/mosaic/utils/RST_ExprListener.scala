package com.databricks.labs.mosaic.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.Locale

class RST_ExprListener(spark: SparkSession) extends QueryExecutionListener {

    private def cleanUp(): Unit = {
        val numExecs = math.max(1, spark.sparkContext.getExecutorMemoryStatus.size - 1)
        val df = spark
            .range(numExecs.toLong)
            .toDF("id")
            .repartition(numExecs)
        val deleteUDF = udf((_: Long) => {
            TmpDirCleaner.collectEmptyTmpDirs()
            1L // unit is not supported in UDFs, so we return a dummy value
        })
        df.select(
          deleteUDF(col("id"))
        ).collect() // this will trigger the UDF and clean up tmp directories
    }

    private def cleanUp(qe: QueryExecution): Unit = {
        if (shouldClean(qe)) { cleanUp() }
    }

    private def shouldClean(qe: QueryExecution): Boolean = {
        val planDescription = qe.executedPlan.toString.toLowerCase(Locale.ROOT)
        // this avoids cache-ing actions from hiding the RST_ expressions
        // if cache is called after RST_ expressions the leaf node would be InMemoryRelation
        // any rst_ expression would be beyond the leaf node horizon
        planDescription.contains("gdal") || planDescription.contains("rst_")
    }

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = cleanUp(qe)
    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = cleanUp(qe)

}
