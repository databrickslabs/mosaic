package com.databricks.labs.mosaic.models.util

import org.apache.spark.sql._

import java.util.UUID

/**
 * Utility class to manage checkpointing of the mosaic model results/auxiliary data.
 *
 * @param database
 *   The database where table will be stored in.
 * @param table
 *   Table name for the checkpoint. UUID will be added to the end of the table name.
 */
case class DeltaTableCheckpoint(database: String, table: String) extends DeltaCheckpoint {

    private val uniqueName = s"$database.${table}_${UUID.randomUUID().toString.replace("-", "_")}"

    /**
      * Deletes the checkpoint table.
      */
    override def delete(): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        spark.sql(s"DROP TABLE IF EXISTS $uniqueName")
    }

    /**
      * Append data to a checkpoint.
      * @param dataset
      *   The dataset to append.
      */
    override def append(dataset: Dataset[_]): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
        dataset.write.mode(SaveMode.Append).format("delta").saveAsTable(uniqueName)
    }

    /**
      * Overwrite the checkpoint for the given table name.
      * @param dataset
      *   The dataset to use to overwrite the checkpoint with.
      */
    override def overwrite(dataset: Dataset[_]): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
        dataset.write.mode(SaveMode.Overwrite).format("delta").saveAsTable(uniqueName)
    }

    /**
      * Read the checkpoint for the given table name.
      * @return
      *   The checkpoint dataset.
      */
    override def load(): Dataset[_] = {
        val spark = SparkSession.builder().getOrCreate()
        spark.read.table(uniqueName)
    }

}
