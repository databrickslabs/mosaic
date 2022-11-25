package com.databricks.labs.mosaic.models.util

import org.apache.spark.sql.Dataset

/**
 * A base trait for checkpointing using Delta Lake.
 * Delta checkpoints can be either a table or a directory.
 * Delta checkpoints are more performant for additive checkpoints.
 */
trait DeltaCheckpoint {

    def delete(): Unit

    def append(dataset: Dataset[_]): Unit

    def overwrite(dataset: Dataset[_]): Unit

    def load(): Dataset[_]

}
