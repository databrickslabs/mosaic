package com.databricks.labs.mosaic.models.util

import org.apache.spark.sql.Dataset

/**
  * CheckpointManager is a utility class to manage checkpointing of the mosaic
  * model.
  *
  * @param checkpoint
  *   Either DeltaFile or DeltaTable checkpoint instance.
  */
case class CheckpointManager(checkpoint: DeltaCheckpoint) {

    /** Delete the checkpoint. */
    def delete(): Unit = checkpoint.delete()

    /**
      * Append data to a checkpoint.
      * @param dataset
      *   The dataset to append.
      */
    def append(dataset: Dataset[_]): Dataset[_] = {
        checkpoint.append(dataset)
        checkpoint.load()
    }

    /**
      * Overwrite the checkpoint.
      * @param dataset
      *   The dataset to use to overwrite the checkpoint with.
      */
    def overwrite(dataset: Dataset[_]): Dataset[_] = {
        checkpoint.overwrite(dataset)
        checkpoint.load()
    }

    /**
      * Read the checkpoint.
      *
      * @return
      *   The checkpoint dataset.
      */
    def load(): Dataset[_] = checkpoint.load()

}

object CheckpointManager {

    /**
      * Create a checkpoint manager using an underlying table.
      *
      * @param database
      *   The database to be used for the checkpoint.
      * @param table
      *   The table name to be used for the checkpoint. The name is suffix with
      *   a UUID to ensure uniqueness.
      * @return
      *   A checkpoint manager.
      */
    def asTable(database: String, table: String): CheckpointManager = {
        new CheckpointManager(DeltaTableCheckpoint(database, table))
    }

    /**
      * Create a checkpoint manager using an underlying directory.
      *
      * @param parentPath
      *   The path to the checkpoint directory. If the directory does not exist,
      *   it will be created.
      * @param name
      *   The name of the checkpoint. A UUID will be added to the end of the
      *   name to ensure uniqueness.
      * @return
      *   A checkpoint manager.
      */
    def asDirectory(parentPath: String, name: String): CheckpointManager = {
        new CheckpointManager(DeltaFileCheckpoint(parentPath, name))
    }

    /**
      * Create a checkpoint manager as either a table or directory. The choice
      * is made based on the isTable parameter.
      *
      * @param location
      *   The path to the checkpoint directory or the database name. If the
      *   directory does not exist, it will be created. If the database does not
      *   exist, it will be created.
      * @param name
      *   The name of the checkpoint. A UUID will be added to the end of the
      *   name to ensure uniqueness.
      * @param isTable
      *   If true, the checkpoint will be a table. If false, the checkpoint will
      *   be a directory.
      * @return
      *   A checkpoint manager.
      */
    def apply(location: String, name: String, isTable: Boolean): CheckpointManager = {
        if (isTable) {
            asTable(location, name)
        } else {
            asDirectory(location, name)
        }
    }

}
