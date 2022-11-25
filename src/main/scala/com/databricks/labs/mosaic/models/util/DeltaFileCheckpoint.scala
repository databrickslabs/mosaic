package com.databricks.labs.mosaic.models.util

import org.apache.spark.sql._

import java.io.{File, IOException}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.UUID

/**
  * Utility class to manage checkpointing of the mosaic model results/auxiliary
  * data.
  *
  * @param parentPath
  *   The path to the checkpoint directory. If the directory does not exist, it
  *   will be created.
  * @param name
  *   Table sub directory name. UUID will be added to the end of the table name.
  */
case class DeltaFileCheckpoint(parentPath: String, name: String) extends DeltaCheckpoint {

    private val uniqueName = s"$parentPath/${name}_${UUID.randomUUID().toString}"

    /** Deletes the checkpoint directory. */
    override def delete(): Unit = {
        deleteRecursively(Paths.get(uniqueName))
    }

    /**
      * Append data to a checkpoint.
      * @param dataset
      *   The dataset to append.
      */
    override def append(dataset: Dataset[_]): Unit = {
        makeDirectory(parentPath)
        dataset.write.mode(SaveMode.Append).format("delta").save(uniqueName)
    }

    /**
      * Overwrite the checkpoint for the given table location.
      * @param dataset
      *   The dataset to use to overwrite the checkpoint with.
      */
    override def overwrite(dataset: Dataset[_]): Unit = {
        makeDirectory(parentPath)
        dataset.write.mode(SaveMode.Overwrite).format("delta").save(uniqueName)
    }

    /**
      * Read the checkpoint for the given table name/location.
      *
      * @return
      *   The checkpoint dataset.
      */
    override def load(): Dataset[_] = {
        val spark = SparkSession.builder().getOrCreate()
        spark.read.format("delta").load(uniqueName)
    }

    /**
      * Delete a directory recursively.
      * @param root
      *   The path to the directory to delete.
      */
    private def deleteRecursively(root: Path): Unit = {
        if (Files.exists(root)) {
            Files.walkFileTree(
              root,
              new SimpleFileVisitor[Path] {
                  override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                      Files.delete(file)
                      FileVisitResult.CONTINUE
                  }
                  override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
                      Files.delete(dir)
                      FileVisitResult.CONTINUE
                  }
              }
            )
        }
    }

    /**
      * Create the checkpoint directory if it does not exist.
      * @param path
      *   The path to the checkpoint directory.
      */
    private def makeDirectory(path: String): Unit = {
        val dir = new File(path)
        if (!dir.exists()) {
            dir.mkdirs()
        }
    }

}
