package com.databricks.labs.mosaic.models

import org.apache.spark.sql._

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

case class CheckpointManager(location: String, isTable: Boolean) {

    def deleteIfExists(): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        if (isTable) {
            spark.sql(s"drop table if exists $location")
        } else {
            deleteRecursively(Path.of(location))
        }
    }

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

    def appendToCheckpoint(dataset: Dataset[_], checkpointName: String): Unit = {
        if (isTable) {
            dataset.write.mode(SaveMode.Append).format("delta").saveAsTable(location + checkpointName)
        } else {
            dataset.write.mode(SaveMode.Append).format("delta").save(location + checkpointName)
        }
    }

    def overwriteCheckpoint(dataset: Dataset[_], checkpointName: String): Unit = {
        if (isTable) {
            dataset.write.mode(SaveMode.Overwrite).format("delta").saveAsTable(location + checkpointName)
        } else {
            dataset.write.mode(SaveMode.Overwrite).format("delta").save(location + checkpointName)
        }
    }

    def loadFromCheckpoint(checkpointName: String): Dataset[_] = {
        val spark = SparkSession.builder().getOrCreate()
        if (isTable) {
            spark.read.table(location + checkpointName)
        } else {
            spark.read.format("delta").load(location + checkpointName)
        }
    }

}
