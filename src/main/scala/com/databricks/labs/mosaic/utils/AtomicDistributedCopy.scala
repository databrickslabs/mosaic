package com.databricks.labs.mosaic.utils

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, Path}
import org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager

import java.util.concurrent.TimeoutException
import java.time.{Duration, Instant}

object AtomicDistributedCopy {

    // Maximum wait time for file existence (10 seconds)
    private val MAX_WAIT_TIME_MS = 10000

    def copyIfNeeded(
                        fileManager: FileSystemBasedCheckpointFileManager,
                        fs: FileSystem,
                        srcPath: Path,
                        dstPath: Path
                    ): Unit = {

        if (!fileManager.exists(dstPath)) {
            try {
                val out = fileManager.createAtomic(dstPath, overwriteIfPossible = false)
                val in = fs.openFile(srcPath).build().get()
                try {
                    val bufferSize = 1024 * 1024 // 1 MB
                    val buffer = new Array[Byte](bufferSize)
                    var bytesRead = in.read(buffer)
                    while (bytesRead > 0) {
                        out.write(buffer, 0, bytesRead)
                        bytesRead = in.read(buffer)
                    }
                } finally {
                    in.close()
                    out.close()
                }
            } catch {
                case _: FileAlreadyExistsException =>
                    waitUntilFileExists(fileManager, dstPath)
            }
        } else {
            waitUntilFileExists(fileManager, dstPath)
        }
    }

    private def waitUntilFileExists(fileManager: FileSystemBasedCheckpointFileManager, path: Path): Unit = {
        val startTime = Instant.now()
        
        while (!fileManager.exists(path)) {
            // Check if we've exceeded our timeout
            if (Duration.between(startTime, Instant.now()).toMillis > MAX_WAIT_TIME_MS) {
                throw new TimeoutException(s"Timed out waiting for file to exist: $path")
            }
            
            Thread.sleep(500)
        }
    }

}
