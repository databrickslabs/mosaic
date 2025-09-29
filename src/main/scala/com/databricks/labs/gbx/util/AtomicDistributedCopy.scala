package com.databricks.labs.gbx.util

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import java.time.{Duration, Instant}

object AtomicDistributedCopy {

    // Maximum wait time for file existence (10 seconds)
    private val MAX_WAIT_TIME_MS = 10000

    def copyIfNeeded(
        srcFs: FileSystem,
        dstFs: FileSystem,
        srcPath: Path,
        dstPath: Path
    ): Unit = {
        if (!dstFs.exists(dstPath)) {
            try {
                val flag = FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, srcFs.getConf)
                if (!flag) {
                    throw new RuntimeException(s"Failed to copy $srcPath to $dstPath")
                }
            } catch {
                case _: Throwable => waitUntilFileExists(dstFs, dstPath)
            }
        } else {
            waitUntilFileExists(dstFs, dstPath)
        }
    }

    private def waitUntilFileExists(fs: FileSystem, path: Path): Unit = {
        val startTime = Instant.now()
        while (!fs.exists(path) && Duration.between(startTime, Instant.now()).toMillis < MAX_WAIT_TIME_MS) {
            Thread.sleep(200)
        }
    }

}
