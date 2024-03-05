package com.databricks.labs.mosaic.utils

import java.io.{BufferedInputStream, FileInputStream}
import java.nio.file.{Files, Paths}

object FileUtils {

    def readBytes(path: String): Array[Byte] = {
        val bufferSize = 1024 * 1024 // 1MB
        val cleanPath = PathUtils.replaceDBFSTokens(path)
        val inputStream = new BufferedInputStream(new FileInputStream(cleanPath))
        val buffer = new Array[Byte](bufferSize)

        var bytesRead = 0
        var bytes = Array.empty[Byte]

        while ({
            bytesRead = inputStream.read(buffer); bytesRead
        } != -1) {
            bytes = bytes ++ buffer.slice(0, bytesRead)
        }
        inputStream.close()
        bytes
    }

    def createMosaicTempDir(prefix: String = "/tmp"): String = {
        val tempRoot = Paths.get(s"$prefix/mosaic_tmp/")
        if (!Files.exists(tempRoot)) {
            Files.createDirectories(tempRoot)
        }
        val tempDir = Files.createTempDirectory(tempRoot, "mosaic")
        tempDir.toFile.getAbsolutePath
    }

}
