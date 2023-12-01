package com.databricks.labs.mosaic.utils

import java.io.{ByteArrayOutputStream, PrintWriter}

object SysUtils {

    import sys.process._

    def runCommand(cmd: String): (String, String, String) = {
        val stdoutStream = new ByteArrayOutputStream
        val stderrStream = new ByteArrayOutputStream
        val stdoutWriter = new PrintWriter(stdoutStream)
        val stderrWriter = new PrintWriter(stderrStream)
        val exitValue = try {
            //noinspection ScalaStyle
            cmd.!!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
        } catch {
            case _: Exception => "ERROR"
        } finally {
            stdoutWriter.close()
            stderrWriter.close()
        }
        (exitValue, stdoutStream.toString, stderrStream.toString)
    }

}
