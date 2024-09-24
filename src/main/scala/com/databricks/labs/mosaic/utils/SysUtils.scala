package com.databricks.labs.mosaic.utils

import java.io.{BufferedReader, ByteArrayOutputStream, InputStreamReader, PrintWriter}

object SysUtils {

    import sys.process._

    def runCommand(cmd: String): (String, String, String) = {
        val stdoutStream = new ByteArrayOutputStream
        val stderrStream = new ByteArrayOutputStream
        val stdoutWriter = new PrintWriter(stdoutStream)
        val stderrWriter = new PrintWriter(stderrStream)
        val exitValue =
            try {
                // noinspection ScalaStyle
                cmd.!!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
            } catch {
                case e: Exception => s"ERROR: ${e.getMessage}"
            } finally {
                stdoutWriter.close()
                stderrWriter.close()
            }
        (exitValue, stdoutStream.toString, stderrStream.toString)
    }

    def runScript(cmd: Array[String]): (String, String, String) = {
        val p = Runtime.getRuntime.exec(cmd)
        val stdinStream = new BufferedReader(new InputStreamReader(p.getInputStream))
        val stderrStream = new BufferedReader(new InputStreamReader(p.getErrorStream))
        val exitValue =
            try {
                p.waitFor()
            } catch {
                case e: Exception => s"ERROR: ${e.getMessage}"
            }
        val stdinOutput = stdinStream.lines().toArray.mkString("\n")
        val stderrOutput = stderrStream.lines().toArray.mkString("\n")
        stdinStream.close()
        stderrStream.close()
        (s"$exitValue", stdinOutput, stderrOutput)
    }
    
    def getLastOutputLine(prompt: (String, String, String)): String = {
        val (_, stdout, _) = prompt
        val lines = stdout.split("\n")
        lines.last
    }

}
