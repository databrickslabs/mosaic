package com.dblabs.spatial.util

import java.io.{BufferedReader, InputStreamReader}

object SysUtils {

    import sys.process._

    def runCommand(parts: Seq[String]): (String, String, String) = {
        val out = new StringBuilder
        val err = new StringBuilder
        val _ = Process(parts).!(
          ProcessLogger(
            s => out.append(s).append('\n'),
            e => err.append(e).append('\n')
          )
        ) // waits & reaps
        val stdout = out.toString
        (stdout, stdout, err.toString) // keep legacy tuple contract
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
