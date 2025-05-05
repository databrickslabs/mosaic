package com.databricks.labs.mosaic.utils

import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

object IsolatedProcess {
    def runInNewProcess(command: String, timeout: Duration): Option[String] = {
        val process = Process(command).run()
        val futureResult = Future {
            val output = Process(command).!!
            output
        }

        try {
            //noinspection ScalaStyle
            Some(Await.result(futureResult, timeout))
        } catch {
            case _: TimeoutException =>
                process.destroy()
                None
        } finally {
            process.destroy()
        }
    }
}
