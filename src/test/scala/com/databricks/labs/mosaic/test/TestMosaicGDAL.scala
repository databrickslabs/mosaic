package com.databricks.labs.mosaic.test

import com.databricks.labs.mosaic.gdal.MosaicGDAL._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging

import java.io.IOException
import scala.io.{BufferedSource, Source}
import scala.sys.process._

object TestMosaicGDAL extends Logging {

    def installGDAL(spark: SparkSession): Unit = {
        if (!wasEnabled(spark) && !isEnabled) installGDAL(Some(spark))
    }

    def installGDAL(spark: Option[SparkSession]): Unit = {
        val sc = spark.map(_.sparkContext)
        val numExecutors = sc.map(_.getExecutorMemoryStatus.size - 1)
        val script = getScript
        for (cmd <- script.getLines.toList) {
            try {
                if (!cmd.startsWith("#") || cmd.nonEmpty) cmd.!!
                sc.map { sparkContext =>
                    if (!sparkContext.isLocal) {
                        sparkContext.parallelize(1 to numExecutors.get).pipe(cmd).collect
                    }
                }
            } catch {
                case e: IOException           => logError(e.getMessage)
                case e: IllegalStateException => logError(e.getMessage)
                case e: SparkException        => logError(e.getMessage)
                case e: Throwable             => logError(e.getMessage)
            } finally {
                script.close
            }
        }
    }

    def getScript: BufferedSource = {
        val scriptPath = System.getProperty("os.name").toLowerCase() match {
            case o: String if o.contains("nux") => "/scripts/install-gdal-databricks.sh"
            case _ => throw new UnsupportedOperationException("This method only supports Ubuntu Linux with `apt`.")
        }
        val script = Source.fromInputStream(getClass.getResourceAsStream(scriptPath))
        script
    }

}
