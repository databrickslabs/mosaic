package com.databricks.labs.mosaic.gdal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException

import java.io.IOException
import scala.io.{BufferedSource, Source}
import scala.sys.process._

object MosaicGDAL extends Logging {

    def enableGDAL(spark: SparkSession): Unit = {
        installGDAL(Some(spark))
        spark.sparkContext.addSparkListener(new GDALListener())
    }

    def installGDAL(spark: Option[SparkSession]): Unit = {
        val sc = spark.map(_.sparkContext)
        val numExecutors = sc.map(_.getExecutorMemoryStatus.size - 1)
        val script = getScript
        for (cmd <- script.getLines.toList) {
            try {
                cmd.!!
                sc.map { sparkContext =>
                    if (!sparkContext.isLocal) {
                        sparkContext.parallelize(1 to numExecutors.get).pipe(cmd).collect
                    }
                }
            } catch {
                case e: IOException           => logError(e.getMessage)
                case e: IllegalStateException => logError(e.getMessage)
                case e: SparkException        => logError(e.getMessage)
            } finally {
                script.close
            }
        }
    }

    def getScript: BufferedSource = {
        val scriptPath = System.getProperty("os.name").toLowerCase() match {
            case o: String if o.contains("nux") => "/scripts/install-gdal-debian-ubuntu.sh"
            case o: String if o.contains("mac") => "/scripts/install-gdal-macos.sh"
            case _ => throw new UnsupportedOperationException("This method only supports Ubuntu Linux with `apt` and MacOS with `brew`.")
        }
        val script = Source.fromInputStream(getClass.getResourceAsStream(scriptPath))
        script
    }

}
