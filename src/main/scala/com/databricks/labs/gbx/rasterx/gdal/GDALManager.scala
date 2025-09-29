package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.internal.Logging
import org.gdal.gdal.gdal

import java.nio.file.{Files, Paths}
import scala.util.{Success, Try}

object GDALManager extends Logging {

    var isEnabled = false
    private val lock = AnyRef
    var checkpointPath: String = _
    var useCheckpoint: Boolean = _

    def init(config: ExpressionConfig): Unit =
        lock.synchronized {
            if (!isEnabled) {
                Try {
                    loadSharedObjects(config.getSharedObjects.values)
                    configureGDAL(config)
                    gdal.AllRegister()
                    isEnabled = true
                } match {
                    case Success(_)                    => logInfo("GDAL environment enabled successfully.")
                    case scala.util.Failure(exception) =>
                        logError("GDAL not enabled. Rasterx requires that GDAL be installed on the cluster.")
                        logError(s"Error: ${exception.getMessage}")
                        isEnabled = false
                        throw exception
                }
            }
        }

    def configureGDAL(config: ExpressionConfig): Unit = {
        // TODO: check the config propagation here
        val CPL_TMPDIR = config.configs.getOrElse("cpl_tmpdir", "/tmp/gdal")
        val GDAL_PAM_PROXY_DIR = config.configs.getOrElse("gdal_pam_proxy_dir", "/tmp/gdal/pam")
        configureGDAL(CPL_TMPDIR, GDAL_PAM_PROXY_DIR)
        config.getGDALConfig.foreach { case (key, value) => gdal.SetConfigOption(key, value) }
        this.checkpointPath = config.getRasterCheckpointDir
        this.useCheckpoint = config.useCheckpoint
    }

    def configureGDAL(CPL_TMPDIR: String, GDAL_PAM_PROXY_DIR: String): Unit = {
        gdal.SetConfigOption("GDAL_VRT_ENABLE_PYTHON", "YES")
        gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "YES")
        gdal.SetConfigOption("CPL_TMPDIR", CPL_TMPDIR)
        gdal.SetConfigOption("GDAL_PAM_PROXY_DIR", GDAL_PAM_PROXY_DIR)
        gdal.SetConfigOption("GDAL_PAM_ENABLED", "YES")
        gdal.SetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "NO")
        gdal.SetConfigOption("CPL_LOG", s"$CPL_TMPDIR/gdal.log")
        gdal.SetConfigOption("GDAL_CACHEMAX", "512")
        gdal.SetCacheMax(512 * 1024 * 1024)
        gdal.SetConfigOption("GDAL_NUM_THREADS", "4")
    }

    def loadSharedObjects(sharedObjects: Iterable[String]): Unit = {
        def loadOrNoop(path: String): Unit = {
            Try {
                if (Files.exists(Paths.get(path))) System.load(path)
            } match {
                case Success(_)                    => logInfo(s"Loaded GDAL shared object: $path")
                case scala.util.Failure(exception) =>
                    logError(s"Failed to load GDAL shared object: $path")
                    logError(s"Error: ${exception.getMessage}")
            }
        }
        loadOrNoop("/usr/lib/libgdalalljni.so")
        sharedObjects.foreach(loadOrNoop)
    }

}
