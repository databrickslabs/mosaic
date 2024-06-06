package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.raster.io.RasterCleaner
import com.databricks.labs.mosaic.core.types.RasterTileType
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StringType}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.concurrent.{Future, blocking}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

trait RasterPathAware {

    private val DISPOSE_DELAY_MILLIS = 1 * 60 * 1000

    private val lastDisposeCheckAtomic = new AtomicLong(-1)
    private val manualModeAtomic = new AtomicBoolean(true)
    private val managedCleanUpFuture = Future {
        GDAL.cleanUpManagedDir(manualModeAtomic.get())// non-blocking long lasting computation
    } // implicit execution context


    /**
      * No reason to constantly try to delete files in temp dir.
      * - Waits a minute between deletion attempts.
      * - Even then their is a futher testing based on the age of files,
      *   see [[com.databricks.labs.MOSAIC_RASTER_LOCAL_AGE_LIMIT_MINUTES]] and
      *   [[com.databricks.labs.MOSAIC_RASTER_TMP_PREFIX]] for managed dir,
      *   e.g. '/tmp/mosaic_tmp'.
      * @param manualMode
      *   if true, skip deleting files, means user is taking resonsibility for cleanup.
      * @return
      */
    private def doManagedCleanUp(manualMode: Boolean): Unit = {
        blocking {
            if (!manualMode && managedCleanUpFuture.isCompleted) {
                manualModeAtomic.set(manualMode)
                val currTime = System.currentTimeMillis()
                if (currTime - lastDisposeCheckAtomic.get() > DISPOSE_DELAY_MILLIS) {
                    lastDisposeCheckAtomic.set(currTime)
                    managedCleanUpFuture
                }
            }
        } // blocking
    }

    /** returns rasterType from a passed DataType, handling RasterTileType as well as string + binary. */
    def getRasterType(dataType: DataType): DataType = {
        dataType match {
            case tile: RasterTileType  => tile.rasterType
            case _ => dataType
        }
    }

    /** test if we have a path type [[StringType]] */
    def isPathType(dataType: DataType): Boolean = {
        getRasterType(dataType).isInstanceOf[StringType]
    }

    /** `isTypeDeleteSafe` tested for deleting files (wrapped in Try). */
    def pathSafeDispose(tile: MosaicRasterTile, manualMode: Boolean): Unit = {
        Try(pathSafeDispose(tile.getRaster, manualMode))
    }

    /** `isTypeDeleteSafe` tested for deleting files (wrapped in Try). */
    def pathSafeDispose(raster: MosaicRasterGDAL, manualMode: Boolean): Unit = {
        Try (RasterCleaner.destroy(raster))
        doManagedCleanUp(manualMode)
    }

    /////////////////////////////////////////////////////////
    // deserialize helpers
    /////////////////////////////////////////////////////////

    /** avoid checkpoint settings when deserializing, just want the actual type */
    def getDeserializeRasterType(idType: DataType, rasterExpr: Expression): DataType = {
        getRasterType(RasterTileType(idType, rasterExpr, useCheckpoint = false))
    }
}
