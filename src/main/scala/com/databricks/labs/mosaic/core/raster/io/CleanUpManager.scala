package com.databricks.labs.mosaic.core.raster.io

import com.databricks.labs.mosaic.core.raster.api.GDAL.cleanUpManualDir
import com.databricks.labs.mosaic.core.raster.io.CleanUpManager.{delayMinutesAtomic, interruptAtomic}
import com.databricks.labs.mosaic.gdal.MosaicGDAL.{getLocalAgeLimitMinutesThreadSafe, getLocalRasterDirThreadSafe, isManualModeThreadSafe}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.DurationInt
import scala.util.Try

private class CleanUpManager extends Thread {

    // scalastyle:off println
    /** Separate thread for the cleanup. */
    override def run(): Unit = {
        println(s"Thread ${Thread.currentThread().getName} is now running.")
        // start loop with initial cleanup
        // - async is a Future (spawned from this long-running separate thread)
        while (!interruptAtomic.get() && !Thread.currentThread().isInterrupted) {
            Try {
                doCleanUp() match {
                    case Some(t) =>
                        if (delayMinutesAtomic.get() > -1) {
                            println(s"Thread ${Thread.currentThread().getName} latest cleanup complete... msg: '$t' " +
                                s"- thread config '${delayMinutesAtomic.get()}' minute(s)")
                        }
                    case _ => () // e.g. not completed due to interrupt
                }
            }

            Thread.sleep(delayMinutesAtomic.get().minute.toMillis)
        }
    }
    // scalastyle:on println

    /**
     * Cleans up LOCAL rasters that are older than [[MOSAIC_RASTER_LOCAL_AGE_LIMIT_MINUTES]],
     * e.g. 30 minutes from the configured local temp directory, e.g. "/tmp/mosaic_tmp";
     * config uses [[MOSAIC_RASTER_TMP_PREFIX]] for the "/tmp" portion of the path.
     * - Cleaning up is destructive and should only be done when the raster is no longer needed,
     *   so instead of cleaning up a specified local path as in versions prior to 0.4.3,
     *   this will clean up ANY files meeting the local age limit threshold.
     * - Manual mode can be configured to skip deletion of interim file writes,
     *   if any (user then takes on responsibility to clean ... or not).
     *
     * @returns
     *   Returns an [[Option[String]] which may be populated with any error information.
     */
    private def doCleanUp(): Option[String] = {
        // scalastyle:off println
        if (!isManualModeThreadSafe) {
            val ageLimit = getLocalAgeLimitMinutesThreadSafe
            val localDir = getLocalRasterDirThreadSafe
            println(s"\n... Thread ${Thread.currentThread().getName} initiating cleanup " +
                s"- age limit? $ageLimit, dir? '$localDir'\n")
            cleanUpManualDir(ageLimit, localDir, keepRoot = true)
        }  else None
    }
    // scalastyle:on println

}

/** static - singleton companion */
object CleanUpManager {

    private val THREAD_NAME = "Mosaic-CleanUp-Manager"
    private val delayMinutesAtomic = new AtomicInteger(1)
    private val interruptAtomic = new AtomicBoolean(false)

    /** initialize clean thread. */
    private var cleanThread = new CleanUpManager()
    synchronized {
        cleanThread.setName(THREAD_NAME)
        cleanThread.start()
    }

    def getCleanThreadDelayMinutes: Int = synchronized(delayMinutesAtomic.get())

    // scalastyle:off println
    def setCleanThreadDelayMinutes(delay: Int): Unit = {
        synchronized(
        delay match {
            case d if d > 0 =>
                delayMinutesAtomic.set(d)
                println(s"... setting $THREAD_NAME delay to $d minutes")
            case _ => ()
                println(s"... ignoring invalid request for $THREAD_NAME delay of $delay minutes, " +
                    s"must be > 0")
        })
    }

    def interruptCleanThread: Unit =
        synchronized({
            interruptAtomic.set(true)
            println(s"... interrupt manually requested for $THREAD_NAME")
        })

    def isCleanThreadAlive: Boolean = synchronized(cleanThread.isAlive)

    def isCleanThreadInterrupt: Boolean = synchronized(interruptAtomic.get())

    def getCleanThreadName: String = THREAD_NAME

    def runCleanThread(): Unit = {
        synchronized(
            if (!isCleanThreadAlive) {
                println(s"... starting new $THREAD_NAME thread (no previous alive)")
                interruptAtomic.set(false) // reset
                cleanThread = new CleanUpManager()
                cleanThread.setName(THREAD_NAME)
                cleanThread.start()
        })
    }
    // scalastyle:on println

}

