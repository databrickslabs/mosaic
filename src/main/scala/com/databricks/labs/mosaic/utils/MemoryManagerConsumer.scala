package com.databricks.labs.mosaic.utils

import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.sql.adapters.MemoryManager

object MemoryManagerConsumer {

    var isFirstCall = true

    private val tmm: TaskMemoryManager = MemoryManager.getMemoryManager

    private val offHeapConsumer =
        new MemoryConsumer(
          tmm,
          tmm.pageSizeBytes(),
          MemoryMode.OFF_HEAP
        ) {
            override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
        }

    private val onHeapConsumer =
        new MemoryConsumer(
          tmm,
          tmm.pageSizeBytes(),
          MemoryMode.ON_HEAP
        ) {
            override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
        }

    def acquireOffHeapMemory(size: Long): Unit = {
        if (isFirstCall) {
            isFirstCall = false
            // Initialize gdal cache region
            // this is a side effect workaround
            offHeapConsumer.acquireMemory(512 * 1024 * 1024) // 512MB
        }
        if (size > 0) offHeapConsumer.acquireMemory(size)
    }

    def adjustOffHeapMemory(tile: MosaicRasterTile, expressionConfig: MosaicExpressionConfig): Unit = {
        val memDif = tile.getRaster.getMemSize - expressionConfig.getTileMaxSize
        if (memDif > 0) {
            offHeapConsumer.acquireMemory(memDif)
        } else if (memDif < 0) {
            offHeapConsumer.freeMemory(-memDif)
        }
    }

    def acquireOnHeapMemory(size: Long): Unit = onHeapConsumer.acquireMemory(size)

    def releaseOffHeapMemory(size: Long): Unit = {
        if (size <= 0) return
        offHeapConsumer.freeMemory(size)
    }

    def releaseOnHeapMemory(size: Long): Unit = onHeapConsumer.freeMemory(size)

}
