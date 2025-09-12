package org.apache.spark.sql.adapters

import org.apache.spark.TaskContext
import org.apache.spark.memory.TaskMemoryManager

object MemoryManager {

    def getMemoryManager: TaskMemoryManager = {
        val tc = TaskContext.get()
        if (tc != null) {
            tc.taskMemoryManager()
        } else {
            throw new IllegalStateException("Spark environment is not initialized.")
        }
    }

}
