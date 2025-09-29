package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig

object CheckpointManager {

    private var checkpointPath: String = _
    private var useCheckpoint: Boolean = false

    def init(config: ExpressionConfig): Unit = {
        checkpointPath = config.getRasterCheckpointDir
        useCheckpoint = config.useCheckpoint
    }

    def setCheckpointPath(path: String): Unit = {
        checkpointPath = path
    }

    def getCheckpointPath: String = {
        if (checkpointPath == null || checkpointPath.isEmpty) {
            throw new IllegalStateException("Checkpoint path is not set. Please initialize CheckpointManager first.")
        }
        checkpointPath
    }

    def setUseCheckpoint(use: Boolean): Unit = {
        useCheckpoint = use
    }

    def isUseCheckpoint: Boolean = useCheckpoint

}
