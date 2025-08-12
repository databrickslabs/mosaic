package com.dblabs.spatial.rasterx.gdal

import com.dblabs.spatial.expressions.ExpressionConfig

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

    def getCheckpointPath: String = checkpointPath

    def setUseCheckpoint(use: Boolean): Unit = {
        useCheckpoint = use
    }

    def isUseCheckpoint: Boolean = useCheckpoint

}
