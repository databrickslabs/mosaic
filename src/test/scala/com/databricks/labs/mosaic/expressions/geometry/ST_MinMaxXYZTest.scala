package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_MinMaxXYZTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_MinMaxXYZBehaviors {

    testAllGeometriesNoCodegen("Testing stXMin NO_CODEGEN") { xMinBehavior }
    testAllGeometriesCodegen("Testing stXMin CODEGEN compilation") { xMinCodegen }
    testAllGeometriesCodegen("Testing stXMin CODEGEN") { xMinBehavior }
    testAllGeometriesNoCodegen("Testing stXMax NO_CODEGEN") { xMaxBehavior }
    testAllGeometriesCodegen("Testing stXMax CODEGEN compilation") { xMaxCodegen }
    testAllGeometriesCodegen("Testing stXMax CODEGEN") { xMaxBehavior }
    testAllGeometriesNoCodegen("Testing stYMin NO_CODEGEN") { yMinBehavior }
    testAllGeometriesCodegen("Testing stYMin CODEGEN compilation") { yMinCodegen }
    testAllGeometriesCodegen("Testing stYMin CODEGEN") { yMinBehavior }
    testAllGeometriesNoCodegen("Testing stYMax NO_CODEGEN") { yMaxBehavior }
    testAllGeometriesCodegen("Testing stYMax CODEGEN compilation") { yMaxCodegen }
    testAllGeometriesCodegen("Testing stYMax CODEGEN") { yMaxBehavior }
    testAllGeometriesNoCodegen("Testing auxiliary methods") { auxiliary }

}
