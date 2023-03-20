package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_LengthTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_LengthBehaviors {

    testAllGeometriesNoCodegen("Testing stLength NO_CODEGEN") { lengthBehaviour }
    testAllGeometriesCodegen("Testing stLength CODEGEN compilation") { lengthCodegen }
    testAllGeometriesCodegen("Testing stLength CODEGEN") { lengthBehaviour }
    testAllGeometriesNoCodegen("Testing auxiliary methods") { auxiliaryMethods }

}
