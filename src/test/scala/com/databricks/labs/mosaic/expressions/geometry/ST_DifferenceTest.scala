package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_DifferenceTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_DifferenceBehaviors {

    testAllGeometriesCodegen("ST_Difference behavior") { behavior }
    testAllGeometriesNoCodegen("ST_Difference behavior") { behavior }
    testAllGeometriesCodegen("ST_Difference codegen compilation") { codegenCompilation }
    testAllGeometriesNoCodegen("ST_Difference auxiliary methods") { auxiliaryMethods }

}
