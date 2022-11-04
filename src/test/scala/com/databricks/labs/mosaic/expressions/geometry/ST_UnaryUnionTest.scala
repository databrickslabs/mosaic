package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_UnaryUnionTest extends MosaicSpatialQueryTest with SharedSparkSession with ST_UnaryUnionBehaviours {

    testAllGeometriesCodegen("ST_UnaryUnion behavior") { behavior }
    testAllGeometriesNoCodegen("ST_UnaryUnion behavior") { behavior }
    testAllGeometriesCodegen("ST_UnaryUnion codegen compilation") { codegenCompilation }
    testAllGeometriesNoCodegen("ST_UnaryUnion auxiliary methods") { auxiliaryMethods }

}
