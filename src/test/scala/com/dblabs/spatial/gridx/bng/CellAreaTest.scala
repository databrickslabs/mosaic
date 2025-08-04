package com.dblabs.spatial.gridx.bng

import com.dblabs.spatial.gridx.grid.BNG
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SharedSparkSession

class CellAreaTest extends PlanTest with SharedSparkSession {

    test("BNG CellArea on sting ids") {
        spark.sparkContext.setLogLevel("ERROR")
        import com.dblabs.spatial.gridx.bng.functions._
        com.dblabs.spatial.gridx.bng.functions.register(spark)

        val df = spark.createDataFrame(Seq(
            ("TQ388791", 0.01),
            ("TQ388792", 0.01),
            ("TQ388793", 0.01)
        )).toDF("cellId", "value")

        functions.register(spark)

        val result = df.select(bng_cell_area(df("cellId")))

        result.show()

    }

    test("BNG CellArea on long ids") {
        spark.sparkContext.setLogLevel("ERROR")
        import com.dblabs.spatial.gridx.bng.functions._

        val df = spark.createDataFrame(Seq(
            (BNG.parse("TQ388791"), 0.01),
            (BNG.parse("TQ388792"), 0.01),
            (BNG.parse("TQ388793"), 0.01)
        )).toDF("cellId", "value")

        functions.register(spark)

        val result = df.select(bng_cell_area(df("cellId")))

        result.show()

    }

}
