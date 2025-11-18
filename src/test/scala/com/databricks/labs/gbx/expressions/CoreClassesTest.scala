package com.databricks.labs.gbx.expressions

import com.databricks.labs.gbx.vectorx.jts.legacy.expressions.ST_LegacyAsWKB
import org.apache.spark.sql.adapters.SparkHadoopUtils
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.must.Matchers.{an, be, noException, not}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.language.postfixOps

class CoreClassesTest extends PlanTest with SilentSparkSession {

    test("Core Expression Classes getters/constructors should not fail") {
        val exprConf = ExpressionConfigExpr()
        noException should be thrownBy exprConf.withNewChildren(Seq.empty)

        val dummyExpr = new WithExpressionInfo {
            override def name: String = "dummy"

            override def usageArgs: String = ""

            override def description: String = ""

            override def extendedUsageArgs: String = ""

            override def examples: String = ""
        }
        an[IllegalAccessException] should be thrownBy dummyExpr.builder()

        noException should be thrownBy ST_LegacyAsWKB.builder()
        noException should be thrownBy ST_LegacyAsWKB.info()
    }

    test("Should be able to get sdu instance") {
        val sdu = SparkHadoopUtils.sdu
        sdu should not be null
    }

}
