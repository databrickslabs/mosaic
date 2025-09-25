package com.dblabs.gbx.expressions

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
        }
        an[IllegalAccessException] should be thrownBy dummyExpr.builder()
    }

    test("Should be able to get sdu instance") {
        val sdu = SparkHadoopUtils.sdu
        sdu should not be null
    }

}
