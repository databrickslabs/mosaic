package com.dblabs.spatial.expressions

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.must.Matchers.{an, be, noException}

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

}
