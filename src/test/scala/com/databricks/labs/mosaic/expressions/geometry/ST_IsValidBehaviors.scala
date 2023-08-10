package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getWKTRowsDf
import com.databricks.labs.mosaic.test.MosaicSpatialQueryTest
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers.{all, an, be, convertToAnyShouldWrapper, noException}

//noinspection AccessorLikeMethodIsUnit
trait ST_IsValidBehaviors extends MosaicSpatialQueryTest {

    def isValidBehaviour(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val sc = spark
        val mc = mosaicContext
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val validDf = getWKTRowsDf().orderBy("id")
        val validResults = validDf.select(st_isvalid(col("wkt"))).as[Boolean].collect().toSeq

        all(validResults) should be(true)

        validDf.createOrReplaceTempView("source")
        val sqlValidResults = spark
            .sql("select st_isvalid(wkt) from source")
            .as[Boolean]
            .collect
            .toSeq

        all(sqlValidResults) should be(true)

        val invalidGeometries = List(
          List(
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))"
          ), // Hole Outside Shell
          List(
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2), (3 3, 3 7, 7 7, 7 3, 3 3))"
          ), // Nested Holes,
          List(
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (5 0, 10 5, 5 10, 0 5, 5 0))"
          ), // Disconnected Interior
          List("POLYGON((0 0, 10 10, 0 10, 10 0, 0 0))"), // Self Intersection
          List(
            "POLYGON((5 0, 10 0, 10 10, 0 10, 0 0, 5 0, 3 3, 5 6, 7 3, 5 0))"
          ), // Ring Self Intersection
          List(
            "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)),(( 2 2, 8 2, 8 8, 2 8, 2 2)))"
          ), // Nested Shells
          List(
            "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)),((0 0, 10 0, 10 10, 0 10, 0 0)))"
          ), // Duplicated Rings
          List("POLYGON((2 2, 8 2))"), // Too Few Points
          List("POLYGON((NaN 3, 3 4, 4 4, 4 3, 3 3))") // Invalid Coordinate
          // List("POLYGON((0 0, 0 10, 10 10, 10 0))") // Ring Not Closed
        )
        val rows = invalidGeometries.map { x => Row(x: _*) }
        val rdd = spark.sparkContext.makeRDD(rows)
        val schema = StructType(
          List(
            StructField("wkt", StringType)
          )
        )
        val invalidGeometriesDf = spark.createDataFrame(rdd, schema)

        val invalidDf = invalidGeometriesDf.withColumn("result", st_isvalid(col("wkt")))
        val invalidResults = invalidDf.select("result").as[Boolean].collect().toList

        all(invalidResults) should be(false)

        invalidDf.createOrReplaceTempView("source")
        val sqlInvalidResults = spark.sql("select st_isvalid(wkt) from source").collect.map(_.getBoolean(0)).toList

        all(sqlInvalidResults) should be(false)
    }

    def isValidCodegen(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        val sc = spark
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val validDf = getWKTRowsDf().orderBy("id")
        val results = validDf.select(st_isvalid(col("wkt"))).as[Boolean]

        val queryExecution = results.queryExecution
        val plan = queryExecution.executedPlan

        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])

        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()

        noException should be thrownBy CodeGenerator.compile(code)

        val stIsValid = ST_IsValid(lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stIsValid.genCode(ctx)
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val mc = mosaicContext
        mc.register(spark)

        val stIsValid = ST_IsValid(lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr, mc.expressionConfig)

        stIsValid.child shouldEqual lit("POLYGON (1 1, 2 2, 3 3, 4 4, 1 1)").expr
        stIsValid.dataType shouldEqual BooleanType
        noException should be thrownBy stIsValid.makeCopy(stIsValid.children.toArray)

    }

    def issue(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("FATAL")
        val sc = spark
        val mc = mosaicContext
        import mc.functions._
        import sc.implicits._
        mc.register(spark)

        val wkt = "MULTIPOLYGON (((-73.193199311173331 45.032701793372674," +
            " -73.193199422881179 45.032701420158844, -73.193199489594178 45.03270104157604," +
            " -73.193199510831988 45.032700660346435, -73.193199486441742 45.032700279212058," +
            " -73.193189346414215 45.032619101872115, -73.193189281895911 45.032618745525369," +
            " -73.193189177484285 45.032618393964931, -73.193189033845357 45.032618049432905," +
            " -73.1931888518943 45.032617714126, -73.1931886327919 45.032617390182267," +
            " -73.193188377935726 45.032617079667538, -73.1931880889503 45.032616784561895," +
            " -73.19318776767841 45.032616506747111, -73.193187416168712 45.032616247994795," +
            " -73.193187036662977 45.032616009954921, -73.193186631581028 45.032615794145443," +
            " -73.193186203505959 45.032615601942169, -73.193185755167562 45.032615434570921," +
            " -73.19318528942442 45.03261529309902, -73.193184809246617 45.032615178428671," +
            " -73.193184317696492 45.032615091291255, -73.193184032908164 45.032615054081091," +
            " -73.193183746022484 45.032615026123324, -73.1931834576363 45.03261500747665," +
            " -73.193183168350487 45.032614998179142, -73.193157790801948 45.032614596556655," +
            " -73.193157479312177 45.03261459701848, -73.193157168229348 45.032614608319179," +
            " -73.193156858304 45.032614630431752, -73.193156550281969 45.032614663302773," +
            " -73.19315624490693 45.032614706853145, -73.193155942913648 45.032614760977545," +
            " -73.193155645030487 45.032614825546155, -73.1931553519753 45.032614900403061," +
            " -73.193155064454146 45.032614985367509, -73.19315478316048 45.032615080235296," +
            " -73.193154508771826 45.032615184777434, -73.193154241949046 45.03261529874225," +
            " -73.193153983336273 45.032615421854715, -73.193153733555562 45.032615553818367," +
            " -73.19315349320992 45.032615694315325, -73.193153262878127 45.032615843006582," +
            " -73.193153043114989 45.032615999534251, -73.193152834450643 45.0326161635207, " +
            "-73.193152637387428 45.032616334570868, -73.1931524524011 45.032616512272384," +
            " -73.193152279936839 45.032616696197373, -73.1931521204103 45.03261688590225," +
            " -73.193151974206032 45.032617080929874, -73.193151841676766 45.032617280810378," +
            " -73.193151723141241 45.032617485062, -73.193151618885651 45.032617693192357," +
            " -73.193151529160616 45.03261790469967, -73.193151454183351 45.032618119074712," +
            " -73.193151394134 45.032618335800606, -73.19315134915702 45.032618554355118," +
            " -73.193151319361135 45.032618774211166, -73.193151304818045 45.032618994839076," +
            " -73.193148472795158 45.032708968760623, -73.193148475581978 45.032709235046163," +
            " -73.193148500581927 45.032709500748645, -73.19314854770748 45.032709764936783," +
            " -73.1931486167935 45.032710026685322, -73.193148707597757 45.032710285077187," +
            " -73.19314881980246 45.032710539207216, -73.1931489530145 45.032710788184829," +
            " -73.193149106766924 45.032711031138192, -73.193149280521069 45.032711267216158, " +
            "-73.19314947366874 45.032711495591407, -73.193149685532674 45.032711715463954," +
            " -73.1931499153713 45.032711926063705, -73.193150162378771 45.032712126652996, " +
            "-73.193150425690291 45.03271231652851, -73.193150704383228 45.032712495025585," +
            " -73.193150997480871 45.032712661518758, -73.1931513039572 45.032712815424929," +
            " -73.193151622737844 45.032712956204534, -73.193151952706273 45.032713083364762," +
            " -73.193152292706571 45.0327131964597, -73.193152641548025 45.032713295093714," +
            " -73.193152998007633 45.032713378920867, -73.193153360837016 45.032713447647481," +
            " -73.193153728765438 45.032713501032966, -73.193154100503861 45.032713538890214," +
            " -73.193154474749718 45.032713561086922, -73.193154850192016 45.032713567544562," +
            " -73.193155225515255 45.032713558241319, -73.193155599404861 45.032713533209154," +
            " -73.1931559705514 45.032713492536345, -73.193156337653889 45.032713436364965," +
            " -73.193156699426623 45.032713364891968, -73.193195049010967 45.0327049699328, " +
            "-73.193195555597441 45.032704841346956, -73.19319604499826 45.0327046827904, " +
            "-73.1931965136942 45.032704495403159, -73.193196958314829 45.032704280532862, " +
            "-73.193197375662976 45.032704039724337, -73.19319776273754 45.032703774709454," +
            " -73.193198116754672 45.032703487393924, -73.193198435168952 45.0327031798439," +
            " -73.193198715690087 45.032702854270973, -73.193198956301572 45.032702513016254," +
            " -73.1931991552728 45.032702158533887, -73.193199311173331 45.032701793372674," +
            " -73.193199311173331 45.032701793372674)))"

        val df = Seq(wkt).toDF("wkt")

        df.where(st_isvalid(col("wkt"))).count() should be(1)

    }

}
