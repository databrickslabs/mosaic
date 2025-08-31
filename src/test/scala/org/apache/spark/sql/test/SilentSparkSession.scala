package org.apache.spark.sql.test

trait SilentSparkSession extends SharedSparkSession {

    override def createSparkSession: TestSparkSession = {
        val conf = sparkConf
        conf.set("spark.driver.extraJavaOptions", "-Djava.library.path=/usr/local/hadoop/lib/native")
        conf.set("spark.executor.extraJavaOptions", "-Djava.library.path=/usr/local/hadoop/lib/native")
        conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        conf.set("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
        conf.set("spark.hadoop.fs.file.impl.disable.cache", "true")
        conf.set("spark.default.parallelism", "8")
        conf.set("spark.sql.shuffle.partitions", "8")
        val session = new TestSparkSession(conf, 1, 12)
        session.sparkContext.setLogLevel("ERROR")
        println(s"master=${session.sparkContext.master}, dp=${session.sparkContext.defaultParallelism}")
        session
    }
}
