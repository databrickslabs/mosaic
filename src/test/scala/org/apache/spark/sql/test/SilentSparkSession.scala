package org.apache.spark.sql.test

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator

trait SilentSparkSession extends SharedSparkSession {

    override def createSparkSession: TestSparkSession = {
        Configurator.setRootLevel(Level.WARN)
        Configurator.setLevel("org.apache.spark", Level.ERROR)
        Configurator.setLevel("org.sparkproject", Level.ERROR)
        Configurator.setLevel("org.eclipse.jetty", Level.WARN)
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
        session
    }
}
