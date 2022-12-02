package org.apache.spark.sql.adapters

import org.apache.spark.util.{Utils => SparkUtils}

object Utils {

    def classForName[C](name: String): Class[C] = SparkUtils.classForName(name)

}
