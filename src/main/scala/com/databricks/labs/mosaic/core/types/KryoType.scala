package com.databricks.labs.mosaic.core.types

import org.apache.spark.sql.types._

class KryoType()
    extends StructType(
      Array(
        StructField("type_id", IntegerType),
        StructField("kryo", BinaryType)
      )
    ) {
    override def simpleString: String = "KRYO"
}
