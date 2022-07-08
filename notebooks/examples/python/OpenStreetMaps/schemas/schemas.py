from pyspark.sql.types import LongType, TimestampType, BooleanType, IntegerType, StringType, StructField, ArrayType, StructType, DoubleType

id_field = StructField("_id", LongType(), True)

common_fields = [
    StructField("_timestamp", TimestampType(), True),
    StructField("_visible", BooleanType(), True),
    StructField("_version", IntegerType(), True),
    StructField(
        "tag",
        ArrayType(
            StructType(
                [
                    StructField("_VALUE", StringType(), True),
                    StructField("_k", StringType(), True),
                    StructField("_v", StringType(), True),
                ]
            )
        ),
        True,
    ),
]

nodes_schema = StructType(
    [
        id_field,
        StructField("_lat", DoubleType(), True),
        StructField("_lon", DoubleType(), True),
        *common_fields,
    ]
)

ways_schema = StructType(
    [
        id_field,
        *common_fields,
        StructField(
            "nd",
            ArrayType(
                StructType(
                    [
                        StructField("_VALUE", StringType(), True),
                        StructField("_ref", LongType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

relations_schema = StructType(
    [
        id_field,
        *common_fields,
        StructField(
            "member",
            ArrayType(
                StructType(
                    [
                        StructField("_VALUE", StringType(), True),
                        StructField("_ref", LongType(), True),
                        StructField("_role", StringType(), True),
                        StructField("_type", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)
